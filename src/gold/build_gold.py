"""Logica da camada gold - importavel sem SparkSession.

Constantes puras (schema gold, janela de pickup, regras de DQ na ordem fixa
R1->R4 e nomes das metricas) + funcoes de transformacao que importam pyspark
apenas quando chamadas. O notebook src/gold/gold_taxi_trips.py espelha este
modulo (bloco INICIO/FIM logica gold) e tests/test_gold_rules.py garante que
os dois nao divergem.

Semantica das regras: a gold mantem apenas linhas que satisfazem TODAS as
regras; cada linha removida e atribuida a PRIMEIRA regra violada (ordem
R1->R4), garantindo linhas_bronze == linhas_gold + soma(removidas).

As linhas reprovadas nao sao descartadas: vao para a quarentena
(taxi_trips_rejected) anotadas com o motivo em _reject_reason. Gold e
quarentena sao complementares e disjuntas - juntas reconstituem a bronze.
"""

# Schema da gold: exatamente 8 colunas, nesta ordem (nomes e tipos do case).
GOLD_COLUMNS = {
    "VendorID": "int",
    "passenger_count": "int",
    "total_amount": "double",
    "tpep_pickup_datetime": "timestamp_ntz",
    "tpep_dropoff_datetime": "timestamp_ntz",
    "taxi_type": "string",
    "pickup_year_month": "string",
    "pickup_hour": "int",
}

# Schema da quarentena: as 8 colunas da gold (mesma ordem) + a linhagem que
# leva de volta ao arquivo de origem + o motivo da reprovacao.
REJECTED_COLUMNS = {
    **GOLD_COLUMNS,
    "source_year_month": "string",
    "_reject_reason": "string",
}

# Janela de pickup Jan-Mai/2023: intervalo MEIO-ABERTO [inicio, fim).
JANELA_PICKUP = ("2023-01-01 00:00:00", "2023-06-01 00:00:00")

# Regras de DQ na ordem fixa R1->R4: (nome_da_metrica, expressao SQL da
# VIOLACAO). NULL viola a regra (a linha nao satisfaz a condicao de limpeza).
REGRAS_DQ = [
    (
        "removidas_r1_pickup_fora_janela",
        "tpep_pickup_datetime IS NULL"
        f" OR tpep_pickup_datetime < TIMESTAMP_NTZ '{JANELA_PICKUP[0]}'"
        f" OR tpep_pickup_datetime >= TIMESTAMP_NTZ '{JANELA_PICKUP[1]}'",
    ),
    (
        "removidas_r2_dropoff_nao_posterior",
        "tpep_dropoff_datetime IS NULL"
        " OR tpep_dropoff_datetime <= tpep_pickup_datetime",
    ),
    (
        "removidas_r3_total_amount_negativo",
        "total_amount IS NULL OR total_amount < 0",
    ),
    (
        "removidas_r4_passenger_count_invalido",
        "passenger_count IS NULL OR passenger_count <= 0",
    ),
]

# Nomes fixos das 6 metricas persistidas em dq_metrics.
METRICAS = [
    "linhas_bronze",
    "removidas_r1_pickup_fora_janela",
    "removidas_r2_dropoff_nao_posterior",
    "removidas_r3_total_amount_negativo",
    "removidas_r4_passenger_count_invalido",
    "linhas_gold",
]


def classificar_dq(df):
    """Adiciona a coluna dq_regra_violada (nome da PRIMEIRA regra violada,
    na ordem R1->R4; NULL = linha limpa) em uma unica passada."""
    from pyspark.sql import functions as F

    nome0, viol0 = REGRAS_DQ[0]
    condicao = F.when(F.expr(viol0), F.lit(nome0))
    for nome, viol in REGRAS_DQ[1:]:
        condicao = condicao.when(F.expr(viol), F.lit(nome))
    return df.withColumn(
        "dq_regra_violada", condicao.otherwise(F.lit(None).cast("string"))
    )


def _projecao_gold():
    """Expressoes das 8 colunas da gold, na ordem de GOLD_COLUMNS. Usada pela
    gold e pela quarentena para que as duas nao possam divergir."""
    from pyspark.sql import functions as F

    return [
        F.col("VendorID").cast("int"),
        F.col("passenger_count").cast("int"),
        F.col("total_amount").cast("double"),
        F.col("tpep_pickup_datetime"),
        F.col("tpep_dropoff_datetime"),
        F.col("taxi_type"),
        F.date_format("tpep_pickup_datetime", "yyyy-MM").alias("pickup_year_month"),
        F.hour("tpep_pickup_datetime").cast("int").alias("pickup_hour"),
    ]


def selecionar_schema_gold(df):
    """Projeta o schema gold (8 colunas) com as derivadas do pickup."""
    return df.select(*_projecao_gold())


def selecionar_schema_rejected(df):
    """Projeta o schema da quarentena (10 colunas): as 8 da gold + a linhagem
    source_year_month + o motivo (_reject_reason, vindo de dq_regra_violada).

    Espera o DataFrame ja classificado e filtrado em dq_regra_violada IS NOT
    NULL. Linhas que violam R1 por pickup nulo produzem derivadas nulas - e o
    comportamento correto: nao se inventa data para um registro invalido."""
    from pyspark.sql import functions as F

    return df.select(
        *_projecao_gold(),
        F.col("source_year_month"),
        F.col("dq_regra_violada").alias("_reject_reason"),
    )


def calcular_metricas(df_classificado, run_ts=None):
    """DataFrame longo (run_ts, metrica, taxi_type, valor) com as 6 metricas
    por yellow/green/total, calculadas da coluna dq_regra_violada."""
    from datetime import datetime

    if run_ts is None:
        run_ts = datetime.now()

    contagens = (
        df_classificado.groupBy("taxi_type", "dq_regra_violada").count().collect()
    )
    tipos = sorted({linha["taxi_type"] for linha in contagens})

    def _valor(escopo, regra):
        return sum(
            linha["count"]
            for linha in contagens
            if (escopo == "total" or linha["taxi_type"] == escopo)
            and linha["dq_regra_violada"] == regra
        )

    linhas = []
    for escopo in [*tipos, "total"]:
        removidas = {nome: _valor(escopo, nome) for nome, _ in REGRAS_DQ}
        linhas_gold = _valor(escopo, None)
        linhas_bronze = linhas_gold + sum(removidas.values())
        linhas.append((run_ts, "linhas_bronze", escopo, linhas_bronze))
        for nome, _ in REGRAS_DQ:
            linhas.append((run_ts, nome, escopo, removidas[nome]))
        linhas.append((run_ts, "linhas_gold", escopo, linhas_gold))

    spark = df_classificado.sparkSession
    return spark.createDataFrame(
        linhas, "run_ts timestamp, metrica string, taxi_type string, valor long"
    )
