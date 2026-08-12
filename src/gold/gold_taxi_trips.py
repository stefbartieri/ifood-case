# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - workspace.nyc_taxi_gold.taxi_trips + dq_metrics
# MAGIC
# MAGIC Le a bronze (`workspace.nyc_taxi_bronze.taxi_trips`, 100% das linhas da
# MAGIC origem), aplica as 4 regras de DQ na ordem fixa R1→R4 (cada linha removida
# MAGIC conta apenas na PRIMEIRA regra violada), grava a gold com as 8 colunas do
# MAGIC case (overwrite completo) e persiste as metricas por regra em
# MAGIC `workspace.nyc_taxi_gold.dq_metrics`. As linhas reprovadas nao sao
# MAGIC descartadas: vao para `workspace.nyc_taxi_gold.taxi_trips_rejected` com o
# MAGIC motivo em `_reject_reason`, permitindo auditoria linha a linha. A
# MAGIC reconciliacao `linhas_bronze == linhas_gold + soma(removidas)` e o
# MAGIC particionamento `gold + quarentena == bronze` sao verificados com `assert`
# MAGIC por taxi_type e no total - o notebook FALHA se nao fecharem.

# COMMAND ----------

# spark e injetado pelo runtime Databricks; o bloco espelhado da logica gold e
# validado por pytest contra src/gold/build_gold.py.
# ruff: noqa: F821, E402

# INICIO logica gold (espelho de src/gold/build_gold.py - nao editar aqui sem
# atualizar o modulo; tests/test_gold_rules.py compara os dois)

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

REJECTED_COLUMNS = {
    **GOLD_COLUMNS,
    "source_year_month": "string",
    "_reject_reason": "string",
}

JANELA_PICKUP = ("2023-01-01 00:00:00", "2023-06-01 00:00:00")

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


# FIM logica gold

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.nyc_taxi_gold")

df_bronze = spark.table("workspace.nyc_taxi_bronze.taxi_trips")
df_classificado = classificar_dq(df_bronze)
df_metricas = calcular_metricas(df_classificado)

# COMMAND ----------

# Gravacao da gold: apenas linhas limpas, 8 colunas, overwrite completo.

df_gold = selecionar_schema_gold(df_classificado.filter("dq_regra_violada IS NULL"))

(
    df_gold.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.nyc_taxi_gold.taxi_trips")
)

print("Gold gravada em workspace.nyc_taxi_gold.taxi_trips")

# COMMAND ----------

# Gravacao das metricas de DQ (overwrite por execucao).

(
    df_metricas.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.nyc_taxi_gold.dq_metrics")
)

print("Metricas gravadas em workspace.nyc_taxi_gold.dq_metrics")

# COMMAND ----------

# Quarentena: linhas reprovadas com o motivo, para auditoria linha a linha.
# Complemento exato da gold - as duas juntas reconstituem a bronze.

df_rejected = selecionar_schema_rejected(
    df_classificado.filter("dq_regra_violada IS NOT NULL")
)

(
    df_rejected.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.nyc_taxi_gold.taxi_trips_rejected")
)

print("Quarentena gravada em workspace.nyc_taxi_gold.taxi_trips_rejected")

# COMMAND ----------

# Reconciliacao obrigatoria: linhas_bronze == linhas_gold + soma(removidas),
# por taxi_type e no total. Falha explicita se nao fechar.

mets = {
    (linha["taxi_type"], linha["metrica"]): linha["valor"]
    for linha in df_metricas.collect()
}
escopos = sorted({t for t, _ in mets})

for escopo in escopos:
    bronze_n = mets[(escopo, "linhas_bronze")]
    gold_n = mets[(escopo, "linhas_gold")]
    removidas_n = sum(
        v for (t, m), v in mets.items() if t == escopo and m.startswith("removidas_")
    )
    print(
        f"[{escopo}] bronze={bronze_n} gold={gold_n} removidas={removidas_n} "
        f"aproveitamento={gold_n / bronze_n:.4f}"
    )
    assert bronze_n == gold_n + removidas_n, (
        f"RECONCILIACAO FALHOU para {escopo}: "
        f"bronze={bronze_n} != gold={gold_n} + removidas={removidas_n}"
    )

print("Reconciliacao OK em todos os escopos")

# COMMAND ----------

# Particionamento: gold + quarentena == bronze, lendo as TABELAS GRAVADAS (nao
# os DataFrames em memoria) - prova que o que foi persistido esta completo.

gold_por_tipo = {
    linha["taxi_type"]: linha["n"]
    for linha in spark.sql(
        "SELECT taxi_type, COUNT(*) AS n "
        "FROM workspace.nyc_taxi_gold.taxi_trips GROUP BY taxi_type"
    ).collect()
}
rej_por_tipo = {
    linha["taxi_type"]: linha["n"]
    for linha in spark.sql(
        "SELECT taxi_type, COUNT(*) AS n "
        "FROM workspace.nyc_taxi_gold.taxi_trips_rejected GROUP BY taxi_type"
    ).collect()
}

for escopo in escopos:
    bronze_n = mets[(escopo, "linhas_bronze")]
    if escopo == "total":
        gold_n, rej_n = sum(gold_por_tipo.values()), sum(rej_por_tipo.values())
    else:
        gold_n, rej_n = gold_por_tipo.get(escopo, 0), rej_por_tipo.get(escopo, 0)
    print(f"[{escopo}] gold={gold_n} + quarentena={rej_n} vs bronze={bronze_n}")
    assert bronze_n == gold_n + rej_n, (
        f"PARTICIONAMENTO FALHOU para {escopo}: "
        f"gold={gold_n} + quarentena={rej_n} != bronze={bronze_n}"
    )

# Cada motivo da quarentena bate com a metrica removidas_* correspondente.
por_motivo = {
    linha["_reject_reason"]: linha["n"]
    for linha in spark.sql(
        "SELECT _reject_reason, COUNT(*) AS n "
        "FROM workspace.nyc_taxi_gold.taxi_trips_rejected GROUP BY _reject_reason"
    ).collect()
}
for nome, _ in REGRAS_DQ:
    esperado = mets[("total", nome)]
    obtido = por_motivo.get(nome, 0)
    assert obtido == esperado, f"{nome}: quarentena={obtido} != dq_metrics={esperado}"

print("Particionamento OK: gold + quarentena reconstituem a bronze")

# COMMAND ----------

# Conferencia final: metricas completas.

spark.sql(
    """
    SELECT metrica, taxi_type, valor
    FROM workspace.nyc_taxi_gold.dq_metrics
    ORDER BY taxi_type, metrica
    """
).show(20, truncate=False)
