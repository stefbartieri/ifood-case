# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — workspace.nyc_taxi_gold.taxi_trips + dq_metrics
# MAGIC
# MAGIC Le a bronze (`workspace.nyc_taxi_bronze.taxi_trips`, 100% das linhas da
# MAGIC origem), aplica as 4 regras de DQ na ordem fixa R1→R4 (cada linha removida
# MAGIC conta apenas na PRIMEIRA regra violada), grava a gold com as 8 colunas do
# MAGIC case (overwrite completo) e persiste as metricas por regra em
# MAGIC `workspace.nyc_taxi_gold.dq_metrics`. A reconciliacao
# MAGIC `linhas_bronze == linhas_gold + soma(removidas)` e verificada com `assert`
# MAGIC por taxi_type e no total — o notebook FALHA se nao fechar.

# COMMAND ----------

# spark e injetado pelo runtime Databricks; o bloco espelhado da logica gold e
# validado por pytest contra src/gold/build_gold.py.
# ruff: noqa: F821, E402

# INICIO logica gold (espelho de src/gold/build_gold.py — nao editar aqui sem
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


def selecionar_schema_gold(df):
    """Projeta o schema gold (8 colunas) com as derivadas do pickup."""
    from pyspark.sql import functions as F

    return df.select(
        F.col("VendorID").cast("int"),
        F.col("passenger_count").cast("int"),
        F.col("total_amount").cast("double"),
        F.col("tpep_pickup_datetime"),
        F.col("tpep_dropoff_datetime"),
        F.col("taxi_type"),
        F.date_format("tpep_pickup_datetime", "yyyy-MM").alias("pickup_year_month"),
        F.hour("tpep_pickup_datetime").cast("int").alias("pickup_hour"),
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

# Conferencia final: metricas completas.

spark.sql(
    """
    SELECT metrica, taxi_type, valor
    FROM workspace.nyc_taxi_gold.dq_metrics
    ORDER BY taxi_type, metrica
    """
).show(20, truncate=False)
