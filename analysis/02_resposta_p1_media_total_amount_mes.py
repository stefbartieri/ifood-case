# Databricks notebook source
# MAGIC %md
# MAGIC # Pergunta 1 — Media de total_amount por mes (yellow taxis, Jan–Mai/2023)
# MAGIC
# MAGIC Resposta calculada de duas formas independentes sobre a camada de consumo:
# MAGIC **PySpark** (DataFrame API sobre `workspace.nyc_taxi_gold.taxi_trips`) e
# MAGIC **SQL** (view `workspace.nyc_taxi_gold.vw_media_total_amount_mes`), com
# MAGIC paridade (|dif| <= 0,01) e validacao contra benchmark externo (±0,5).
# MAGIC
# MAGIC Filtros herdados da gold: pickup em Jan–Mai/2023, dropoff >
# MAGIC pickup, `total_amount >= 0`, `passenger_count` nao nulo e > 0.

# COMMAND ----------

# spark e injetado pelo runtime Databricks.
# ruff: noqa: F821, E402

from pyspark.sql import functions as F

BENCHMARK_P1 = {
    "2023-01": 27.44,
    "2023-02": 27.33,
    "2023-03": 28.26,
    "2023-04": 28.76,
    "2023-05": 29.46,
}

gold = spark.table("workspace.nyc_taxi_gold.taxi_trips")

# COMMAND ----------

# MAGIC %md ## Calculo 1 — PySpark (DataFrame API)

# COMMAND ----------

p1_pyspark = (
    gold.filter(F.col("taxi_type") == "yellow")
    .groupBy("pickup_year_month")
    .agg(
        F.round(F.avg("total_amount"), 2).alias("media_pyspark"),
        F.count("*").alias("qtd_pyspark"),
    )
    .orderBy("pickup_year_month")
)
display(p1_pyspark)

# COMMAND ----------

# MAGIC %md ## Calculo 2 — SQL (view da gold)

# COMMAND ----------

p1_sql = spark.sql(
    """
    SELECT pickup_year_month,
           media_total_amount AS media_sql,
           qtd_corridas       AS qtd_sql
    FROM workspace.nyc_taxi_gold.vw_media_total_amount_mes
    ORDER BY pickup_year_month
    """
)
display(p1_sql)

# COMMAND ----------

# MAGIC %md ## Comparacao: paridade SQL vs PySpark + benchmark

# COMMAND ----------

comparacao = {r["pickup_year_month"]: r for r in p1_sql.collect()}
for r in p1_pyspark.collect():
    comparacao[r["pickup_year_month"]] = {
        **comparacao[r["pickup_year_month"]].asDict(),
        "media_pyspark": r["media_pyspark"],
        "qtd_pyspark": r["qtd_pyspark"],
    }

print(
    f"{'mes':<9}{'sql':>8}{'pyspark':>9}{'|dif|':>7}  paridade"
    f"{'benchmark':>11}{'desvio':>8}  status"
)
problemas = 0
for mes in sorted(comparacao):
    c = comparacao[mes]
    dif = abs(c["media_sql"] - c["media_pyspark"])
    paridade = "OK" if dif <= 0.01 else "FALHOU"
    desvio = c["media_sql"] - BENCHMARK_P1[mes]
    status = "OK" if abs(desvio) <= 0.5 else "INVESTIGAR"
    problemas += (paridade != "OK") + (status != "OK")
    print(
        f"{mes:<9}{c['media_sql']:>8.2f}{c['media_pyspark']:>9.2f}{dif:>7.3f}"
        f"  {paridade:<8}{BENCHMARK_P1[mes]:>9.2f}{desvio:>8.2f}  {status}"
    )
print(f"\n{'TUDO OK' if problemas == 0 else f'{problemas} problema(s) — investigar'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Investigacao condicional (interpretar apenas se houver INVESTIGAR acima)
# MAGIC
# MAGIC Recalcula a media do yellow direto da **bronze** com cada combinacao de
# MAGIC filtros da gold, para isolar qual regra explicaria um desvio.

# COMMAND ----------

bronze_yellow = spark.table("workspace.nyc_taxi_bronze.taxi_trips").filter(
    "taxi_type = 'yellow' "
    "AND tpep_pickup_datetime >= TIMESTAMP_NTZ '2023-01-01 00:00:00' "
    "AND tpep_pickup_datetime < TIMESTAMP_NTZ '2023-06-01 00:00:00'"
)
cenarios = {
    "sem_filtros": "1=1",
    "so_total_amount>=0": "total_amount >= 0",
    "so_passenger>0": "passenger_count IS NOT NULL AND passenger_count > 0",
    "gold(r3+r4)": (
        "total_amount >= 0 AND passenger_count IS NOT NULL AND passenger_count > 0"
    ),
}
resultado = None
for nome, cond in cenarios.items():
    df = (
        bronze_yellow.filter(cond)
        .withColumn("mes", F.date_format("tpep_pickup_datetime", "yyyy-MM"))
        .groupBy("mes")
        .agg(F.round(F.avg("total_amount"), 2).alias(nome))
    )
    resultado = df if resultado is None else resultado.join(df, "mes")
display(resultado.orderBy("mes"))

# COMMAND ----------

# MAGIC %md ## Resultado final (tabela consolidada)

# COMMAND ----------

print("| mes | media_sql | media_pyspark | |dif| | benchmark | desvio |")
print("|-----|-----------|---------------|-------|-----------|--------|")
for mes in sorted(comparacao):
    c = comparacao[mes]
    dif = abs(c["media_sql"] - c["media_pyspark"])
    print(
        f"| {mes} | {c['media_sql']:.2f} | {c['media_pyspark']:.2f} | {dif:.3f} "
        f"| {BENCHMARK_P1[mes]:.2f} | {c['media_sql'] - BENCHMARK_P1[mes]:+.2f} |"
    )
