# Databricks notebook source
# MAGIC %md
# MAGIC # EDA - NYC TLC Trip Records (Jan-Mai/2023, yellow + green)
# MAGIC
# MAGIC Analise exploratoria sobre a **bronze** (`workspace.nyc_taxi_bronze.taxi_trips`),
# MAGIC que preserva 100% das linhas da origem - as anomalias ainda estao visiveis aqui
# MAGIC (a gold ja as removeu). Cada hipotese levantada na pesquisa e confirmada ou
# MAGIC refutada com uma query de contagem. Ao final, cruzamos com
# MAGIC `workspace.nyc_taxi_gold.dq_metrics` para quantificar o impacto da limpeza.

# COMMAND ----------

# spark e injetado pelo runtime Databricks.
# ruff: noqa: F821, E402

from pyspark.sql import functions as F

bronze = spark.table("workspace.nyc_taxi_bronze.taxi_trips")

# COMMAND ----------

# MAGIC %md ## 1. Volumetria por taxi_type e mes (vs valores esperados da origem)

# COMMAND ----------

ESPERADO_YELLOW = {
    "2023-01": 3_066_766,
    "2023-02": 2_913_955,
    "2023-03": 3_403_766,
    "2023-04": 3_288_250,
    "2023-05": 3_513_649,
}
ESPERADO_GREEN_TOTAL = 339_630

vol = {
    (r["taxi_type"], r["source_year_month"]): r["linhas"]
    for r in bronze.groupBy("taxi_type", "source_year_month")
    .agg(F.count("*").alias("linhas"))
    .collect()
}

print(f"{'mes':<10}{'yellow':>12}{'esperado':>12}  status")
for mes, esperado in ESPERADO_YELLOW.items():
    obtido = vol.get(("yellow", mes), 0)
    status = "OK" if obtido == esperado else "DIVERGE"
    print(f"{mes:<10}{obtido:>12}{esperado:>12}  {status}")

green_total = sum(v for (t, _), v in vol.items() if t == "green")
status_green = "OK" if green_total == ESPERADO_GREEN_TOTAL else "DIVERGE"
print(f"\ngreen total: {green_total} (esperado {ESPERADO_GREEN_TOTAL}) {status_green}")
print(f"total geral: {sum(vol.values())} (esperado 16526016)")

# COMMAND ----------

# MAGIC %md ## 2. Distribuicoes de total_amount, passenger_count e trip_distance

# COMMAND ----------

display(
    bronze.select("total_amount", "passenger_count", "trip_distance").summary(
        "count", "min", "25%", "50%", "75%", "max"
    )
)

# COMMAND ----------

# Percentis por taxi_type (aproximados) para ver a cauda das distribuicoes.
display(
    bronze.groupBy("taxi_type").agg(
        F.expr("percentile_approx(total_amount, array(0.01,0.5,0.99))").alias(
            "total_amount_p1_p50_p99"
        ),
        F.expr("percentile_approx(trip_distance, array(0.5,0.99,0.999))").alias(
            "trip_distance_p50_p99_p999"
        ),
        F.avg("passenger_count").alias("media_passageiros"),
    )
)

# COMMAND ----------

# MAGIC %md ## 3. Anomalias - uma hipotese por celula (confirma/refuta)

# COMMAND ----------

# Hipotese A: 71.743 nulls de passenger_count no yellow de jan/2023 (2,34%).
nulls_jan = bronze.filter(
    "taxi_type = 'yellow' AND source_year_month = '2023-01' "
    "AND passenger_count IS NULL"
).count()
print(f"nulls passenger_count yellow 2023-01: {nulls_jan} (esperado 71743)")
print("CONFIRMADA" if nulls_jan == 71_743 else "REFUTADA - investigar")

# Contexto adicional: nulls e zeros na base inteira (impacto da regra R4 da gold).
display(
    bronze.groupBy("taxi_type").agg(
        F.sum(F.expr("CAST(passenger_count IS NULL AS INT)")).alias("nulls"),
        F.sum(F.expr("CAST(passenger_count = 0 AS INT)")).alias("zeros"),
    )
)

# COMMAND ----------

# Hipotese B: pickups fora de Jan-Mai/2023 (minimo esperado: 2008-12-31).
fora_janela = bronze.filter(
    "tpep_pickup_datetime < TIMESTAMP_NTZ '2023-01-01 00:00:00' "
    "OR tpep_pickup_datetime >= TIMESTAMP_NTZ '2023-06-01 00:00:00'"
)
minimo = bronze.agg(F.min("tpep_pickup_datetime")).first()[0]
print(f"pickups fora da janela: {fora_janela.count()} | pickup minimo: {minimo}")
print("CONFIRMADA" if str(minimo).startswith("2008-12-31") else "REFUTADA - investigar")

# COMMAND ----------

# Hipotese C: total_amount negativo (estornos; minimo esperado -751.00).
negativos = bronze.filter("total_amount < 0")
minimo_total = bronze.agg(F.min("total_amount")).first()[0]
print(f"linhas total_amount < 0: {negativos.count()} | minimo: {minimo_total}")
print("CONFIRMADA" if minimo_total <= -751.0 else "REFUTADA - investigar")

# COMMAND ----------

# Hipotese D: payment_type = 0 (valor NAO documentado no dicionario TLC, 1-6).
pt0 = bronze.filter("payment_type = 0").count()
print(f"linhas payment_type = 0: {pt0}")
print("CONFIRMADA (valor fora do dicionario presente)" if pt0 > 0 else "REFUTADA")

# COMMAND ----------

# Hipotese E: RatecodeID = 99 (unknown).
rc99 = bronze.filter("RatecodeID = 99").count()
print(f"linhas RatecodeID = 99: {rc99}")
print("CONFIRMADA" if rc99 > 0 else "REFUTADA")

# COMMAND ----------

# Hipotese F: outlier de trip_distance (maximo esperado ~258.928 milhas).
max_dist = bronze.agg(F.max("trip_distance")).first()[0]
print(f"trip_distance maximo: {max_dist} milhas")
print("CONFIRMADA" if max_dist > 100_000 else "REFUTADA - investigar")

# COMMAND ----------

# MAGIC %md ## 4. Impacto da limpeza (cruzamento com dq_metrics da gold)

# COMMAND ----------

dq = spark.table("workspace.nyc_taxi_gold.dq_metrics")
display(dq.orderBy("taxi_type", "metrica"))

mets = {(r["taxi_type"], r["metrica"]): r["valor"] for r in dq.collect()}
for escopo in ["yellow", "green", "total"]:
    bronze_n = mets[(escopo, "linhas_bronze")]
    gold_n = mets[(escopo, "linhas_gold")]
    removidas = sum(
        v for (t, m), v in mets.items() if t == escopo and m.startswith("removidas_")
    )
    print(
        f"[{escopo}] bronze={bronze_n} gold={gold_n} removidas={removidas} "
        f"aproveitamento={gold_n / bronze_n:.4f} "
        f"reconciliacao={'OK' if bronze_n == gold_n + removidas else 'FALHOU'}"
    )

print(
    "\nNota: aproveitamento total ~94,7% - a regra R4 remove nulls E zeros de "
    "passenger_count (~4,3% do yellow); regra R4 documentada no README."
)
