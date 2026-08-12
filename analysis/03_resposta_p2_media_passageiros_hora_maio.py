# Databricks notebook source
# MAGIC %md
# MAGIC # Pergunta 2 - Media de passenger_count por hora do dia (maio/2023)
# MAGIC
# MAGIC Respondida em DOIS escopos lado a lado: `frota_completa` (yellow + green)
# MAGIC e `yellow`, calculada em **PySpark** (gold) e **SQL** (view
# MAGIC `vw_media_passageiros_hora_maio`), com paridade |dif| <= 0,01 nos 48 valores.
# MAGIC
# MAGIC Inclui a **dupla leitura** do resultado: ocupacao media por corrida
# MAGIC (`AVG`) vs demanda de passageiros por hora (`SUM/31 dias`), que respondem
# MAGIC perguntas de negocio diferentes e tem picos em horarios diferentes.

# COMMAND ----------

# spark e injetado pelo runtime Databricks.
# ruff: noqa: F821, E402

from pyspark.sql import functions as F

gold_maio = spark.table("workspace.nyc_taxi_gold.taxi_trips").filter(
    "pickup_year_month = '2023-05'"
)

# COMMAND ----------

# MAGIC %md ## Calculo 1 - PySpark (frota_completa e yellow lado a lado)

# COMMAND ----------

frota_py = gold_maio.groupBy("pickup_hour").agg(
    F.round(F.avg("passenger_count"), 3).alias("frota_pyspark")
)
yellow_py = (
    gold_maio.filter("taxi_type = 'yellow'")
    .groupBy("pickup_hour")
    .agg(F.round(F.avg("passenger_count"), 3).alias("yellow_pyspark"))
)
p2_pyspark = frota_py.join(yellow_py, "pickup_hour").orderBy("pickup_hour")
display(p2_pyspark)

# COMMAND ----------

# MAGIC %md ## Calculo 2 - SQL (view da gold, pivotada no mesmo formato)

# COMMAND ----------

p2_sql = spark.sql(
    """
    SELECT pickup_hour,
           MAX(CASE WHEN escopo = 'frota_completa' THEN media_passageiros END)
               AS frota_sql,
           MAX(CASE WHEN escopo = 'yellow' THEN media_passageiros END)
               AS yellow_sql
    FROM workspace.nyc_taxi_gold.vw_media_passageiros_hora_maio
    GROUP BY pickup_hour
    ORDER BY pickup_hour
    """
)
display(p2_sql)

# COMMAND ----------

# MAGIC %md ## Paridade SQL vs PySpark (48 valores: 24 horas x 2 escopos)

# COMMAND ----------

sql_map = {r["pickup_hour"]: r for r in p2_sql.collect()}
py_map = {r["pickup_hour"]: r for r in p2_pyspark.collect()}

print(
    f"{'hora':<6}{'frota_sql':>10}{'frota_py':>10}{'yellow_sql':>11}{'yellow_py':>10}  paridade"
)
falhas = 0
for hora in range(24):
    s, p = sql_map[hora], py_map[hora]
    dif_f = abs(s["frota_sql"] - p["frota_pyspark"])
    dif_y = abs(s["yellow_sql"] - p["yellow_pyspark"])
    ok = dif_f <= 0.01 and dif_y <= 0.01
    falhas += 0 if ok else 1
    print(
        f"{hora:<6}{s['frota_sql']:>10.3f}{p['frota_pyspark']:>10.3f}"
        f"{s['yellow_sql']:>11.3f}{p['yellow_pyspark']:>10.3f}"
        f"  {'OK' if ok else 'FALHOU'}"
    )
print(
    f"\nPARIDADE: {'OK nos 48 valores' if falhas == 0 else f'{falhas} hora(s) com falha'}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dupla leitura: ocupacao por corrida vs demanda por hora
# MAGIC
# MAGIC - **Ocupacao media por corrida** (`AVG(passenger_count)`): quantas pessoas
# MAGIC   ha, em media, dentro de um taxi que inicia corrida naquela hora. Pico
# MAGIC   esperado na madrugada (~2h): corridas de lazer/grupos saindo de bares.
# MAGIC - **Demanda de passageiros por hora** (`SUM(passenger_count)/31 dias`):
# MAGIC   quantas pessoas embarcam por hora em um dia tipico de maio. Pico esperado
# MAGIC   no fim da tarde (~18h): volume de corridas domina a conta.
# MAGIC
# MAGIC **Nuance metodologica**: `AVG()` ignora NULL - por isso o filtro da gold
# MAGIC (`passenger_count IS NOT NULL AND > 0`, regra R4) nao altera a media da
# MAGIC P2 em relacao a calcula-la na bronze ignorando nulls; ele altera a
# MAGIC `qtd_corridas` (denominadores) e a P1 (que usa todas as linhas validas).
# MAGIC O corte de zeros, por sua vez, evita puxar a ocupacao para baixo com
# MAGIC registros invalidos (0 passageiros).

# COMMAND ----------

DIAS_MAIO = 31
leituras = None
for escopo, cond in [("frota", "1=1"), ("yellow", "taxi_type = 'yellow'")]:
    df = (
        gold_maio.filter(cond)
        .groupBy("pickup_hour")
        .agg(
            F.round(F.avg("passenger_count"), 3).alias(f"ocupacao_{escopo}"),
            F.round(F.sum("passenger_count") / DIAS_MAIO, 1).alias(f"demanda_{escopo}"),
        )
    )
    leituras = df if leituras is None else leituras.join(df, "pickup_hour")
leituras = leituras.orderBy("pickup_hour")
display(leituras)

dados = leituras.collect()
for metrica in ["ocupacao_frota", "demanda_frota", "ocupacao_yellow", "demanda_yellow"]:
    pico = max(dados, key=lambda r: r[metrica])
    print(f"pico de {metrica}: {pico['pickup_hour']}h ({pico[metrica]})")

# COMMAND ----------

# MAGIC %md ## Resultado final (tabela consolidada)

# COMMAND ----------

print("| hora | frota_sql | frota_pyspark | yellow_sql | yellow_pyspark |")
print("|------|-----------|---------------|------------|----------------|")
for hora in range(24):
    s, p = sql_map[hora], py_map[hora]
    print(
        f"| {hora} | {s['frota_sql']:.3f} | {p['frota_pyspark']:.3f} "
        f"| {s['yellow_sql']:.3f} | {p['yellow_pyspark']:.3f} |"
    )
