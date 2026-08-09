# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — workspace.nyc_taxi_bronze.taxi_trips
# MAGIC
# MAGIC Le os parquets ORIGINAIS da landing (`/Volumes/workspace/nyc_taxi_landing/files/`),
# MAGIC mes a mes (NUNCA `read.parquet(dir/*)` — ha schema drift real entre 2023-01 e
# MAGIC 2023-02..05), aplica renames + casts explicitos para o schema canonico de 24
# MAGIC colunas e grava a tabela Delta particionada por (`taxi_type`, `source_year_month`).
# MAGIC
# MAGIC **Nenhuma linha e filtrada** — a bronze preserva 100% das linhas da origem;
# MAGIC limpeza acontece somente na gold. Reexecucao e idempotente via
# MAGIC `replaceWhere` restrito as particoes processadas.
# MAGIC
# MAGIC Widgets: `taxi_types` (default `yellow,green`) e `meses` (default
# MAGIC `2023-01..2023-05`) permitem reprocessamento seletivo por particao.

# COMMAND ----------

# spark e dbutils sao injetados pelo runtime Databricks; o bloco espelhado do
# schema canonico e validado por pytest contra src/bronze/schema_canonico.py.
# ruff: noqa: F821, E402

dbutils.widgets.text("taxi_types", "yellow,green")
dbutils.widgets.text("meses", "2023-01,2023-02,2023-03,2023-04,2023-05")

taxi_types = [
    t.strip() for t in dbutils.widgets.get("taxi_types").split(",") if t.strip()
]
meses = [m.strip() for m in dbutils.widgets.get("meses").split(",") if m.strip()]

print(f"taxi_types={taxi_types} meses={meses}")

# COMMAND ----------

# INICIO schema canonico (espelho de src/bronze/schema_canonico.py — nao editar
# aqui sem atualizar o modulo; tests/test_schema_canonico.py compara os dois)

COLUNAS_CANONICAS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
    "ehail_fee",
    "trip_type",
    "taxi_type",
    "source_year_month",
    "ingested_at",
]

TIPOS_CANONICOS = {
    "VendorID": "int",
    "tpep_pickup_datetime": "timestamp_ntz",
    "tpep_dropoff_datetime": "timestamp_ntz",
    "passenger_count": "int",
    "trip_distance": "double",
    "RatecodeID": "int",
    "store_and_fwd_flag": "string",
    "PULocationID": "int",
    "DOLocationID": "int",
    "payment_type": "int",
    "fare_amount": "double",
    "extra": "double",
    "mta_tax": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "improvement_surcharge": "double",
    "total_amount": "double",
    "congestion_surcharge": "double",
    "airport_fee": "double",
    "ehail_fee": "double",
    "trip_type": "int",
    "taxi_type": "string",
    "source_year_month": "string",
    "ingested_at": "timestamp",
}

RENAMES = {
    "yellow": {"Airport_fee": "airport_fee"},
    "green": {
        "lpep_pickup_datetime": "tpep_pickup_datetime",
        "lpep_dropoff_datetime": "tpep_dropoff_datetime",
    },
}

COLUNAS_AUSENTES = {
    "yellow": ["ehail_fee", "trip_type"],
    "green": ["airport_fee"],
}

COLUNAS_LINHAGEM = ["taxi_type", "source_year_month", "ingested_at"]

_COLUNAS_COMUNS = [
    "VendorID",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
]


def colunas_esperadas(taxi_type: str, ano_mes: str) -> set[str]:
    """Colunas de entrada validas para (taxi_type, ano_mes) — drift conhecido."""
    if taxi_type == "yellow":
        airport = "airport_fee" if ano_mes == "2023-01" else "Airport_fee"
        return set(_COLUNAS_COMUNS) | {
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            airport,
        }
    if taxi_type == "green":
        return set(_COLUNAS_COMUNS) | {
            "lpep_pickup_datetime",
            "lpep_dropoff_datetime",
            "ehail_fee",
            "trip_type",
        }
    raise ValueError(f"taxi_type desconhecido: {taxi_type!r}")


# FIM schema canonico

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.nyc_taxi_bronze")

# COMMAND ----------

# Pre-verificacao: todos os arquivos esperados devem existir no Volume antes
# de qualquer escrita.

VOLUME_BASE = "/Volumes/workspace/nyc_taxi_landing/files"

esperados = {
    (t, m): f"{VOLUME_BASE}/{t}/2023/{t}_tripdata_{m}.parquet"
    for t in taxi_types
    for m in meses
}

faltando = []
for path in esperados.values():
    try:
        dbutils.fs.ls(path)
    except Exception:
        faltando.append(path)

if faltando:
    raise FileNotFoundError(
        "Arquivos esperados ausentes no Volume (confira a carga da landing): "
        + ", ".join(sorted(faltando))
    )

print(f"Pre-verificacao ok: {len(esperados)} arquivos encontrados no Volume")

# COMMAND ----------

# Transformacao: le mes a mes, valida colunas (drift novo => falha), aplica
# renames + casts explicitos, adiciona linhagem e une com unionByName.

from functools import reduce

from pyspark.sql import functions as F

dfs = []
for taxi_type in taxi_types:
    for ano_mes in meses:
        path = esperados[(taxi_type, ano_mes)]
        df = spark.read.parquet(path)

        entrada = set(df.columns)
        validas = colunas_esperadas(taxi_type, ano_mes)
        if entrada != validas:
            raise ValueError(
                f"Drift de schema NAO mapeado em {path}: "
                f"colunas inesperadas={sorted(entrada - validas)}; "
                f"colunas faltantes={sorted(validas - entrada)}"
            )

        for origem, destino in RENAMES[taxi_type].items():
            if origem in df.columns:
                df = df.withColumnRenamed(origem, destino)

        for col in COLUNAS_AUSENTES[taxi_type]:
            df = df.withColumn(col, F.lit(None).cast(TIPOS_CANONICOS[col]))

        for col in COLUNAS_CANONICAS:
            if col in COLUNAS_LINHAGEM:
                continue
            df = df.withColumn(col, F.col(col).cast(TIPOS_CANONICOS[col]))

        df = (
            df.withColumn("taxi_type", F.lit(taxi_type))
            .withColumn("source_year_month", F.lit(ano_mes))
            .withColumn("ingested_at", F.current_timestamp())
            .select(*COLUNAS_CANONICAS)
        )
        dfs.append(df)
        print(f"[ok] {taxi_type} {ano_mes}: transformado ({path})")

df_bronze = reduce(lambda a, b: a.unionByName(b), dfs)

# COMMAND ----------

# Escrita Delta idempotente: overwrite restrito as particoes processadas.

replace_where = " OR ".join(
    f"(taxi_type = '{t}' AND source_year_month = '{m}')"
    for t in taxi_types
    for m in meses
)
print(f"replaceWhere: {replace_where}")

(
    df_bronze.write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", replace_where)
    .partitionBy("taxi_type", "source_year_month")
    .saveAsTable("workspace.nyc_taxi_bronze.taxi_trips")
)

print("Gravacao concluida em workspace.nyc_taxi_bronze.taxi_trips")

# COMMAND ----------

# Conferencia final: contagem por particao e schema da tabela.

spark.sql(
    """
    SELECT taxi_type, source_year_month, COUNT(*) AS linhas
    FROM workspace.nyc_taxi_bronze.taxi_trips
    GROUP BY taxi_type, source_year_month
    ORDER BY taxi_type, source_year_month
    """
).show(50, truncate=False)

spark.table("workspace.nyc_taxi_bronze.taxi_trips").printSchema()
