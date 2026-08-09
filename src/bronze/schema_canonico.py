"""Schema canonico da camada bronze — Python puro, sem pyspark.

Fonte de verdade do mapeamento landing -> bronze: 24 colunas (21 de dados +
3 de linhagem), renames por taxi_type e colunas ausentes preenchidas com NULL.
O notebook src/bronze/bronze_taxi_trips.py espelha estas estruturas (bloco
delimitado por INICIO/FIM schema canonico) e o teste
tests/test_schema_canonico.py garante que os dois nao divergem.
"""

# Lista ordenada das 24 colunas da tabela bronze (ordem canonica).
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

# Tipo canonico de cada coluna (tipos SQL do Spark, em minusculas).
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

# Renames aplicados na leitura, por taxi_type (nome origem -> nome canonico).
RENAMES = {
    "yellow": {"Airport_fee": "airport_fee"},
    "green": {
        "lpep_pickup_datetime": "tpep_pickup_datetime",
        "lpep_dropoff_datetime": "tpep_dropoff_datetime",
    },
}

# Colunas canonicas que NAO existem na origem de cada taxi_type (viram NULL).
COLUNAS_AUSENTES = {
    "yellow": ["ehail_fee", "trip_type"],
    "green": ["airport_fee"],
}

# Colunas de linhagem adicionadas pelo pipeline (nao vem da origem).
COLUNAS_LINHAGEM = ["taxi_type", "source_year_month", "ingested_at"]

# Colunas de dados comuns aos dois tipos, nos nomes ORIGINAIS da origem
# (sem renames; a grafia de airport_fee/lpep_* e tratada por taxi_type/mes).
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
    """Conjunto de colunas de entrada validas para (taxi_type, ano_mes).

    Reflete o drift REAL verificado nos arquivos 2023-01..2023-05:
    - yellow: 19 colunas; a taxa de aeroporto e 'airport_fee' em 2023-01 e
      'Airport_fee' em 2023-02..05;
    - green: 20 colunas (lpep_*, ehail_fee e trip_type; sem airport_fee),
      mesma grafia em todos os meses.
    Qualquer coluna fora deste conjunto e drift novo => o notebook falha.
    """
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
