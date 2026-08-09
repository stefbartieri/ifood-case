"""Testes do schema canonico da bronze — sem SparkSession."""

from pathlib import Path

from src.bronze.schema_canonico import (
    COLUNAS_AUSENTES,
    COLUNAS_CANONICAS,
    COLUNAS_LINHAGEM,
    RENAMES,
    TIPOS_CANONICOS,
    colunas_esperadas,
)

MESES = ["2023-01", "2023-02", "2023-03", "2023-04", "2023-05"]


def test_24_colunas_na_ordem_canonica() -> None:
    assert len(COLUNAS_CANONICAS) == 24
    assert COLUNAS_CANONICAS[:3] == [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
    ]
    assert COLUNAS_CANONICAS[-3:] == ["taxi_type", "source_year_month", "ingested_at"]
    assert len(set(COLUNAS_CANONICAS)) == 24


def test_tipos_canonicos_cobrem_todas_as_colunas() -> None:
    assert set(TIPOS_CANONICOS) == set(COLUNAS_CANONICAS)
    assert TIPOS_CANONICOS["tpep_pickup_datetime"] == "timestamp_ntz"
    assert TIPOS_CANONICOS["tpep_dropoff_datetime"] == "timestamp_ntz"
    assert TIPOS_CANONICOS["ingested_at"] == "timestamp"
    for col in [
        "VendorID",
        "passenger_count",
        "RatecodeID",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "trip_type",
    ]:
        assert TIPOS_CANONICOS[col] == "int", col
    for col in [
        "trip_distance",
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
    ]:
        assert TIPOS_CANONICOS[col] == "double", col
    for col in ["store_and_fwd_flag", "taxi_type", "source_year_month"]:
        assert TIPOS_CANONICOS[col] == "string", col


def test_renames_cobrem_airport_fee_e_lpep() -> None:
    assert RENAMES["yellow"] == {"Airport_fee": "airport_fee"}
    assert RENAMES["green"] == {
        "lpep_pickup_datetime": "tpep_pickup_datetime",
        "lpep_dropoff_datetime": "tpep_dropoff_datetime",
    }


def test_colunas_ausentes_por_taxi_type() -> None:
    assert COLUNAS_AUSENTES["yellow"] == ["ehail_fee", "trip_type"]
    assert COLUNAS_AUSENTES["green"] == ["airport_fee"]


def test_colunas_esperadas_yellow_drift_airport_fee() -> None:
    jan = colunas_esperadas("yellow", "2023-01")
    fev = colunas_esperadas("yellow", "2023-02")
    assert len(jan) == 19
    assert "airport_fee" in jan and "Airport_fee" not in jan
    for mes in MESES[1:]:
        esperado = colunas_esperadas("yellow", mes)
        assert len(esperado) == 19
        assert "Airport_fee" in esperado and "airport_fee" not in esperado
    assert jan - {"airport_fee"} == fev - {"Airport_fee"}


def test_colunas_esperadas_green_estavel_nos_5_meses() -> None:
    conjuntos = [colunas_esperadas("green", mes) for mes in MESES]
    assert all(c == conjuntos[0] for c in conjuntos)
    green = conjuntos[0]
    assert len(green) == 20
    assert {
        "lpep_pickup_datetime",
        "lpep_dropoff_datetime",
        "ehail_fee",
        "trip_type",
    } <= green
    assert "airport_fee" not in green and "Airport_fee" not in green


def test_taxi_type_desconhecido_falha() -> None:
    try:
        colunas_esperadas("fhv", "2023-01")
    except ValueError:
        return
    raise AssertionError("taxi_type invalido deveria levantar ValueError")


def test_notebook_espelha_o_modulo() -> None:
    """O bloco INICIO/FIM schema canonico do notebook nao pode divergir daqui."""
    notebook = Path(__file__).parent.parent / "src" / "bronze" / "bronze_taxi_trips.py"
    fonte = notebook.read_text(encoding="utf-8")
    inicio = fonte.index("# INICIO schema canonico")
    fim = fonte.index("# FIM schema canonico")
    bloco = fonte[inicio:fim]

    espaco: dict = {}
    exec(bloco, espaco)  # noqa: S102 - bloco controlado do proprio repo

    assert espaco["COLUNAS_CANONICAS"] == COLUNAS_CANONICAS
    assert espaco["TIPOS_CANONICOS"] == TIPOS_CANONICOS
    assert espaco["RENAMES"] == RENAMES
    assert espaco["COLUNAS_AUSENTES"] == COLUNAS_AUSENTES
    assert espaco["COLUNAS_LINHAGEM"] == COLUNAS_LINHAGEM
    for taxi_type in ["yellow", "green"]:
        for mes in MESES:
            assert espaco["colunas_esperadas"](taxi_type, mes) == colunas_esperadas(
                taxi_type, mes
            ), (taxi_type, mes)
