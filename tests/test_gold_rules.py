"""Testes da logica gold: puros + Spark local com skip automatico."""

import sys
from datetime import datetime
from pathlib import Path

import pytest
from src.gold.build_gold import (
    GOLD_COLUMNS,
    JANELA_PICKUP,
    METRICAS,
    REGRAS_DQ,
    REJECTED_COLUMNS,
    calcular_metricas,
    classificar_dq,
    selecionar_schema_gold,
    selecionar_schema_rejected,
)

# ---------------------------------------------------------------------------
# Testes puros (rodam em qualquer Python, sem SparkSession)
# ---------------------------------------------------------------------------


def test_regras_dq_ordem_e_nomes() -> None:
    nomes = [nome for nome, _ in REGRAS_DQ]
    assert nomes == [
        "removidas_r1_pickup_fora_janela",
        "removidas_r2_dropoff_nao_posterior",
        "removidas_r3_total_amount_negativo",
        "removidas_r4_passenger_count_invalido",
    ]


def test_metricas_nomes_fixos() -> None:
    assert METRICAS == [
        "linhas_bronze",
        "removidas_r1_pickup_fora_janela",
        "removidas_r2_dropoff_nao_posterior",
        "removidas_r3_total_amount_negativo",
        "removidas_r4_passenger_count_invalido",
        "linhas_gold",
    ]


def test_janela_pickup_meio_aberta() -> None:
    assert JANELA_PICKUP == ("2023-01-01 00:00:00", "2023-06-01 00:00:00")
    viol_r1 = REGRAS_DQ[0][1]
    assert f"< TIMESTAMP_NTZ '{JANELA_PICKUP[0]}'" in viol_r1
    assert f">= TIMESTAMP_NTZ '{JANELA_PICKUP[1]}'" in viol_r1
    assert "IS NULL" in viol_r1


def test_gold_columns_8_colunas_nomes_e_tipos() -> None:
    assert list(GOLD_COLUMNS) == [
        "VendorID",
        "passenger_count",
        "total_amount",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "taxi_type",
        "pickup_year_month",
        "pickup_hour",
    ]
    assert GOLD_COLUMNS["VendorID"] == "int"
    assert GOLD_COLUMNS["passenger_count"] == "int"
    assert GOLD_COLUMNS["total_amount"] == "double"
    assert GOLD_COLUMNS["tpep_pickup_datetime"] == "timestamp_ntz"
    assert GOLD_COLUMNS["tpep_dropoff_datetime"] == "timestamp_ntz"
    assert GOLD_COLUMNS["taxi_type"] == "string"
    assert GOLD_COLUMNS["pickup_year_month"] == "string"
    assert GOLD_COLUMNS["pickup_hour"] == "int"


def test_rejected_columns_10_colunas_gold_mais_linhagem_e_motivo() -> None:
    esperado = [*GOLD_COLUMNS, "source_year_month", "_reject_reason"]
    assert list(REJECTED_COLUMNS) == esperado
    assert len(REJECTED_COLUMNS) == 10
    # As 8 primeiras sao exatamente as da gold, com os mesmos tipos.
    for coluna, tipo in GOLD_COLUMNS.items():
        assert REJECTED_COLUMNS[coluna] == tipo
    assert REJECTED_COLUMNS["source_year_month"] == "string"
    assert REJECTED_COLUMNS["_reject_reason"] == "string"


def test_motivos_possiveis_sao_metricas_conhecidas() -> None:
    """Nenhum _reject_reason pode ser orfao: todo motivo tem metrica em
    dq_metrics, senao a quarentena nao reconcilia com as contagens."""
    motivos = {nome for nome, _ in REGRAS_DQ}
    assert motivos.issubset(set(METRICAS))
    assert motivos == {m for m in METRICAS if m.startswith("removidas_")}


def test_notebook_espelha_o_modulo() -> None:
    """O bloco INICIO/FIM logica gold do notebook nao pode divergir do modulo."""
    notebook = Path(__file__).parent.parent / "src" / "gold" / "gold_taxi_trips.py"
    fonte = notebook.read_text(encoding="utf-8")
    inicio = fonte.index("# INICIO logica gold")
    fim = fonte.index("# FIM logica gold")
    bloco = fonte[inicio:fim]

    espaco: dict = {}
    exec(bloco, espaco)  # noqa: S102 - bloco controlado do proprio repo

    assert espaco["GOLD_COLUMNS"] == GOLD_COLUMNS
    assert espaco["REJECTED_COLUMNS"] == REJECTED_COLUMNS
    assert espaco["JANELA_PICKUP"] == JANELA_PICKUP
    assert espaco["REGRAS_DQ"] == REGRAS_DQ
    assert espaco["METRICAS"] == METRICAS


# ---------------------------------------------------------------------------
# Testes com Spark local (skip automatico: Python >= 3.14 ou sessao indisponivel)
# ---------------------------------------------------------------------------


def _java_major() -> int | None:
    """Versao major do Java no PATH (None se ausente/indetectavel)."""
    import re
    import subprocess

    try:
        saida = subprocess.run(
            ["java", "-version"], capture_output=True, text=True, timeout=30
        ).stderr
        m = re.search(r'version "(\d+)(?:\.(\d+))?', saida)
        if not m:
            return None
        major = int(m.group(1))
        return int(m.group(2)) if major == 1 and m.group(2) else major
    except Exception:
        return None


@pytest.fixture(scope="module")
def spark():
    if sys.version_info >= (3, 14):
        pytest.skip("pyspark 4.x nao suporta Python >= 3.14")
    java = _java_major()
    if java is None or java < 17:
        pytest.skip(
            f"Spark 4.x exige Java 17+ (Java detectado: {java}); "
            "testes Spark locais pulados"
        )
    try:
        from pyspark.sql import SparkSession

        sessao = (
            SparkSession.builder.master("local[1]")
            .appName("test_gold_rules")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception as exc:  # pragma: no cover - depende do ambiente (JVM)
        pytest.skip(f"Spark local indisponivel: {exc}")
    yield sessao
    sessao.stop()


@pytest.fixture(scope="module")
def df_mini(spark):
    """1 linha limpa + 1 violacao de cada regra + 1 violando R1 E R4 (deve
    contar SO em R1) + 1 linha green limpa."""
    linhas = [
        # (VendorID, pickup, dropoff, passenger_count, total_amount, taxi_type,
        #  source_year_month = mes do ARQUIVO de origem)
        (1, "2023-03-10 08:00", "2023-03-10 08:20", 1, 20.0, "yellow", "2023-03"),
        (2, "2008-12-31 23:00", "2023-01-01 01:00", 1, 10.0, "yellow", "2023-01"),
        (1, "2023-05-05 12:00", "2023-05-05 12:00", 2, 15.0, "yellow", "2023-05"),
        (2, "2023-02-01 07:00", "2023-02-01 07:30", 1, -751.0, "yellow", "2023-02"),
        (1, "2023-04-15 18:00", "2023-04-15 18:40", 0, 30.0, "yellow", "2023-04"),
        (2, "2030-01-01 00:00", "2030-01-01 00:30", None, 25.0, "yellow", "2023-05"),
        (1, "2023-05-20 22:00", "2023-05-20 22:45", 3, 40.0, "green", "2023-05"),
    ]
    df = spark.createDataFrame(
        [
            (
                v,
                datetime.fromisoformat(pu),
                datetime.fromisoformat(do),
                pc,
                ta,
                tt,
                sym,
            )
            for v, pu, do, pc, ta, tt, sym in linhas
        ],
        "VendorID int, tpep_pickup_datetime timestamp, tpep_dropoff_datetime "
        "timestamp, passenger_count int, total_amount double, taxi_type string, "
        "source_year_month string",
    )
    return df.selectExpr(
        "VendorID",
        "CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ) AS tpep_pickup_datetime",
        "CAST(tpep_dropoff_datetime AS TIMESTAMP_NTZ) AS tpep_dropoff_datetime",
        "passenger_count",
        "total_amount",
        "taxi_type",
        "source_year_month",
    )


def test_classificacao_primeira_regra_violada(df_mini) -> None:
    resultado = {
        (linha["VendorID"], linha["tpep_pickup_datetime"].isoformat()): linha[
            "dq_regra_violada"
        ]
        for linha in classificar_dq(df_mini).collect()
    }
    assert resultado[(1, "2023-03-10T08:00:00")] is None
    assert resultado[(2, "2008-12-31T23:00:00")] == "removidas_r1_pickup_fora_janela"
    assert resultado[(1, "2023-05-05T12:00:00")] == "removidas_r2_dropoff_nao_posterior"
    assert resultado[(2, "2023-02-01T07:00:00")] == "removidas_r3_total_amount_negativo"
    assert (
        resultado[(1, "2023-04-15T18:00:00")] == "removidas_r4_passenger_count_invalido"
    )
    # Viola R1 (2030) e R4 (null): conta SO em R1 (primeira violada).
    assert resultado[(2, "2030-01-01T00:00:00")] == "removidas_r1_pickup_fora_janela"
    assert resultado[(1, "2023-05-20T22:00:00")] is None


def test_metricas_e_reconciliacao(df_mini) -> None:
    df_classificado = classificar_dq(df_mini)
    metricas = calcular_metricas(df_classificado)
    valores = {
        (linha["taxi_type"], linha["metrica"]): linha["valor"]
        for linha in metricas.collect()
    }
    assert valores[("yellow", "linhas_bronze")] == 6
    assert valores[("yellow", "removidas_r1_pickup_fora_janela")] == 2
    assert valores[("yellow", "removidas_r2_dropoff_nao_posterior")] == 1
    assert valores[("yellow", "removidas_r3_total_amount_negativo")] == 1
    assert valores[("yellow", "removidas_r4_passenger_count_invalido")] == 1
    assert valores[("yellow", "linhas_gold")] == 1
    assert valores[("green", "linhas_bronze")] == 1
    assert valores[("green", "linhas_gold")] == 1
    assert valores[("total", "linhas_bronze")] == 7
    assert valores[("total", "linhas_gold")] == 2
    # Reconciliacao em todos os escopos.
    for escopo in ["yellow", "green", "total"]:
        removidas = sum(
            v
            for (t, m), v in valores.items()
            if t == escopo and m.startswith("removidas_")
        )
        assert (
            valores[(escopo, "linhas_bronze")]
            == valores[(escopo, "linhas_gold")] + removidas
        ), escopo
    # 6 metricas x 3 escopos = 18 linhas.
    assert metricas.count() == 18


def test_schema_gold_8_colunas(df_mini) -> None:
    df_gold = selecionar_schema_gold(
        classificar_dq(df_mini).filter("dq_regra_violada IS NULL")
    )
    assert df_gold.columns == list(GOLD_COLUMNS)
    tipos = dict(df_gold.dtypes)
    assert tipos["pickup_year_month"] == "string"
    assert tipos["pickup_hour"] == "int"
    assert tipos["total_amount"] == "double"
    linhas = {linha["taxi_type"]: linha for linha in df_gold.collect()}
    assert linhas["yellow"]["pickup_year_month"] == "2023-03"
    assert linhas["yellow"]["pickup_hour"] == 8
    assert linhas["green"]["pickup_year_month"] == "2023-05"
    assert linhas["green"]["pickup_hour"] == 22


def test_quarentena_complementar_e_disjunta(df_mini) -> None:
    """Gold e quarentena particionam a entrada: soma bate, nenhuma linha em
    ambas, todo rejeitado tem motivo e todo motivo e uma regra conhecida."""
    df_classificado = classificar_dq(df_mini)
    df_gold = selecionar_schema_gold(df_classificado.filter("dq_regra_violada IS NULL"))
    df_rejected = selecionar_schema_rejected(
        df_classificado.filter("dq_regra_violada IS NOT NULL")
    )

    assert df_rejected.columns == list(REJECTED_COLUMNS)
    assert df_gold.count() + df_rejected.count() == df_mini.count()
    assert df_rejected.filter("_reject_reason IS NULL").count() == 0

    motivos = {linha["_reject_reason"] for linha in df_rejected.collect()}
    assert motivos == {nome for nome, _ in REGRAS_DQ}

    # A linhagem sobrevive: a corrida de 2008 veio do arquivo de janeiro.
    de_2008 = df_rejected.filter("VendorID = 2 AND total_amount = 10.0").collect()[0]
    assert de_2008["source_year_month"] == "2023-01"
    assert de_2008["_reject_reason"] == "removidas_r1_pickup_fora_janela"

    # Derivadas continuam calculadas a partir do pickup, mesmo invalido.
    assert de_2008["pickup_year_month"] == "2008-12"
