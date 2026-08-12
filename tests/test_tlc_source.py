"""Testes da logica de ingestao: puros, sem rede e sem SparkSession."""

from pathlib import Path

import pytest
from src.ingestion.tlc_source import (
    BASE_URL,
    MESES,
    TAXI_TYPES,
    TOTAIS_ESPERADOS_BYTES,
    caminho_destino,
    montar_url,
    nome_arquivo,
    precisa_baixar,
)


def test_escopo_do_case() -> None:
    assert TAXI_TYPES == ["yellow", "green"]
    assert MESES == ["2023-01", "2023-02", "2023-03", "2023-04", "2023-05"]
    assert BASE_URL == "https://d37ci6vzurychx.cloudfront.net/trip-data"


def test_nome_arquivo_preserva_o_nome_da_origem() -> None:
    assert nome_arquivo("yellow", "2023-01") == "yellow_tripdata_2023-01.parquet"
    assert nome_arquivo("green", "2023-05") == "green_tripdata_2023-05.parquet"


def test_montar_url() -> None:
    assert montar_url("yellow", "2023-03") == (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/"
        "yellow_tripdata_2023-03.parquet"
    )
    # Barra final na base nao pode duplicar a barra da URL.
    assert montar_url("green", "2023-02", "https://exemplo/trip-data/") == (
        "https://exemplo/trip-data/green_tripdata_2023-02.parquet"
    )


def test_caminho_destino_usa_barra_normal_e_o_ano_do_mes() -> None:
    assert caminho_destino(
        "/Volumes/workspace/nyc_taxi_landing/files", "yellow", "2023-04"
    ) == (
        "/Volumes/workspace/nyc_taxi_landing/files/yellow/2023/"
        "yellow_tripdata_2023-04.parquet"
    )
    # Raiz com barra final nao duplica separador.
    assert caminho_destino("data/landing/", "green", "2023-01") == (
        "data/landing/green/2023/green_tripdata_2023-01.parquet"
    )
    # Nunca separador do Windows: o destino pode ser um Volume.
    assert "\\" not in caminho_destino("data/landing", "yellow", "2023-01")


@pytest.mark.parametrize(
    ("tamanho_local", "content_length", "esperado"),
    [
        (None, 1427002, True),  # arquivo ausente
        (0, 1427002, True),  # arquivo vazio (download interrompido no inicio)
        (1000, 1427002, True),  # download parcial: autocorretivo
        (1427003, 1427002, True),  # tamanho maior que o da origem
        (1427002, 1427002, False),  # integro: unico caso que pula
        (1427002, None, True),  # origem nao informou tamanho: nao da para afirmar
        (None, None, True),
    ],
)
def test_precisa_baixar(
    tamanho_local: int | None, content_length: int | None, esperado: bool
) -> None:
    assert precisa_baixar(tamanho_local, content_length) is esperado


def test_totais_esperados_por_frota() -> None:
    """Invariante da carga completa, conferido na ingestao original."""
    assert TOTAIS_ESPERADOS_BYTES == {"yellow": 264426470, "green": 7936049}
    assert set(TOTAIS_ESPERADOS_BYTES) == set(TAXI_TYPES)


def test_notebook_espelha_o_modulo() -> None:
    """O bloco INICIO/FIM do notebook nao pode divergir do modulo."""
    notebook = (
        Path(__file__).parent.parent / "src" / "ingestion" / "ingestao_landing.py"
    )
    fonte = notebook.read_text(encoding="utf-8")
    inicio = fonte.index("# INICIO logica de ingestao")
    fim = fonte.index("# FIM logica de ingestao")
    bloco = fonte[inicio:fim]

    espaco: dict = {}
    exec(bloco, espaco)  # noqa: S102 - bloco controlado do proprio repo

    assert espaco["BASE_URL"] == BASE_URL
    assert espaco["TAXI_TYPES"] == TAXI_TYPES
    assert espaco["MESES"] == MESES
    assert espaco["TOTAIS_ESPERADOS_BYTES"] == TOTAIS_ESPERADOS_BYTES
    # As funcoes precisam produzir os mesmos resultados, nao so existir.
    for taxi_type in TAXI_TYPES:
        for ano_mes in MESES:
            assert espaco["nome_arquivo"](taxi_type, ano_mes) == nome_arquivo(
                taxi_type, ano_mes
            )
            assert espaco["montar_url"](taxi_type, ano_mes) == montar_url(
                taxi_type, ano_mes
            )
            assert espaco["caminho_destino"]("/x", taxi_type, ano_mes) == (
                caminho_destino("/x", taxi_type, ano_mes)
            )
    assert espaco["precisa_baixar"](None, 10) is True
    assert espaco["precisa_baixar"](10, 10) is False
