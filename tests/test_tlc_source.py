"""Testes da logica de ingestao: puros, sem rede e sem SparkSession."""

from pathlib import Path

import pytest
from src.ingestion.tlc_source import (
    BASE_URL,
    MESES,
    TAXI_TYPES,
    TOTAIS_ESPERADOS_BYTES,
    caminho_destino,
    landing_integra,
    montar_url,
    nome_arquivo,
    precisa_baixar,
)

# Bytes reais de cada arquivo na origem: a soma por frota e o invariante.
TAMANHOS_OK = {
    "yellow": [47673370, 47748012, 56127762, 54222699, 58654627],
    "green": [1427002, 1533740, 1730999, 1570467, 1673841],
}


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


def test_landing_integra_reconhece_a_carga_completa() -> None:
    """Escopo completo, arquivos presentes e totais batendo: dispensa a rede."""
    assert landing_integra(TAMANHOS_OK, True) is True
    # Os totais do fixture sao exatamente os invariantes por frota.
    for frota, tamanhos in TAMANHOS_OK.items():
        assert sum(tamanhos) == TOTAIS_ESPERADOS_BYTES[frota]


@pytest.mark.parametrize(
    ("tamanhos", "escopo_completo"),
    [
        ({}, True),  # sem frota nenhuma: nao da para afirmar nada
        ({"yellow": TAMANHOS_OK["yellow"]}, False),  # escopo parcial
        # Arquivo ausente na landing.
        ({"green": [1427002, 1533740, 1730999, 1570467, None]}, True),
        # Download parcial: total diverge do invariante.
        ({"green": [1427002, 1533740, 1730999, 1570467, 1]}, True),
        # Frota fora do invariante conhecido.
        ({"fhv": [1, 2, 3, 4, 5]}, True),
        ({"green": []}, True),  # frota sem arquivo algum
    ],
)
def test_landing_integra_recusa_o_que_nao_da_para_afirmar(
    tamanhos: dict[str, list[int | None]], escopo_completo: bool
) -> None:
    assert landing_integra(tamanhos, escopo_completo) is False


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
    assert espaco["landing_integra"](TAMANHOS_OK, True) is True
    assert espaco["landing_integra"](TAMANHOS_OK, False) is False
