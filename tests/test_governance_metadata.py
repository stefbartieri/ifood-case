"""Testes dos metadados do catalogo: puros, sem SparkSession e sem workspace.

Garantem que o SQL de governanca cobre todas as colunas reais do projeto e que
o arquivo respeita as duas convencoes que o executor exige (sem apostrofo e sem
ponto-e-virgula dentro de literais).
"""

import re
from pathlib import Path

from src.bronze.schema_canonico import COLUNAS_CANONICAS
from src.gold.build_gold import GOLD_COLUMNS, REJECTED_COLUMNS

RAIZ = Path(__file__).parent.parent
SQL_GOVERNANCA = RAIZ / "src" / "governance" / "catalog_metadata.sql"
SQL_VIEWS = RAIZ / "src" / "gold" / "sql" / "create_views.sql"
NOTEBOOK = RAIZ / "src" / "governance" / "aplicar_metadados.py"

TEXTO = SQL_GOVERNANCA.read_text(encoding="utf-8")

# Colunas de dq_metrics (definidas no schema de escrita do notebook da gold).
COLUNAS_DQ_METRICS = ["run_ts", "metrica", "taxi_type", "valor"]

TAGS_ESPERADAS = {"camada", "dominio", "fonte", "contem_pii", "projeto"}


def _colunas_comentadas(tabela: str) -> set[str]:
    """Colunas que recebem COMMENT via ALTER TABLE para a tabela informada."""
    padrao = re.compile(
        rf"ALTER TABLE\s+{re.escape(tabela)}\s+ALTER COLUMN\s+`?(\w+)`?",
        re.IGNORECASE,
    )
    return {m.group(1) for m in padrao.finditer(TEXTO)}


def test_todas_as_colunas_da_bronze_comentadas() -> None:
    comentadas = _colunas_comentadas("workspace.nyc_taxi_bronze.taxi_trips")
    assert comentadas == set(COLUNAS_CANONICAS)
    assert len(comentadas) == 24


def test_todas_as_colunas_da_gold_comentadas() -> None:
    comentadas = _colunas_comentadas("workspace.nyc_taxi_gold.taxi_trips")
    assert comentadas == set(GOLD_COLUMNS)
    assert len(comentadas) == 8


def test_todas_as_colunas_da_quarentena_comentadas() -> None:
    comentadas = _colunas_comentadas("workspace.nyc_taxi_gold.taxi_trips_rejected")
    assert comentadas == set(REJECTED_COLUMNS)
    assert len(comentadas) == 10


def test_todas_as_colunas_de_dq_metrics_comentadas() -> None:
    comentadas = _colunas_comentadas("workspace.nyc_taxi_gold.dq_metrics")
    assert comentadas == set(COLUNAS_DQ_METRICS)


def test_schemas_tabelas_e_volume_comentados() -> None:
    for objeto in (
        "SCHEMA workspace.nyc_taxi_landing",
        "SCHEMA workspace.nyc_taxi_bronze",
        "SCHEMA workspace.nyc_taxi_gold",
        "VOLUME workspace.nyc_taxi_landing.files",
        "TABLE workspace.nyc_taxi_bronze.taxi_trips",
        "TABLE workspace.nyc_taxi_gold.taxi_trips",
        "TABLE workspace.nyc_taxi_gold.dq_metrics",
        "TABLE workspace.nyc_taxi_gold.taxi_trips_rejected",
    ):
        assert f"COMMENT ON {objeto} IS" in TEXTO, objeto


def test_vocabulario_de_tags_fixo() -> None:
    chaves = set(re.findall(r"'(\w+)'\s*=\s*'", TEXTO))
    assert chaves == TAGS_ESPERADAS
    # As 4 tabelas recebem tags.
    assert TEXTO.count("SET TAGS") == 4


def test_sem_apostrofo_dentro_dos_literais() -> None:
    """Apostrofo em portugues quebraria a string SQL."""
    for i, linha in enumerate(TEXTO.splitlines(), 1):
        if linha.strip().startswith("--"):
            continue
        # Numero de aspas simples por linha tem de ser par (abre e fecha).
        assert linha.count("'") % 2 == 0, f"aspas impares na linha {i}: {linha[:60]}"


def test_sem_ponto_e_virgula_dentro_dos_literais() -> None:
    """O executor divide por ';' - um ';' dentro de literal partiria o comando."""
    for i, linha in enumerate(TEXTO.splitlines(), 1):
        if linha.strip().startswith("--"):
            continue
        literais = re.findall(r"'([^']*)'", linha)
        for literal in literais:
            assert ";" not in literal, f"';' em literal na linha {i}: {literal[:60]}"


def test_views_tem_comentarios_inline() -> None:
    """Comentarios das views moram na definicao: CREATE OR REPLACE as recria."""
    views = SQL_VIEWS.read_text(encoding="utf-8")
    for coluna in (
        "pickup_year_month",
        "media_total_amount",
        "qtd_corridas",
        "escopo",
        "pickup_hour",
        "media_passageiros",
    ):
        assert re.search(rf"{coluna}\s+COMMENT\s+'", views), coluna
    assert views.count("COMMENT '") >= 9  # 7 colunas + 2 views


def test_parser_ignora_ponto_e_virgula_em_comentario() -> None:
    """Regressao: dividir por ';' antes de remover comentarios quebra comandos."""
    fonte = NOTEBOOK.read_text(encoding="utf-8")
    inicio = fonte.index("def dividir_comandos")
    fim = fonte.index("comandos = dividir_comandos")
    espaco: dict = {}
    exec(fonte[inicio:fim], espaco)  # noqa: S102 - bloco do proprio repo

    sql = (
        "-- comentario com ; no meio\n"
        "COMMENT ON TABLE a IS 'texto';\n"
        "-- outro ; comentario\n"
        "ALTER TABLE b ALTER COLUMN c COMMENT 'x';\n"
    )
    assert len(espaco["dividir_comandos"](sql)) == 2


def test_sql_de_governanca_cobre_46_colunas() -> None:
    """Total conferido: 24 bronze + 8 gold + 4 metricas + 10 quarentena."""
    assert len(re.findall(r"ALTER COLUMN", TEXTO)) == 46
