#!/usr/bin/env bash
# Utilitario do projeto (macOS/Linux).
# Uso: ./scripts/projeto.sh <acao>
# Acoes: setup | dados | qualidade | deploy

set -euo pipefail
cd "$(dirname "$0")/.."
VENV_PYTHON=".venv/bin/python"

setup() {
    if [ ! -x "$VENV_PYTHON" ]; then
        local py=""
        for candidato in python3.13 python3.12; do
            if command -v "$candidato" > /dev/null 2>&1; then
                py="$candidato"
                break
            fi
        done
        if [ -z "$py" ]; then
            echo "Python 3.12/3.13 nao encontrado. O pyspark nao suporta Python 3.14." >&2
            exit 1
        fi
        echo "Criando .venv com $py..."
        "$py" -m venv .venv
    fi
    "$VENV_PYTHON" -m pip install --upgrade pip
    "$VENV_PYTHON" -m pip install -r requirements.txt
    echo
    echo "Setup concluido. Para ativar a venv: source .venv/bin/activate"
}

dados() {
    [ -x "$VENV_PYTHON" ] || { echo "Venv ausente - rode antes: ./scripts/projeto.sh setup" >&2; exit 1; }
    "$VENV_PYTHON" src/ingestion/download_tlc.py
    echo
    echo "10 parquets em data/landing/ (idempotente: re-execucao nao rebaixa nada)."
}

qualidade() {
    [ -x "$VENV_PYTHON" ] || { echo "Venv ausente - rode antes: ./scripts/projeto.sh setup" >&2; exit 1; }
    "$VENV_PYTHON" -m ruff check .
    "$VENV_PYTHON" -m black --check .
    "$VENV_PYTHON" -m pytest -q
    echo
    echo "Qualidade OK: ruff + black + pytest."
}

deploy() {
    command -v databricks > /dev/null 2>&1 || { echo "Databricks CLI nao encontrada." >&2; exit 1; }
    if [ -z "${DATABRICKS_HOST:-}" ] || [ -z "${DATABRICKS_TOKEN:-}" ]; then
        echo "Defina DATABRICKS_HOST e DATABRICKS_TOKEN (PAT) antes." >&2
        exit 1
    fi
    databricks bundle validate
    databricks bundle deploy
    databricks bundle run pipeline_nyc_taxi
    echo
    echo "Pipeline executado com sucesso via job pipeline_nyc_taxi."
}

case "${1:-ajuda}" in
    setup) setup ;;
    dados) dados ;;
    qualidade) qualidade ;;
    deploy) deploy ;;
    *)
        echo "Uso: ./scripts/projeto.sh <acao>"
        echo
        echo "  setup      cria a venv (Python 3.12/3.13) e instala requirements.txt"
        echo "  dados      baixa os 10 parquets da TLC para data/landing/ (idempotente)"
        echo "  qualidade  roda ruff + black --check + pytest"
        echo "  deploy     bundle validate + deploy + run (exige CLI e PAT configurados)"
        ;;
esac
