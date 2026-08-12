#!/usr/bin/env bash
# Utilitario do projeto (macOS/Linux).
# Uso: ./scripts/projeto.sh <acao>
# Acoes: setup | dados | qualidade | landing | deploy

set -euo pipefail
cd "$(dirname "$0")/.."
VENV_PYTHON=".venv/bin/python"
VOLUME_RAIZ="dbfs:/Volumes/workspace/nyc_taxi_landing/files"
FROTAS=(yellow green)
MESES=(01 02 03 04 05)

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

# Host do target default, so para montar mensagens de erro acionaveis.
host_do_bundle() {
    grep -m1 -E '^[[:space:]]+host:' databricks.yml | awk '{print $2}' \
        || echo "https://<workspace>.cloud.databricks.com"
}

assert_databricks_pronto() {
    command -v databricks > /dev/null 2>&1 || {
        echo "Databricks CLI nao encontrada (binario standalone, nao e pacote pip)." >&2
        exit 1
    }
    # "auth describe" resolve o host pelo target default do bundle, igual ao
    # deploy, e vale para qualquer metodo (perfil, PAT ou service principal).
    # Ter ~/.databrickscfg nao basta: o perfil pode ser de OUTRO workspace.
    # O comando sai com codigo 0 mesmo sem credencial - quem decide e o "status".
    local descricao
    descricao="$(databricks auth describe -o json 2> /dev/null || true)"
    if printf '%s' "$descricao" | grep -q '"status"[[:space:]]*:[[:space:]]*"success"'; then
        return 0
    fi
    local destino
    destino="$(host_do_bundle)"
    cat >&2 <<EOF
Sem credencial valida para o workspace do bundle ($destino). Escolha uma das opcoes:

  1) OAuth (recomendado, o host ja vem do databricks.yml):
     databricks auth login --host $destino

  2) PAT (token pessoal):
     export DATABRICKS_HOST='$destino'; export DATABRICKS_TOKEN='dapi...'

Se voce ja esta logado em outro workspace, o perfil existente nao serve: a
credencial precisa ser do host acima (ou ajuste o host em databricks.yml).
EOF
    exit 1
}

# Arquivos ja presentes na landing do Volume (0 se a pasta nem existe).
arquivos_no_volume() {
    local total=0 n
    for frota in "${FROTAS[@]}"; do
        # A pasta pode nem existir (Volume recem-criado). Sem o "|| true" o
        # pipefail derrubaria o script inteiro via set -e: erro conta zero.
        n=$(databricks fs ls "$VOLUME_RAIZ/$frota/2023" --output json 2> /dev/null \
            | grep -o '"name"' | wc -l | tr -d ' ' || true)
        total=$((total + n))
    done
    echo "$total"
}

landing() {
    assert_databricks_pronto
    [ -x "$VENV_PYTHON" ] || { echo "Venv ausente - rode antes: ./scripts/projeto.sh setup" >&2; exit 1; }

    # Os schemas e o Volume sao recursos do bundle (resources/catalogo.yml),
    # entao quem os cria e o deploy - nao ha criacao duplicada aqui.
    echo "[1/4] Garantindo schemas e Volume no workspace..."
    databricks bundle deploy

    echo
    echo "[2/4] Conferindo os parquets locais..."
    "$VENV_PYTHON" src/ingestion/download_tlc.py

    echo
    echo "[3/4] Enviando o que falta para o Volume..."
    local enviados=0 listagem tamanho_remoto tamanho_local arquivo
    for frota in "${FROTAS[@]}"; do
        # "fs cp" nao cria diretorios intermediarios: o bundle cria o Volume,
        # mas {frota}/2023 so passa a existir aqui. "mkdir" cria a arvore
        # inteira e e idempotente, entao roda a cada execucao sem efeito extra.
        databricks fs mkdir "$VOLUME_RAIZ/$frota/2023"
        listagem="$(databricks fs ls "$VOLUME_RAIZ/$frota/2023" --output json 2> /dev/null || echo '[]')"
        for mes in "${MESES[@]}"; do
            arquivo="${frota}_tripdata_2023-${mes}.parquet"
            tamanho_local=$(wc -c < "data/landing/$frota/2023/$arquivo" | tr -d ' ')
            tamanho_remoto=$(printf '%s' "$listagem" | "$VENV_PYTHON" -c "
import json, sys
try:
    itens = json.load(sys.stdin)
except Exception:
    itens = []
print(next((i['size'] for i in itens if i['name'] == '$arquivo'), ''))
")
            if [ "$tamanho_remoto" = "$tamanho_local" ]; then
                echo "  [ok]     $arquivo ja no Volume ($tamanho_local bytes)"
                continue
            fi
            echo "  [upload] $arquivo ($tamanho_local bytes)"
            databricks fs cp "data/landing/$frota/2023/$arquivo" \
                "$VOLUME_RAIZ/$frota/2023/$arquivo" --overwrite
            enviados=$((enviados + 1))
        done
    done

    echo
    echo "[4/4] Conferindo os totais por frota..."
    # Os invariantes vivem no modulo Python; o script nao os duplica.
    for frota in "${FROTAS[@]}"; do
        databricks fs ls "$VOLUME_RAIZ/$frota/2023" --output json | "$VENV_PYTHON" -c "
import json, sys
from src.ingestion.tlc_source import TOTAIS_ESPERADOS_BYTES

itens = json.load(sys.stdin)
total = sum(i['size'] for i in itens)
alvo = TOTAIS_ESPERADOS_BYTES['$frota']
if len(itens) != ${#MESES[@]} or total != alvo:
    raise SystemExit(
        f'Landing de $frota inconsistente: {len(itens)} arquivos, {total} bytes '
        f'(esperado ${#MESES[@]} arquivos, {alvo} bytes).'
    )
print(f'  [$frota] {len(itens)} arquivos, {total} bytes')
"
    done
    echo
    echo "Landing pronta ($enviados arquivo(s) enviado(s)). Agora: ./scripts/projeto.sh deploy"
}

# Avisa quando a landing esta incompleta, sem impedir o deploy: a task de
# ingestao baixa da origem quando o egresso do workspace alcanca o CDN da TLC,
# e falha em segundos com mensagem acionavel quando nao alcanca.
aviso_landing() {
    local encontrados total
    encontrados="$(arquivos_no_volume)"
    total=$((${#FROTAS[@]} * ${#MESES[@]}))
    if [ "$encontrados" -eq "$total" ]; then
        echo "[pre-flight] landing completa ($encontrados/$total arquivos no Volume)."
        return 0
    fi
    cat >&2 <<EOF
[pre-flight] AVISO: landing incompleta ($encontrados/$total arquivos no Volume)

  A 1a task do job baixa os dados da origem publica, mas o egresso de internet
  do workspace pode nao alcancar o CDN da TLC (allowlist da Free Edition).

  Se a ingestao falhar por rede, carregue a landing da sua maquina com:
    ./scripts/projeto.sh landing
EOF
}

deploy() {
    assert_databricks_pronto
    aviso_landing
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
    landing) landing ;;
    deploy) deploy ;;
    *)
        echo "Uso: ./scripts/projeto.sh <acao>"
        echo
        echo "  setup      cria a venv (Python 3.12/3.13) e instala requirements.txt"
        echo "  dados      baixa os 10 parquets da TLC para data/landing/ (idempotente)"
        echo "  qualidade  roda ruff + black --check + pytest"
        echo "  landing    cria schemas/Volume e carrega a landing da sua maquina"
        echo "  deploy     bundle validate + deploy + run (exige CLI autenticada)"
        ;;
esac
