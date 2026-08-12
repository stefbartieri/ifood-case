# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestao - origem publica da TLC direto para a landing
# MAGIC
# MAGIC Baixa os parquets mensais de `{yellow|green}_tripdata_2023-MM.parquet` do
# MAGIC CDN da TLC e grava em `/Volumes/workspace/nyc_taxi_landing/files/`,
# MAGIC preservando o nome original do arquivo.
# MAGIC
# MAGIC A landing e imutavel: este notebook so CRIA arquivo ausente ou substitui
# MAGIC arquivo cujo tamanho diverge da origem. Nunca apaga em massa.
# MAGIC
# MAGIC Integridade e idempotencia usam o mesmo criterio: `Content-Length`. Arquivo
# MAGIC ja presente com o tamanho correto e pulado, o que torna a reexecucao barata
# MAGIC e um download parcial autocorretivo.

# COMMAND ----------

# dbutils e injetado pelo runtime Databricks; o bloco espelhado da logica de
# ingestao e validado por pytest contra src/ingestion/tlc_source.py.
# ruff: noqa: F821, E402

dbutils.widgets.text("taxi_types", "yellow,green", "Tipos de taxi")
dbutils.widgets.text("meses", "2023-01,2023-02,2023-03,2023-04,2023-05", "Meses")
dbutils.widgets.text(
    "volume_root", "/Volumes/workspace/nyc_taxi_landing/files", "Raiz da landing"
)

TAXI_TYPES_PARAM = [t.strip() for t in dbutils.widgets.get("taxi_types").split(",")]
MESES_PARAM = [m.strip() for m in dbutils.widgets.get("meses").split(",")]
VOLUME_ROOT = dbutils.widgets.get("volume_root").rstrip("/")

# COMMAND ----------

# INICIO logica de ingestao (espelho de src/ingestion/tlc_source.py - nao editar
# aqui sem atualizar o modulo; tests/test_tlc_source.py compara os dois)

# Origem publica da TLC (sem barra final).
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Escopo do case: yellow + green, Jan-Mai/2023.
TAXI_TYPES = ["yellow", "green"]
MESES = ["2023-01", "2023-02", "2023-03", "2023-04", "2023-05"]

# Soma dos bytes por frota, conferida na ingestao original contra o
# Content-Length de cada arquivo. Serve de invariante ao final da carga.
TOTAIS_ESPERADOS_BYTES = {"yellow": 264426470, "green": 7936049}


def nome_arquivo(taxi_type: str, ano_mes: str) -> str:
    """Nome ORIGINAL do arquivo na origem (nunca renomear na landing)."""
    return f"{taxi_type}_tripdata_{ano_mes}.parquet"


def montar_url(taxi_type: str, ano_mes: str, base_url: str = BASE_URL) -> str:
    """URL publica do parquet mensal."""
    return f"{base_url.rstrip('/')}/{nome_arquivo(taxi_type, ano_mes)}"


def caminho_destino(raiz: str, taxi_type: str, ano_mes: str) -> str:
    """Caminho final na landing: {raiz}/{taxi_type}/{ano}/{arquivo_original}.

    Sempre com barra normal - o destino pode ser um Volume do Unity Catalog
    (/Volumes/...), onde separador do Windows nao vale.
    """
    ano = ano_mes.split("-")[0]
    return f"{raiz.rstrip('/')}/{taxi_type}/{ano}/{nome_arquivo(taxi_type, ano_mes)}"


def landing_integra(
    tamanhos_por_frota: dict[str, list[int | None]], escopo_completo: bool
) -> bool:
    """True quando a landing ja esta completa e com os bytes esperados.

    Criterio deliberadamente conservador: so vale para o escopo completo do
    case (todos os meses de TOTAIS_ESPERADOS_BYTES), porque o invariante
    conhecido e a soma por frota - em escopo parcial nao da para afirmar
    integridade sem consultar a origem.

    Serve para decidir se a ingestao precisa tocar a rede: com a landing
    integra, nem o teste de fumaca nem os HEADs sao necessarios, o que mantem
    o pipeline reproduzivel em workspace sem egresso para o CDN da TLC.
    """
    if not escopo_completo or not tamanhos_por_frota:
        return False
    for frota, tamanhos in tamanhos_por_frota.items():
        esperado = TOTAIS_ESPERADOS_BYTES.get(frota)
        if esperado is None or not tamanhos or any(t is None for t in tamanhos):
            return False
        if sum(tamanhos) != esperado:  # type: ignore[arg-type]
            return False
    return True


def precisa_baixar(tamanho_local: int | None, content_length: int | None) -> bool:
    """Decide entre baixar e pular.

    Baixa quando o arquivo nao existe (tamanho_local None), quando o tamanho
    diverge do Content-Length ou quando a origem nao informou o tamanho (nesse
    caso nao da para afirmar que o arquivo local esta integro). So pula com
    tamanhos conhecidos e iguais - o que torna um download parcial
    autocorretivo na proxima execucao.
    """
    if tamanho_local is None or content_length is None:
        return True
    return tamanho_local != content_length


# FIM logica de ingestao

# COMMAND ----------

# Imports e helpers. Nenhuma chamada de rede acontece nesta celula.

import os
import shutil
import socket
import tempfile
import time
import urllib.request

RETRY_WAITS_S = [2, 4, 8]
TIMEOUT_S = 60


def content_length_da_origem(url: str) -> int | None:
    """Tamanho anunciado pela origem, via HEAD. None se nao informado."""
    req = urllib.request.Request(url, method="HEAD")
    with urllib.request.urlopen(req, timeout=TIMEOUT_S) as resp:
        valor = resp.headers.get("Content-Length")
    return int(valor) if valor else None


def tamanho_no_volume(caminho: str) -> int | None:
    """Tamanho do arquivo na landing. None se ainda nao existe."""
    try:
        return os.path.getsize(caminho)
    except OSError:
        return None


def baixar_para_o_volume(url: str, destino: str) -> int:
    """Baixa para um temporario do driver e copia para o Volume.

    Volumes aceitam escrita sequencial, mas nao ha garantia de rename atomico
    entre sistemas de arquivos; por isso a validacao de tamanho acontece DEPOIS
    da copia, sobre o arquivo que realmente ficou no destino.
    """
    os.makedirs(os.path.dirname(destino), exist_ok=True)
    with tempfile.TemporaryDirectory() as tmp:
        temporario = os.path.join(tmp, os.path.basename(destino))
        with (
            urllib.request.urlopen(url, timeout=TIMEOUT_S) as resp,
            open(temporario, "wb") as saida,
        ):
            shutil.copyfileobj(resp, saida, length=1024 * 1024)
        shutil.copyfile(temporario, destino)
    return os.path.getsize(destino)


# COMMAND ----------

# Estado da landing ANTES de qualquer acesso a rede. Com o escopo completo ja
# presente e os totais por frota batendo, a ingestao nao precisa da origem -
# isso mantem o pipeline reproduzivel em workspace sem egresso para o CDN.

TAMANHOS_ATUAIS = {
    taxi_type: [
        tamanho_no_volume(caminho_destino(VOLUME_ROOT, taxi_type, ano_mes))
        for ano_mes in MESES_PARAM
    ]
    for taxi_type in TAXI_TYPES_PARAM
}
LANDING_PRONTA = landing_integra(TAMANHOS_ATUAIS, set(MESES_PARAM) == set(MESES))

print("Landing integra:", LANDING_PRONTA)

# COMMAND ----------

# Teste de fumaca: o egresso de internet do workspace e restrito a uma
# allowlist nao publicada. Falhar aqui, com mensagem clara, e muito melhor que
# estourar um socket no meio do terceiro arquivo. So roda quando ha o que
# baixar - landing integra dispensa a rede.

if LANDING_PRONTA:
    print("Landing ja completa: teste de fumaca e download dispensados.")
else:
    try:
        print("DNS:", socket.gethostbyname("d37ci6vzurychx.cloudfront.net"))
        _req = urllib.request.Request(montar_url("green", MESES[0]), method="HEAD")
        with urllib.request.urlopen(_req, timeout=TIMEOUT_S) as _resp:
            print("HEAD OK, Content-Length:", _resp.headers.get("Content-Length"))
    except Exception as exc:
        raise RuntimeError(
            "Sem acesso a origem da TLC a partir deste workspace "
            f"({type(exc).__name__}: {exc}) e a landing nao esta completa. O "
            "egresso de internet e restrito a uma allowlist. Use o caminho "
            "alternativo: baixe com src/ingestion/download_tlc.py na sua maquina "
            "e suba os arquivos para o Volume conforme "
            "docs/manual_steps/001-setup-databricks.md."
        ) from exc

# COMMAND ----------

# Carga: um arquivo por vez, com retry. Pula o que ja esta integro.

resumo = []
A_CARREGAR = [] if LANDING_PRONTA else TAXI_TYPES_PARAM

for taxi_type in A_CARREGAR:
    for ano_mes in MESES_PARAM:
        url = montar_url(taxi_type, ano_mes)
        destino = caminho_destino(VOLUME_ROOT, taxi_type, ano_mes)
        esperado = content_length_da_origem(url)
        local = tamanho_no_volume(destino)

        if not precisa_baixar(local, esperado):
            resumo.append((os.path.basename(destino), "pulado", local))
            print(f"[pulado] {os.path.basename(destino)} ({local} bytes)")
            continue

        ultimo_erro = None
        gravados = None
        for espera in [0, *RETRY_WAITS_S]:
            if espera:
                print(f"[retry] aguardando {espera}s (erro anterior: {ultimo_erro})")
                time.sleep(espera)
            try:
                gravados = baixar_para_o_volume(url, destino)
                if esperado is not None and gravados != esperado:
                    raise ValueError(
                        f"gravados {gravados} != Content-Length {esperado}"
                    )
                break
            except Exception as exc:
                ultimo_erro = exc
                gravados = None

        if gravados is None:
            raise RuntimeError(f"falha ao baixar {url}: {ultimo_erro}")

        resumo.append((os.path.basename(destino), "baixado", gravados))
        print(f"[baixado] {os.path.basename(destino)} ({gravados} bytes)")

# COMMAND ----------

# Verificacao final: os arquivos do escopo existem e a soma por frota bate com
# o total conferido na origem. Falha explicita se faltar ou divergir.

for taxi_type in TAXI_TYPES_PARAM:
    total = 0
    for ano_mes in MESES_PARAM:
        destino = caminho_destino(VOLUME_ROOT, taxi_type, ano_mes)
        tamanho = tamanho_no_volume(destino)
        assert tamanho is not None, f"arquivo ausente na landing: {destino}"
        total += tamanho
    esperado_frota = TOTAIS_ESPERADOS_BYTES.get(taxi_type)
    print(f"[{taxi_type}] {len(MESES_PARAM)} arquivos, {total} bytes")
    if set(MESES_PARAM) == set(MESES) and esperado_frota is not None:
        assert (
            total == esperado_frota
        ), f"total de {taxi_type} = {total} bytes, esperado {esperado_frota}"

print("Landing conferida:", len(resumo), "arquivos processados")

for arquivo, status, tamanho in resumo:
    print(f"  {arquivo:38s} {status:8s} {tamanho} bytes")
