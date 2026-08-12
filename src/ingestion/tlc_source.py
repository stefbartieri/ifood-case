"""Partes deterministicas da ingestao da NYC TLC - importavel sem dependencias.

Sem I/O e sem SparkSession: so as constantes da origem e as funcoes que
montam URL, nome de arquivo, caminho de destino e a decisao de baixar ou
pular. Usado pelo script local (src/ingestion/download_tlc.py) e espelhado no
notebook src/ingestion/ingestao_landing.py (bloco INICIO/FIM logica de
ingestao); tests/test_tlc_source.py garante que os dois nao divergem.

Autoridade de integridade: o Content-Length do CDN. O Content-Type e
inconsistente e nao ha ETag confiavel, entao a comparacao de tamanho e o
unico criterio de "arquivo ja esta correto".
"""

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
