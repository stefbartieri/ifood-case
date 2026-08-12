"""Download idempotente dos parquets da NYC TLC para a landing zone local.

Baixa os arquivos {taxi_type}_tripdata_{year}-{MM}.parquet do CDN da TLC para
data/landing/{taxi_type}/{year}/, preservando o nome original.

Robustez: retry com backoff exponencial (2s/4s/8s), download em streaming para
arquivo temporario .part com rename atomico e validacao por Content-Length.
Idempotencia: arquivo final existente e pulado sem re-download.

Cada arquivo processado gera uma linha JSON append-only em
data/landing/ingestion_log.jsonl.
"""

import argparse
import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import requests

# Mantem as duas formas de execucao funcionando: como script
# (python src/ingestion/download_tlc.py, forma documentada no README) e como
# modulo (python -m src.ingestion.download_tlc). Sem isto, a raiz do repo nao
# entra no sys.path e o import do modulo compartilhado falha.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.ingestion.tlc_source import (  # noqa: E402
    BASE_URL,
    MESES,
    TAXI_TYPES,
    caminho_destino,
    montar_url,
)

RETRY_WAITS_S = [2, 4, 8]
CHUNK_SIZE = 1024 * 1024
TIMEOUT_S = 60


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Baixa os parquets da NYC TLC para a landing zone local."
    )
    parser.add_argument(
        "--base-url",
        default=BASE_URL,
        help="URL base do CDN da TLC (sem barra final)",
    )
    parser.add_argument("--year", default="2023", help="Ano dos arquivos (YYYY)")
    parser.add_argument(
        "--months",
        nargs="+",
        default=[mes.split("-")[1] for mes in MESES],
        help="Meses (MM) a baixar",
    )
    parser.add_argument(
        "--taxi-types",
        nargs="+",
        default=list(TAXI_TYPES),
        help="Tipos de taxi (yellow, green)",
    )
    parser.add_argument(
        "--dest",
        default="data/landing",
        help="Diretorio raiz da landing local",
    )
    return parser.parse_args()


def log_line(log_path: Path, entry: dict) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def download_file(url: str, final_path: Path) -> tuple[int, int]:
    """Baixa url para final_path via .part + rename atomico.

    Retorna (content_length, bytes_gravados). Levanta ValueError se os bytes
    gravados divergirem do Content-Length reportado pelo CDN.
    """
    part_path = final_path.with_suffix(final_path.suffix + ".part")
    final_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with requests.get(url, stream=True, timeout=TIMEOUT_S) as resp:
            resp.raise_for_status()
            content_length = int(resp.headers["Content-Length"])
            bytes_written = 0
            with part_path.open("wb") as f:
                for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                    f.write(chunk)
                    bytes_written += len(chunk)
        if bytes_written != content_length:
            raise ValueError(
                f"bytes gravados ({bytes_written}) != Content-Length ({content_length})"
            )
        part_path.replace(final_path)
        return content_length, bytes_written
    finally:
        part_path.unlink(missing_ok=True)


def process_file(url: str, final_path: Path, log_path: Path) -> str:
    """Processa um arquivo (pula, baixa com retry ou registra erro).

    Retorna o status registrado no log.
    """
    now = datetime.now(UTC).astimezone().isoformat(timespec="seconds")
    entry = {
        "arquivo": final_path.name,
        "url": url,
        "content_length": None,
        "bytes_gravados": None,
        "baixado_em": now,
        "status": "erro",
    }
    if final_path.exists():
        entry["status"] = "pulado_ja_existia"
        entry["bytes_gravados"] = final_path.stat().st_size
        log_line(log_path, entry)
        print(f"[pulado] {final_path.name} ja existe ({entry['bytes_gravados']} bytes)")
        return entry["status"]

    last_error: Exception | None = None
    for wait_s in [0, *RETRY_WAITS_S]:
        if wait_s:
            print(
                f"[retry] {final_path.name}: aguardando {wait_s}s (erro: {last_error})"
            )
            time.sleep(wait_s)
        try:
            content_length, bytes_written = download_file(url, final_path)
            entry["content_length"] = content_length
            entry["bytes_gravados"] = bytes_written
            entry["status"] = "baixado"
            entry["baixado_em"] = (
                datetime.now(UTC).astimezone().isoformat(timespec="seconds")
            )
            log_line(log_path, entry)
            print(f"[baixado] {final_path.name} ({bytes_written} bytes)")
            return entry["status"]
        except Exception as exc:  # noqa: BLE001 - qualquer falha entra no retry
            last_error = exc
    entry["status"] = "erro"
    log_line(log_path, entry)
    print(f"[erro] {final_path.name}: {last_error}", file=sys.stderr)
    return entry["status"]


def main() -> int:
    args = parse_args()
    dest_root = Path(args.dest)
    log_path = dest_root / "ingestion_log.jsonl"
    statuses: list[str] = []
    for taxi_type in args.taxi_types:
        for month in args.months:
            ano_mes = f"{args.year}-{month}"
            url = montar_url(taxi_type, ano_mes, args.base_url)
            final_path = Path(caminho_destino(args.dest, taxi_type, ano_mes))
            statuses.append(process_file(url, final_path, log_path))
    erros = statuses.count("erro")
    print(
        f"Concluido: {statuses.count('baixado')} baixados, "
        f"{statuses.count('pulado_ja_existia')} pulados, {erros} erros"
    )
    return 1 if erros else 0


if __name__ == "__main__":
    sys.exit(main())
