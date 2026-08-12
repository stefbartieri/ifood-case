# Guia manual 001 - Setup do Databricks Free Edition e upload da landing

> Setup do Databricks Free Edition e carga da landing zone. Execute cada
> passo abaixo no workspace e confira as saídas indicadas em cada validação.

## 1. Criar a conta Databricks Free Edition

1. Acesse https://www.databricks.com/learn/free-edition e crie a conta
   (login posterior em https://login.databricks.com).
2. Ao entrar no workspace, anote a **URL do workspace** (formato
   `https://<id>.cloud.databricks.com`) - ela será usada na opção CLI do upload.

Observações da Free Edition (não são erros): tudo roda serverless; o catálogo
default é `workspace`; DBFS root é desabilitado (por isso usamos UC Volumes).

## 2. Criar os schemas e o Volume da landing (SQL)

1. No menu lateral, abra **SQL Editor** (warehouse "Serverless Starter
   Warehouse" default já serve).
2. Cole e execute o bloco abaixo:

```sql
CREATE SCHEMA IF NOT EXISTS workspace.nyc_taxi_landing;
CREATE SCHEMA IF NOT EXISTS workspace.nyc_taxi_bronze;
CREATE SCHEMA IF NOT EXISTS workspace.nyc_taxi_gold;
CREATE VOLUME IF NOT EXISTS workspace.nyc_taxi_landing.files;
SHOW SCHEMAS IN workspace LIKE 'nyc_taxi*';
DESCRIBE VOLUME workspace.nyc_taxi_landing.files;
```

3. **[VALIDAÇÃO 1]** Confira a saída de `SHOW SCHEMAS ...` (deve listar
   `nyc_taxi_landing`, `nyc_taxi_bronze`, `nyc_taxi_gold`) e de
   `DESCRIBE VOLUME workspace.nyc_taxi_landing.files`.

## 3. Upload dos 10 parquets para o Volume (alternativa)

> **O caminho padrão hoje é automático.** A primeira task do job baixa os
> arquivos da origem pública direto para o Volume - veja
> [011-ingestao-automatizada.md](011-ingestao-automatizada.md). Esta seção
> continua válida como alternativa para quando o egresso de internet do
> workspace não alcançar a origem, ou para carregar a landing sem executar o
> job.

Os arquivos estão em `data/landing/{yellow|green}/2023/` na sua máquina
(baixados pelo script `src/ingestion/download_tlc.py`). O destino final é:

```text
/Volumes/workspace/nyc_taxi_landing/files/yellow/2023/yellow_tripdata_2023-01..05.parquet
/Volumes/workspace/nyc_taxi_landing/files/green/2023/green_tripdata_2023-01..05.parquet
```

Escolha UMA das opções abaixo.

### Opção 1 - Upload pela UI (mais simples)

1. Menu lateral → **Catalog** → catálogo `workspace` → schema
   `nyc_taxi_landing` → volume `files`.
2. Botão **Upload to volume**. Antes de enviar, crie a estrutura de pastas:
   no diálogo de upload é possível criar pastas - crie `yellow/2023` e envie os
   5 arquivos `yellow_tripdata_2023-0*.parquet`; depois crie `green/2023` e
   envie os 5 `green_tripdata_2023-0*.parquet`.
3. Limite da UI: 5 GB/arquivo (nosso maior tem ~56 MB - ok).
4. **Não renomeie nenhum arquivo** - a landing preserva os nomes originais.

### Opção 2 - Upload pela CLI do Databricks

1. Instale o binário da CLI (NÃO é pacote pip):
   `winget install Databricks.DatabricksCLI`
   (alternativa: baixar release em https://github.com/databricks/cli/releases).
2. Gere um PAT no workspace: **Settings → Developer → Access tokens →
   Generate new token**. NUNCA commite o token (o `.gitignore` cobre `.env`).
3. No PowerShell, na raiz do repo:

```powershell
$env:DATABRICKS_HOST = "https://<workspace>.cloud.databricks.com"
$env:DATABRICKS_TOKEN = "<PAT>"
foreach ($t in "yellow","green") {
  databricks fs mkdir "dbfs:/Volumes/workspace/nyc_taxi_landing/files/$t/2023"
  foreach ($m in "01","02","03","04","05") {
    databricks fs cp "data/landing/$t/2023/${t}_tripdata_2023-$m.parquet" "dbfs:/Volumes/workspace/nyc_taxi_landing/files/$t/2023/${t}_tripdata_2023-$m.parquet" --overwrite
  }
}
```

> O prefixo `dbfs:/` é obrigatório nos paths de Volume da CLI.

## 4. Verificação final (obrigatória)

Rode UMA das listagens abaixo e confira a saída completa (com tamanhos):

- **SQL Editor**:

```sql
LIST '/Volumes/workspace/nyc_taxi_landing/files/yellow/2023';
LIST '/Volumes/workspace/nyc_taxi_landing/files/green/2023';
```

- **CLI**:

```powershell
databricks fs ls -l "dbfs:/Volumes/workspace/nyc_taxi_landing/files/yellow/2023"
databricks fs ls -l "dbfs:/Volumes/workspace/nyc_taxi_landing/files/green/2023"
```

2. **[VALIDAÇÃO 2]** Confira a listagem com os 10 arquivos e tamanhos.
   Tamanhos esperados do yellow (bytes exatos):

| Arquivo | Bytes |
|---|---|
| yellow_tripdata_2023-01.parquet | 47.673.370 |
| yellow_tripdata_2023-02.parquet | 47.748.012 |
| yellow_tripdata_2023-03.parquet | 56.127.762 |
| yellow_tripdata_2023-04.parquet | 54.222.699 |
| yellow_tripdata_2023-05.parquet | 58.654.627 |

   Green: comparar com os bytes registrados em
   `data/landing/ingestion_log.jsonl` (faixa de sanidade 1,4-1,7 MiB cada).
   Qualquer divergência ⇒ re-upload do arquivo divergente.

## Apêndice - Caminho A (alternativo): download in-notebook

Use somente se preferir evitar o upload manual. O egresso de internet dos
notebooks da Free Edition é restrito a uma allowlist NÃO publicada - o teste
de fumaça abaixo falha rápido se o CDN da TLC não estiver liberado.

Num notebook Python do workspace:

```python
import socket

socket.gethostbyname("d37ci6vzurychx.cloudfront.net")
# Se levantar "[Errno -3] Temporary failure in name resolution",
# o CDN NÃO está na allowlist -> use o Caminho B (upload manual acima).
```

Se o teste resolver o DNS, baixe direto para o Volume (escrita sequencial é
suportada; Volumes não suportam random writes):

```python
import urllib.request

for t in ["yellow", "green"]:
    for m in ["01", "02", "03", "04", "05"]:
        fname = f"{t}_tripdata_2023-{m}.parquet"
        urllib.request.urlretrieve(
            f"https://d37ci6vzurychx.cloudfront.net/trip-data/{fname}",
            f"/Volumes/workspace/nyc_taxi_landing/files/{t}/2023/{fname}",
        )
        print("ok", fname)
```

Depois, execute a verificação da seção 4 normalmente (as validações 1 e 2
continuam obrigatórias).
