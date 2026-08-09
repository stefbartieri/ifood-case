# Guia manual 002 — Executar o notebook bronze no Databricks

> Camada bronze. Importe o notebook `src/bronze/bronze_taxi_trips.py`
> no workspace e execute 2 vezes (a 2ª prova a idempotência).
> Pré-requisito: landing carregada (10 parquets no Volume).

## 1. Importar o notebook no workspace

**Opção UI (recomendada):**

1. No workspace, menu lateral → **Workspace** → sua pasta de usuário.
2. Botão **⋮ (kebab)** ou clique-direito → **Import** → **File**.
3. Selecione o arquivo local:
   `C:\Users\andre\OneDrive\Área de Trabalho\ifood\src\bronze\bronze_taxi_trips.py`
4. O arquivo abre como notebook Python (o formato `# Databricks notebook source`
   é reconhecido automaticamente).

**Opção CLI (alternativa, se tiver a CLI configurada):**

```powershell
databricks workspace import /Workspace/Users/<seu-email>/bronze_taxi_trips --file src/bronze/bronze_taxi_trips.py --format SOURCE --language PYTHON
```

## 2. Executar (1ª execução)

1. Abra o notebook importado e conecte ao compute **Serverless** (canto superior
   direito → Connect → Serverless).
2. Clique em **Run all**. Os widgets `taxi_types` e `meses` já vêm com os
   defaults corretos (`yellow,green` e `2023-01..2023-05`) — não mude nada.
3. Aguarde (alguns minutos). A última célula imprime a contagem por partição e
   o schema.

**Se falhar**: leia a mensagem de erro completa da célula que falhou.

## 3. Queries de validação (SQL Editor)

Rode o bloco abaixo no **SQL Editor** e confira TODAS as saídas:

```sql
-- (a) Tabela Delta e partições
DESCRIBE DETAIL workspace.nyc_taxi_bronze.taxi_trips;

-- (b) Schema (24 colunas e tipos)
DESCRIBE TABLE workspace.nyc_taxi_bronze.taxi_trips;

-- (c) Contagens yellow por partição (esperado: 3066766 / 2913955 / 3403766 / 3288250 / 3513649)
SELECT source_year_month, COUNT(*) AS linhas
FROM workspace.nyc_taxi_bronze.taxi_trips
WHERE taxi_type = 'yellow'
GROUP BY source_year_month ORDER BY source_year_month;

-- (d) Green: 5 partições e total 339630
SELECT source_year_month, COUNT(*) AS linhas
FROM workspace.nyc_taxi_bronze.taxi_trips
WHERE taxi_type = 'green'
GROUP BY source_year_month ORDER BY source_year_month;

-- (e) Total geral (esperado: 16526016)
SELECT COUNT(*) AS total FROM workspace.nyc_taxi_bronze.taxi_trips;

-- (f) Colunas exclusivas nulas no tipo oposto (esperado: 0)
SELECT COUNT(*) AS violacoes
FROM workspace.nyc_taxi_bronze.taxi_trips
WHERE (taxi_type = 'yellow' AND (ehail_fee IS NOT NULL OR trip_type IS NOT NULL))
   OR (taxi_type = 'green' AND airport_fee IS NOT NULL);
```

## 4. Reexecutar o notebook (2ª execução — idempotência)

1. Volte ao notebook e clique em **Run all** de novo (mesmos widgets).
2. Ao terminar, rode novamente as queries **(c)**, **(d)** e **(e)** no SQL
   Editor e cole as saídas — os números devem ser IDÊNTICOS aos da 1ª execução
   (o `replaceWhere` substitui as partições em vez de duplicar).

## 5. Checklist de validação

- [ ] Saída de `DESCRIBE DETAIL` (a)
- [ ] Saída de `DESCRIBE TABLE` (b)
- [ ] Contagens yellow (c) e green (d) + total (e) — 1ª execução
- [ ] Resultado da query de colunas exclusivas (f)
- [ ] Contagens (c)/(d)/(e) — 2ª execução (idempotência)
