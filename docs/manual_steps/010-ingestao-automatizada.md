# Ingestão automatizada - executar e conferir

A landing deixa de depender de upload manual: o notebook
`src/ingestion/ingestao_landing.py` baixa os 10 parquets direto da origem
pública da TLC para o Volume, e roda como primeira task do job. Com isso um
único `databricks bundle run` reproduz a solução do zero.

O caminho antigo (baixar na sua máquina e subir pela interface) continua
documentado em [001-setup-databricks.md](001-setup-databricks.md) e serve de
alternativa caso o egresso de internet do workspace deixe de alcançar a origem.

---

## 1. Fazer o deploy

O job passa de 5 para 6 tasks; o deploy atualiza a definição no workspace.

```bash
databricks bundle deploy
```

Resultado esperado: `Deployment complete!`.

---

## 2. Provar que a ingestão realmente acontece

Este é o passo que diferencia "o arquivo já estava lá" de "o pipeline foi
buscar". Apague **um** arquivo do Volume e deixe o job repor.

```sql
-- No SQL Editor, apenas para ver o estado atual:
LIST '/Volumes/workspace/nyc_taxi_landing/files/green/2023';
```

Agora remova o arquivo pela interface (Catalog → workspace →
nyc_taxi_landing → files → green → 2023 → `green_tripdata_2023-01.parquet` →
menu de contexto → Delete), ou pela CLI:

```bash
databricks fs rm dbfs:/Volumes/workspace/nyc_taxi_landing/files/green/2023/green_tripdata_2023-01.parquet
```

---

## 3. Rodar o pipeline

```bash
databricks bundle run pipeline_nyc_taxi
```

Resultado esperado: **6 tasks** concluídas, começando por `ingestao`. Nos logs
dessa task devem aparecer:

- a linha do teste de fumaça - `DNS: <ip>` e `HEAD OK, Content-Length: 1427002`;
- `[baixado] green_tripdata_2023-01.parquet (1427002 bytes)` - o arquivo
  apagado voltou;
- `[pulado] ...` para os outros 9, que já estavam íntegros;
- o resumo final por frota, com os totais em bytes.

A primeira execução em Volume vazio leva alguns minutos (~272 MB). Com os
arquivos presentes, a task é quase instantânea - ela só faz um `HEAD` por
arquivo e compara tamanhos.

---

## 4. Conferir o Volume

```sql
LIST '/Volumes/workspace/nyc_taxi_landing/files/yellow/2023';
LIST '/Volumes/workspace/nyc_taxi_landing/files/green/2023';
```

Resultado esperado: 5 arquivos em cada, com os nomes originais. A soma dos
bytes tem de ser **264.426.470** no yellow e **7.936.049** no green - os mesmos
totais conferidos na carga original. O próprio notebook verifica isso com
`assert`, então uma divergência já teria derrubado a task.

---

## 5. Idempotência

Rode de novo:

```bash
databricks bundle run pipeline_nyc_taxi
```

Resultado esperado: na task `ingestao`, **10 arquivos pulados e nenhum
baixado**. Nada é rebaixado sem necessidade, e a landing continua imutável.

---

## 6. Nada regrediu

Os dados são os mesmos, então os números do pipeline não podem mudar.

```sql
SELECT metrica, valor FROM workspace.nyc_taxi_gold.dq_metrics
WHERE taxi_type = 'total' ORDER BY metrica;

SELECT COUNT(*) AS quarentena FROM workspace.nyc_taxi_gold.taxi_trips_rejected;

SELECT * FROM workspace.nyc_taxi_gold.vw_media_total_amount_mes
ORDER BY pickup_year_month;
```

Esperado: bronze **16.526.016** · gold **15.651.177** · quarentena **874.839** ·
médias **27,46 · 27,37 · 28,28 · 28,78 · 29,45**.

Trocar a forma de trazer os arquivos não pode alterar um único número - é
exatamente isso que estas consultas provam.

---

## Quando a landing já está carregada

A task mede a landing **antes** de tocar a rede. Com os 10 arquivos presentes e
os totais por frota batendo, ela imprime `Landing integra: True`, pula o teste
de fumaça e o download, e vai direto para a verificação final. É isso que
mantém o pipeline reproduzível onde o egresso não alcança a origem.

O critério é conservador de propósito: só vale para o escopo completo, porque o
único invariante conhecido é a soma de bytes por frota. Em escopo parcial a
rede volta a ser necessária.

## A allowlist varia entre workspaces

O egresso de internet da Free Edition é restrito a uma lista que a plataforma
não publica, e ela **não é a mesma em todos os workspaces**. Medição feita em
um workspace onde o CDN da TLC não é alcançável:

| Host | Resultado |
|---|---|
| `d37ci6vzurychx.cloudfront.net` (origem da TLC) | DNS não resolve |
| `www.nyc.gov` | DNS não resolve |
| `pypi.org` | resolve, HTTPS 200 |
| `github.com` | resolve, HTTPS 200 |
| `docs.databricks.com` | resolve, HTTPS 200 |
| `s3.amazonaws.com` | resolve, conexão OK |

Em outro workspace da mesma edição, a origem resolve normalmente e a task baixa
os arquivos. Não há explicação confirmada para a diferença. Por isso o projeto
não depende de a rede estar disponível: existe o caminho de carregar a landing
da sua máquina, e a ingestão o reconhece sozinha.

## Se o egresso falhar

A task `ingestao` para no teste de fumaça com uma mensagem explícita apontando
o caminho alternativo. Nesse caso, use
[001-setup-databricks.md](001-setup-databricks.md) para baixar localmente e
subir os arquivos, e rode o job a partir da task `bronze`.

---

## Checklist de validação

1. Saída do teste de fumaça (DNS + `Content-Length`).
2. Log da task `ingestao` mostrando o arquivo apagado sendo rebaixado.
3. `LIST` das duas pastas do Volume.
4. Evidência das 6 tasks concluídas.
5. Log da segunda execução (10 pulados).
6. Resultado das três consultas de não-regressão.
