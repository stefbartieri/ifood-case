# Quarentena de qualidade - executar e conferir

As linhas reprovadas pelas regras de qualidade deixam de ser descartadas e
passam a ser gravadas em `workspace.nyc_taxi_gold.taxi_trips_rejected`, cada
uma anotada com a regra que a reprovou (`_reject_reason`).

Nada mais muda: as regras, a ordem delas, o schema da gold e as métricas de
`dq_metrics` continuam idênticos. As consultas de conferência abaixo existem
justamente para provar isso.

---

## 1. Executar

Escolha **uma** das duas formas.

**Opção A - pipeline completo pelo bundle** (recomendada; ~2 min):

```bash
databricks bundle deploy
databricks bundle run pipeline_nyc_taxi
```

**Opção B - só a camada gold, pela interface**: abra o notebook
`src/gold/gold_taxi_trips.py` no workspace e use **Run all**.

Nos dois casos o notebook falha sozinho se algo não fechar: há um `assert` de
reconciliação e outro de particionamento. Terminar sem erro já é evidência.

---

## 2. Estrutura da tabela nova

```sql
DESCRIBE TABLE workspace.nyc_taxi_gold.taxi_trips_rejected;
```

Esperado: **10 colunas**, nesta ordem - `VendorID`, `passenger_count`,
`total_amount`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `taxi_type`,
`pickup_year_month`, `pickup_hour`, `source_year_month`, `_reject_reason`.

---

## 3. Contagem por motivo e por frota

```sql
SELECT _reject_reason,
       COUNT(*)                                          AS total,
       COUNT_IF(taxi_type = 'yellow')                    AS yellow,
       COUNT_IF(taxi_type = 'green')                     AS green
FROM workspace.nyc_taxi_gold.taxi_trips_rejected
GROUP BY _reject_reason
ORDER BY _reject_reason;
```

Valores esperados (coluna `total`):

| `_reject_reason` | total |
|---|---|
| `removidas_r1_pickup_fora_janela` | 113 |
| `removidas_r2_dropoff_nao_posterior` | 6.595 |
| `removidas_r3_total_amount_negativo` | 142.294 |
| `removidas_r4_passenger_count_invalido` | 725.837 |
| **soma** | **874.839** |

Por frota, a soma tem de dar **848.249** no yellow e **26.590** no green.

---

## 4. Particionamento: gold + quarentena reconstituem a bronze

Esta é a consulta central - a coluna `diferenca` tem de ser **0** nas três
linhas.

```sql
WITH g AS (
  SELECT COALESCE(taxi_type, 'total') AS taxi_type, COUNT(*) AS gold
  FROM workspace.nyc_taxi_gold.taxi_trips GROUP BY ROLLUP(taxi_type)
), r AS (
  SELECT COALESCE(taxi_type, 'total') AS taxi_type, COUNT(*) AS quarentena
  FROM workspace.nyc_taxi_gold.taxi_trips_rejected GROUP BY ROLLUP(taxi_type)
), b AS (
  SELECT taxi_type, valor AS bronze
  FROM workspace.nyc_taxi_gold.dq_metrics WHERE metrica = 'linhas_bronze'
)
SELECT b.taxi_type, b.bronze, g.gold, r.quarentena,
       b.bronze - g.gold - r.quarentena AS diferenca
FROM b JOIN g USING (taxi_type) JOIN r USING (taxi_type)
ORDER BY b.taxi_type;
```

O `ROLLUP` gera a linha agregada junto das linhas por frota, o que permite
comparar os três escopos numa consulta só.

Resultado obtido na execução real:

| taxi_type | bronze | gold | quarentena | diferenca |
|---|---|---|---|---|
| green | 339.630 | 313.040 | 26.590 | **0** |
| total | 16.526.016 | 15.651.177 | 874.839 | **0** |
| yellow | 16.186.386 | 15.338.137 | 848.249 | **0** |

---

## 5. Nada regrediu

As métricas e as respostas do case não podem ter mudado.

```sql
SELECT metrica, taxi_type, valor
FROM workspace.nyc_taxi_gold.dq_metrics
ORDER BY taxi_type, metrica;

SELECT * FROM workspace.nyc_taxi_gold.vw_media_total_amount_mes;
```

Esperado: bronze 16.526.016 / gold 15.651.177 (total), e a média mensal em
27,46 · 27,37 · 28,28 · 28,78 · 29,45. Qualquer divergência aqui é problema -
não ajuste de expectativa.

---

## 6. Auditoria: para que a quarentena serve

Antes era possível saber **quantas** linhas caíram por regra; agora dá para ver
**quais**.

```sql
SELECT VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
       passenger_count, total_amount, taxi_type, source_year_month
FROM workspace.nyc_taxi_gold.taxi_trips_rejected
WHERE _reject_reason = 'removidas_r3_total_amount_negativo'
ORDER BY total_amount
LIMIT 5;
```

Esperado: o menor `total_amount` da base, **−982,95**.

```sql
SELECT source_year_month, COUNT(*) AS corridas
FROM workspace.nyc_taxi_gold.taxi_trips_rejected
WHERE _reject_reason = 'removidas_r1_pickup_fora_janela'
GROUP BY source_year_month
ORDER BY source_year_month;
```

Mostra de qual arquivo original vieram as corridas com data fora da janela.

> **Sobre datas nulas**: uma corrida sem `tpep_pickup_datetime` cai na R1 e
> aparece na quarentena com `pickup_year_month` e `pickup_hour` nulos. É o
> comportamento correto - não se inventa data para um registro inválido; a
> coluna `source_year_month` continua indicando o arquivo de origem.

---

## 7. Idempotência

Execute de novo (opção A ou B) e repita a consulta do passo 3: as contagens têm
de ser exatamente as mesmas. A gravação é `overwrite`, então reprocessar não
duplica nada.

---

## Checklist de validação

1. Saída do `DESCRIBE TABLE` (passo 2).
2. Resultado da contagem por motivo e frota (passo 3).
3. Resultado da consulta de particionamento, com a coluna `diferenca` (passo 4).
4. `dq_metrics` completo e a view P1 (passo 5).
5. As 5 linhas da amostra de auditoria (passo 6).
6. Contagens após a segunda execução (passo 7).
