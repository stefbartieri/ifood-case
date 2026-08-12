# Guia manual 003 - Executar a camada gold (limpeza + DQ + views)

> Camada de consumo. Importe e execute o notebook `src/gold/gold_taxi_trips.py`
> e crie as views de `src/gold/sql/create_views.sql`.
> Pré-requisito: bronze populada com 16.526.016 linhas.

## 1. Importar o notebook

Workspace → sua pasta → **Import** → **File** → selecione
`C:\Users\andre\OneDrive\Área de Trabalho\ifood\src\gold\gold_taxi_trips.py`
(mesmo mecanismo do notebook bronze; o notebook é autocontido).

## 2. Executar o notebook (1ª execução)

1. Abra o notebook, conecte ao **Serverless** e clique em **Run all**.
2. A célula de reconciliação imprime, por escopo (`green`, `yellow`, `total`):
   `bronze=... gold=... removidas=... aproveitamento=0.9...` e termina com
   `Reconciliacao OK em todos os escopos` - se falhar o assert, investigue o erro.
3. **[VALIDAÇÃO 1]** Confira a saída da célula de reconciliação e a tabela de
   métricas da última célula (18 linhas).

## 3. Validar o schema da gold (SQL Editor)

```sql
DESCRIBE TABLE workspace.nyc_taxi_gold.taxi_trips;
```

**[VALIDAÇÃO 2]** Confira a saída - esperado: exatamente 8 colunas
(`VendorID` int, `passenger_count` int, `total_amount` double,
`tpep_pickup_datetime` timestamp_ntz, `tpep_dropoff_datetime` timestamp_ntz,
`taxi_type` string, `pickup_year_month` string, `pickup_hour` int).

## 4. Conferência da reconciliação via SQL

```sql
SELECT taxi_type,
       MAX(CASE WHEN metrica = 'linhas_bronze' THEN valor END)
       - MAX(CASE WHEN metrica = 'linhas_gold' THEN valor END)
       - SUM(CASE WHEN metrica LIKE 'removidas_%' THEN valor ELSE 0 END) AS diferenca
FROM workspace.nyc_taxi_gold.dq_metrics
GROUP BY taxi_type;
```

**[VALIDAÇÃO 3]** Confira a saída - esperado: `diferenca = 0` nas 3 linhas
(`yellow`, `green`, `total`).

## 5. Criar as views de consumo

Abra o arquivo local `src/gold/sql/create_views.sql`, cole o conteúdo INTEIRO
no SQL Editor e execute (cria `vw_media_total_amount_mes` e
`vw_media_passageiros_hora_maio`).

## 6. Consultar as views

```sql
SELECT * FROM workspace.nyc_taxi_gold.vw_media_total_amount_mes
ORDER BY pickup_year_month;
```

**[VALIDAÇÃO 4]** Confira a saída (esperado: 5 linhas, 2023-01..2023-05; médias
na faixa ~27-30).

```sql
SELECT * FROM workspace.nyc_taxi_gold.vw_media_passageiros_hora_maio
ORDER BY escopo, pickup_hour;
```

**[VALIDAÇÃO 5]** Confira a saída (esperado: 48 linhas = 2 escopos × 24 horas).

## 7. Reexecutar o notebook (idempotência)

1. **Run all** de novo no notebook `gold_taxi_trips` (overwrite completo).
2. Repita a consulta abaixo e confira a saída:

```sql
SELECT metrica, taxi_type, valor
FROM workspace.nyc_taxi_gold.dq_metrics
ORDER BY taxi_type, metrica;
```

**[VALIDAÇÃO 6]** Os valores devem ser idênticos aos da 1ª execução (só
`run_ts` muda).

## Checklist de validação

- [ ] Reconciliação + métricas do notebook (1ª execução)
- [ ] `DESCRIBE TABLE` da gold (8 colunas)
- [ ] Query de conferência com `diferenca = 0`
- [ ] SELECT da view P1 (5 linhas)
- [ ] SELECT da view P2 (48 linhas)
- [ ] Métricas após a 2ª execução (idênticas)
