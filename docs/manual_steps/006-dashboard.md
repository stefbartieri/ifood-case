# Guia manual 006 - Dashboard AI/BI com as respostas do case

> Entrega extra: dashboard nativo do Databricks SQL apresentando as duas
> respostas do case a partir das views da gold. Pré-requisito: views criadas
> e validadas (guia 003).

## 1. Verificar o SQL Warehouse

1. Menu lateral → **SQL Warehouses**.
2. Confirme que o warehouse serverless (2X-Small, "Serverless Starter
   Warehouse") existe e clique em **Start** se estiver parado.
3. Aguarde o estado **Running** (a primeira query pode ter um cold start de
   alguns segundos).

## 2. Teste de fumaça nas views (SQL Editor)

Execute e confira:

```sql
SELECT * FROM workspace.nyc_taxi_gold.vw_media_total_amount_mes
ORDER BY pickup_year_month;   -- esperado: 5 linhas (2023-01..2023-05)

SELECT * FROM workspace.nyc_taxi_gold.vw_media_passageiros_hora_maio
ORDER BY escopo, pickup_hour; -- esperado: 48 linhas (24 horas x 2 escopos)
```

Se qualquer uma falhar, pare aqui e reexecute o guia 003 antes de continuar.

## 3. Criar o dashboard

1. Menu lateral → **Dashboards** → **Create dashboard**.
2. Renomeie (clique no título no topo) para: `NYC Taxi - Respostas do Case`.

## 4. Criar os 2 datasets (aba Data)

1. Na aba **Data** do dashboard, clique em **+ Create from SQL** e crie:
   - Dataset 1 - nome `p1_media_total_amount_mes`:

     ```sql
     SELECT * FROM workspace.nyc_taxi_gold.vw_media_total_amount_mes
     ```

   - Dataset 2 - nome `p2_media_passageiros_hora_maio`:

     ```sql
     SELECT * FROM workspace.nyc_taxi_gold.vw_media_passageiros_hora_maio
     ```

2. Rode cada um (Run) para materializar a pré-via dos dados.

## 5. Visualização P1 - barras (média por mês)

1. Na aba **Canvas**, clique em **Add a visualization** e posicione o widget.
2. Configure: **Dataset** = `p1_media_total_amount_mes`; **Visualization** =
   **Bar**; **X axis** = `pickup_year_month`; **Y axis** =
   `media_total_amount` (agregação SUM ou AVG dá o mesmo resultado - há 1
   linha por mês).
3. Título do widget: `P1 - Média de total_amount por mês (yellow, 2023)`.

## 6. Visualização P2 - linhas (média por hora, 2 séries)

1. Adicione outra visualização: **Dataset** = `p2_media_passageiros_hora_maio`;
   **Visualization** = **Line**; **X axis** = `pickup_hour`; **Y axis** =
   `media_passageiros`; **Color/Group by** = `escopo` (gera as séries
   `frota_completa` e `yellow`).
2. Título do widget: `P2 - Média de passenger_count por hora (maio/2023)`.

## 7. Publicar

Clique em **Publish** (canto superior direito) e confirme (publicação interna
default, sem compartilhamento externo).
