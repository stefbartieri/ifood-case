-- Pergunta 1 do case: qual a media de total_amount recebido por mes,
-- considerando todos os yellow taxis da frota (Jan-Mai/2023)?
--
-- Fonte: view workspace.nyc_taxi_gold.vw_media_total_amount_mes,
-- construida sobre a gold workspace.nyc_taxi_gold.taxi_trips (ja filtrada:
-- pickup em Jan-Mai/2023, dropoff > pickup, total_amount >= 0,
-- passenger_count nao nulo e > 0; taxi_type = 'yellow' na propria view).
-- Executar no SQL Warehouse serverless.

SELECT pickup_year_month,
       media_total_amount,
       qtd_corridas
FROM workspace.nyc_taxi_gold.vw_media_total_amount_mes
ORDER BY pickup_year_month;
