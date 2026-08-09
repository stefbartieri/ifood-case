-- Views de consumo do case. Executar no SQL Editor (warehouse
-- serverless) apos o notebook gold_taxi_trips popular a gold.

-- Pergunta 1: media de total_amount por mes (SO yellow taxis).
CREATE OR REPLACE VIEW workspace.nyc_taxi_gold.vw_media_total_amount_mes AS
SELECT pickup_year_month,
       ROUND(AVG(total_amount), 2) AS media_total_amount,
       COUNT(*)                    AS qtd_corridas
FROM workspace.nyc_taxi_gold.taxi_trips
WHERE taxi_type = 'yellow'
GROUP BY pickup_year_month;

-- Pergunta 2: media de passenger_count por hora do dia em maio/2023, em dois
-- escopos lado a lado (frota_completa = yellow+green; yellow = so yellow).
CREATE OR REPLACE VIEW workspace.nyc_taxi_gold.vw_media_passageiros_hora_maio AS
SELECT 'frota_completa' AS escopo, pickup_hour,
       ROUND(AVG(passenger_count), 3) AS media_passageiros,
       COUNT(*) AS qtd_corridas
FROM workspace.nyc_taxi_gold.taxi_trips
WHERE pickup_year_month = '2023-05'
GROUP BY pickup_hour
UNION ALL
SELECT 'yellow' AS escopo, pickup_hour,
       ROUND(AVG(passenger_count), 3) AS media_passageiros,
       COUNT(*) AS qtd_corridas
FROM workspace.nyc_taxi_gold.taxi_trips
WHERE pickup_year_month = '2023-05' AND taxi_type = 'yellow'
GROUP BY pickup_hour;
