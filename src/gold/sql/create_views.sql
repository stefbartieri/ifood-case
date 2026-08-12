-- Views de consumo do case. Executar no SQL Editor (warehouse
-- serverless) apos o notebook gold_taxi_trips popular a gold.

-- Pergunta 1: media de total_amount por mes (SO yellow taxis).
-- Os comentarios da view ficam AQUI, na definicao: a view e recriada a cada
-- execucao e sobrescreveria qualquer comentario aplicado por fora.
CREATE OR REPLACE VIEW workspace.nyc_taxi_gold.vw_media_total_amount_mes (
  pickup_year_month COMMENT 'Competencia do embarque no formato AAAA-MM.',
  media_total_amount COMMENT 'Media de total_amount das corridas yellow da competencia, em dolares, arredondada a 2 casas.',
  qtd_corridas COMMENT 'Corridas yellow consideradas na media, ja limpas pelas regras de qualidade.'
)
COMMENT 'Resposta 1: media do valor total por mes, apenas frota yellow. Calculada sobre a camada de consumo.'
AS
SELECT pickup_year_month,
       ROUND(AVG(total_amount), 2) AS media_total_amount,
       COUNT(*)                    AS qtd_corridas
FROM workspace.nyc_taxi_gold.taxi_trips
WHERE taxi_type = 'yellow'
GROUP BY pickup_year_month;

-- Pergunta 2: media de passenger_count por hora do dia em maio/2023, em dois
-- escopos lado a lado (frota_completa = yellow+green; yellow = so yellow).
CREATE OR REPLACE VIEW workspace.nyc_taxi_gold.vw_media_passageiros_hora_maio (
  escopo COMMENT 'Recorte da frota: frota_completa soma yellow e green, yellow considera apenas a frota amarela.',
  pickup_hour COMMENT 'Hora do dia do embarque, de 0 a 23.',
  media_passageiros COMMENT 'Media de passageiros por corrida na hora, arredondada a 3 casas. Mede ocupacao por corrida, nao demanda total.',
  qtd_corridas COMMENT 'Corridas consideradas na media, ja limpas pelas regras de qualidade.'
)
COMMENT 'Resposta 2: media de passageiros por hora do dia em maio de 2023, em dois escopos lado a lado.'
AS
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
