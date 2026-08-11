-- Pergunta 2 do case: qual a media de passenger_count por hora do dia, para
-- as corridas de maio/2023? Respondida em DOIS escopos lado a lado:
--   frota_completa = yellow + green (interpretacao "todos os taxis");
--   yellow         = apenas yellow (comparabilidade com a pergunta 1).
--
-- Fonte: view workspace.nyc_taxi_gold.vw_media_passageiros_hora_maio
-- sobre a gold ja limpa; mes definido pela derivada
-- pickup_year_month = '2023-05' (momento do embarque, nao do arquivo).
-- Executar no SQL Warehouse serverless.

SELECT escopo,
       pickup_hour,
       media_passageiros,
       qtd_corridas
FROM workspace.nyc_taxi_gold.vw_media_passageiros_hora_maio
ORDER BY escopo, pickup_hour;
