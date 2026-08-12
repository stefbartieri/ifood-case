-- Metadados do catalogo: comentarios e tags aplicados como codigo.
-- Idempotente: reexecutar sobrescreve o mesmo texto.
--
-- Convencoes obrigatorias neste arquivo:
--   1. nenhum apostrofo dentro dos literais (quebraria a string SQL)
--   2. nenhum ponto-e-virgula dentro dos literais (o executor divide por ';')
-- Ambas sao verificadas por tests/test_governance_metadata.py.

-- ---------------------------------------------------------------------------
-- Bloco 1: schemas e volume
-- ---------------------------------------------------------------------------

COMMENT ON SCHEMA workspace.nyc_taxi_landing IS
'Camada de aterrissagem. Guarda os arquivos publicos da NYC TLC exatamente como baixados, sem renomear e sem transformar. Imutavel: serve de fonte para reconstruir qualquer camada sem depender da origem seguir disponivel.';

COMMENT ON VOLUME workspace.nyc_taxi_landing.files IS
'Arquivos parquet mensais da NYC TLC, organizados em {taxi_type}/{ano}/{arquivo_original}.parquet. Integridade conferida por Content-Length na ingestao.';

COMMENT ON SCHEMA workspace.nyc_taxi_bronze IS
'Camada conformada. Unifica as frotas yellow e green num schema canonico de 24 colunas com casts explicitos, preservando 100 por cento das linhas da origem. Nenhum filtro de qualidade e aplicado aqui.';

COMMENT ON SCHEMA workspace.nyc_taxi_gold IS
'Camada de consumo. Dados limpos pelas regras de qualidade documentadas no contrato de dados, mais as tabelas de evidencia (metricas e quarentena) e as views que respondem as perguntas de negocio.';

-- ---------------------------------------------------------------------------
-- Bloco 2: tabelas
-- ---------------------------------------------------------------------------

COMMENT ON TABLE workspace.nyc_taxi_bronze.taxi_trips IS
'Corridas de taxi de Nova York, yellow e green unificadas, de janeiro a maio de 2023. Schema canonico de 24 colunas, particionada por taxi_type e competencia do arquivo de origem. Contem TODAS as linhas da origem, inclusive as invalidas: a limpeza acontece na camada de consumo. 16.526.016 linhas.';

COMMENT ON TABLE workspace.nyc_taxi_gold.taxi_trips IS
'Camada de consumo: uma linha por corrida, ja aprovada nas quatro regras de qualidade. Grao, garantias e politica de mudanca estao no contrato de dados em docs/DATA_CONTRACT.md. 15.651.177 linhas.';

COMMENT ON TABLE workspace.nyc_taxi_gold.dq_metrics IS
'Metricas de qualidade por execucao, em formato longo. Sustenta a reconciliacao exata entrada = publicadas + reprovadas, verificada por assert a cada carga. Sobrescrita a cada execucao.';

COMMENT ON TABLE workspace.nyc_taxi_gold.taxi_trips_rejected IS
'Quarentena: as linhas reprovadas pelas regras de qualidade, anotadas com o motivo. Complemento exato da camada de consumo: as duas juntas reconstituem a camada conformada. Permite auditoria linha a linha do que foi removido. 874.839 linhas.';

-- ---------------------------------------------------------------------------
-- Bloco 3: colunas da bronze (24)
-- ---------------------------------------------------------------------------

ALTER TABLE workspace.nyc_taxi_bronze.taxi_trips ALTER COLUMN VendorID
  COMMENT 'Fornecedor do registro na origem. 1 = Creative Mobile Technologies, 2 = Curb ou VeriFone.';
ALTER TABLE workspace.nyc_taxi_bronze.taxi_trips ALTER COLUMN tpep_pickup_datetime
  COMMENT 'Inicio da corrida, horario local de Nova York sem fuso. Na frota green a origem chama esta coluna de lpep_pickup_datetime, renomeada aqui para unificar as duas frotas.';
ALTER TABLE workspace.nyc_taxi_bronze.taxi_trips ALTER COLUMN tpep_dropoff_datetime
  COMMENT 'Fim da corrida, horario local de Nova York sem fuso. Pode ser anterior ao embarque em registros invalidos, que so sao removidos na camada de consumo.';
ALTER TABLE workspace.nyc_taxi_bronze.taxi_trips ALTER COLUMN passenger_count
  COMMENT 'Passageiros informados pelo motorista, nao medidos. Aceita nulo e zero nesta camada.';
ALTER TABLE workspace.nyc_taxi_bronze.taxi_trips ALTER COLUMN trip_distance
  COMMENT 'Distancia percorrida em milhas, medida pelo taximetro. Contem valores implausiveis que nao sao tratados.';
ALTER TABLE workspace.nyc_taxi_bronze.taxi_trips ALTER COLUMN RatecodeID
  COMMENT 'Tarifa aplicada ao fim da corrida. 1 = padrao, 2 = JFK, 3 = Newark, 4 = Nassau ou Westchester, 5 = valor negociado, 6 = corrida em grupo, 99 = desconhecido.';
ALTER TABLE workspace.nyc_taxi_bronze.taxi_trips ALTER COLUMN store_and_fwd_flag
  COMMENT 'Indica se o registro ficou armazenado no veiculo antes de ser enviado, por falta de conexao. Y = armazenado, N = enviado direto.';
ALTER TABLE workspace.nyc_taxi_bronze.taxi_trips ALTER COLUMN PULocationID
  COMMENT 'Zona de taxi onde o taximetro foi acionado, de 1 a 265, conforme a tabela de zonas da TLC.';
ALTER TABLE workspace.nyc_taxi_bronze.taxi_trips ALTER COLUMN DOLocationID
  COMMENT 'Zona de taxi onde o taximetro foi desligado, de 1 a 265, conforme a tabela de zonas da TLC.';
ALTER TABLE workspace.nyc_taxi_bronze.taxi_trips ALTER COLUMN payment_type
  COMMENT 'Forma de pagamento. 1 = cartao de credito, 2 = dinheiro, 3 = sem cobranca, 4 = disputa, 5 = desconhecido, 6 = corrida anulada. O valor 0 nao consta no dicionario da origem e ocorre nos mesmos registros com passageiros nulos.';
ALTER TABLE workspace.nyc_taxi_bronze.taxi_trips ALTER COLUMN fare_amount
  COMMENT 'Valor calculado pelo taximetro com base em tempo e distancia, em dolares.';
ALTER TABLE workspace.nyc_taxi_bronze.taxi_trips ALTER COLUMN extra
  COMMENT 'Acrescimos e sobretaxas diversas, como adicional de horario de pico e noturno.';
ALTER TABLE workspace.nyc_taxi_bronze.taxi_trips ALTER COLUMN mta_tax
  COMMENT 'Taxa fixa da autoridade metropolitana de transporte, acionada pela tarifa em uso.';
ALTER TABLE workspace.nyc_taxi_bronze.taxi_trips ALTER COLUMN tip_amount
  COMMENT 'Gorjeta. Preenchida automaticamente em pagamentos com cartao. Gorjeta em dinheiro nao e registrada.';
ALTER TABLE workspace.nyc_taxi_bronze.taxi_trips ALTER COLUMN tolls_amount
  COMMENT 'Total de pedagios pagos durante a corrida.';
ALTER TABLE workspace.nyc_taxi_bronze.taxi_trips ALTER COLUMN improvement_surcharge
  COMMENT 'Sobretaxa de melhoria, cobrada no acionamento do taximetro.';
ALTER TABLE workspace.nyc_taxi_bronze.taxi_trips ALTER COLUMN total_amount
  COMMENT 'Valor total cobrado do passageiro, sem gorjeta em dinheiro. Aceita valores negativos nesta camada, que representam estornos e sao removidos na camada de consumo.';
ALTER TABLE workspace.nyc_taxi_bronze.taxi_trips ALTER COLUMN congestion_surcharge
  COMMENT 'Sobretaxa de congestionamento aplicada em parte de Manhattan.';
ALTER TABLE workspace.nyc_taxi_bronze.taxi_trips ALTER COLUMN airport_fee
  COMMENT 'Tarifa de embarque em aeroporto. Exclusiva da frota yellow, nula nos registros green. A origem alterna a grafia entre airport_fee e Airport_fee ao longo dos meses, unificada aqui.';
ALTER TABLE workspace.nyc_taxi_bronze.taxi_trips ALTER COLUMN ehail_fee
  COMMENT 'Tarifa de chamada eletronica. Exclusiva da frota green, nula nos registros yellow.';
ALTER TABLE workspace.nyc_taxi_bronze.taxi_trips ALTER COLUMN trip_type
  COMMENT 'Como a corrida foi iniciada. 1 = na rua, 2 = por despacho. Exclusiva da frota green, nula nos registros yellow.';
ALTER TABLE workspace.nyc_taxi_bronze.taxi_trips ALTER COLUMN taxi_type
  COMMENT 'Linhagem: frota de origem do registro, yellow ou green. Coluna de particionamento.';
ALTER TABLE workspace.nyc_taxi_bronze.taxi_trips ALTER COLUMN source_year_month
  COMMENT 'Linhagem: competencia do ARQUIVO de origem, no formato AAAA-MM, derivada do nome do arquivo. Nao confundir com a data do embarque: arquivos mensais contem corridas de outras competencias. Coluna de particionamento e unidade de reprocessamento.';
ALTER TABLE workspace.nyc_taxi_bronze.taxi_trips ALTER COLUMN ingested_at
  COMMENT 'Linhagem: momento em que a linha foi gravada nesta camada.';

-- ---------------------------------------------------------------------------
-- Bloco 4: colunas da camada de consumo (8)
-- ---------------------------------------------------------------------------

ALTER TABLE workspace.nyc_taxi_gold.taxi_trips ALTER COLUMN VendorID
  COMMENT 'Fornecedor do registro na origem. 1 = Creative Mobile Technologies, 2 = Curb ou VeriFone.';
ALTER TABLE workspace.nyc_taxi_gold.taxi_trips ALTER COLUMN passenger_count
  COMMENT 'Passageiros informados pelo motorista. Garantido preenchido e maior que zero nesta camada.';
ALTER TABLE workspace.nyc_taxi_gold.taxi_trips ALTER COLUMN total_amount
  COMMENT 'Valor total cobrado do passageiro em dolares, sem gorjeta em dinheiro. Garantido maior ou igual a zero nesta camada.';
ALTER TABLE workspace.nyc_taxi_gold.taxi_trips ALTER COLUMN tpep_pickup_datetime
  COMMENT 'Inicio da corrida, horario local de Nova York sem fuso. Garantido dentro da janela de cobertura de janeiro a maio de 2023.';
ALTER TABLE workspace.nyc_taxi_gold.taxi_trips ALTER COLUMN tpep_dropoff_datetime
  COMMENT 'Fim da corrida, horario local de Nova York sem fuso. Garantido posterior ao embarque.';
ALTER TABLE workspace.nyc_taxi_gold.taxi_trips ALTER COLUMN taxi_type
  COMMENT 'Frota de origem da corrida: yellow ou green.';
ALTER TABLE workspace.nyc_taxi_gold.taxi_trips ALTER COLUMN pickup_year_month
  COMMENT 'Competencia do EMBARQUE no formato AAAA-MM, derivada do horario de inicio. Use esta coluna para analises mensais, nao a competencia do arquivo de origem.';
ALTER TABLE workspace.nyc_taxi_gold.taxi_trips ALTER COLUMN pickup_hour
  COMMENT 'Hora do dia do embarque, de 0 a 23, derivada do horario de inicio.';

-- ---------------------------------------------------------------------------
-- Bloco 5: colunas das metricas de qualidade (4)
-- ---------------------------------------------------------------------------

ALTER TABLE workspace.nyc_taxi_gold.dq_metrics ALTER COLUMN run_ts
  COMMENT 'Momento da execucao que produziu estas metricas.';
ALTER TABLE workspace.nyc_taxi_gold.dq_metrics ALTER COLUMN metrica
  COMMENT 'Nome da metrica: linhas_bronze e linhas_gold para os totais, e removidas_r1 a r4 para cada regra de qualidade, na ordem de aplicacao.';
ALTER TABLE workspace.nyc_taxi_gold.dq_metrics ALTER COLUMN taxi_type
  COMMENT 'Escopo da metrica: yellow, green ou total. A reconciliacao fecha nos tres.';
ALTER TABLE workspace.nyc_taxi_gold.dq_metrics ALTER COLUMN valor
  COMMENT 'Contagem de linhas da metrica no escopo.';

-- ---------------------------------------------------------------------------
-- Bloco 6: colunas da quarentena (10)
-- ---------------------------------------------------------------------------

ALTER TABLE workspace.nyc_taxi_gold.taxi_trips_rejected ALTER COLUMN VendorID
  COMMENT 'Fornecedor do registro na origem, como veio da camada conformada.';
ALTER TABLE workspace.nyc_taxi_gold.taxi_trips_rejected ALTER COLUMN passenger_count
  COMMENT 'Passageiros informados. Pode ser nulo ou zero, que e um dos motivos de reprovacao.';
ALTER TABLE workspace.nyc_taxi_gold.taxi_trips_rejected ALTER COLUMN total_amount
  COMMENT 'Valor total cobrado. Pode ser negativo, que e um dos motivos de reprovacao.';
ALTER TABLE workspace.nyc_taxi_gold.taxi_trips_rejected ALTER COLUMN tpep_pickup_datetime
  COMMENT 'Inicio da corrida como veio da origem. Pode estar fora da janela de cobertura ou nulo.';
ALTER TABLE workspace.nyc_taxi_gold.taxi_trips_rejected ALTER COLUMN tpep_dropoff_datetime
  COMMENT 'Fim da corrida como veio da origem. Pode ser anterior ou igual ao embarque.';
ALTER TABLE workspace.nyc_taxi_gold.taxi_trips_rejected ALTER COLUMN taxi_type
  COMMENT 'Frota de origem da corrida: yellow ou green.';
ALTER TABLE workspace.nyc_taxi_gold.taxi_trips_rejected ALTER COLUMN pickup_year_month
  COMMENT 'Competencia do embarque, derivada do horario de inicio. Nula quando o embarque e nulo, porque nao se inventa data para registro invalido.';
ALTER TABLE workspace.nyc_taxi_gold.taxi_trips_rejected ALTER COLUMN pickup_hour
  COMMENT 'Hora do embarque, derivada do horario de inicio. Nula quando o embarque e nulo.';
ALTER TABLE workspace.nyc_taxi_gold.taxi_trips_rejected ALTER COLUMN source_year_month
  COMMENT 'Competencia do ARQUIVO de origem. Permite rastrear a linha reprovada ate o arquivo publicado pela TLC.';
ALTER TABLE workspace.nyc_taxi_gold.taxi_trips_rejected ALTER COLUMN `_reject_reason`
  COMMENT 'Primeira regra de qualidade violada pela linha. Corresponde ao nome da metrica em dq_metrics, o que permite conferir a contagem contra a quarentena.';

-- ---------------------------------------------------------------------------
-- Bloco 7: tags de classificacao (isolado de proposito)
--
-- Este bloco vai por ultimo porque tags de governanca podem nao estar
-- habilitadas em toda edicao da plataforma. O executor trata falha aqui sem
-- abortar: os comentarios acima ja terao sido aplicados.
--
-- Vocabulario fixo: camada, dominio, fonte, contem_pii, projeto.
-- ---------------------------------------------------------------------------

ALTER TABLE workspace.nyc_taxi_bronze.taxi_trips SET TAGS (
  'camada' = 'bronze', 'dominio' = 'nyc_taxi', 'fonte' = 'nyc_tlc',
  'contem_pii' = 'nao', 'projeto' = 'nyc_taxi_lakehouse');

ALTER TABLE workspace.nyc_taxi_gold.taxi_trips SET TAGS (
  'camada' = 'gold', 'dominio' = 'nyc_taxi', 'fonte' = 'nyc_tlc',
  'contem_pii' = 'nao', 'projeto' = 'nyc_taxi_lakehouse');

ALTER TABLE workspace.nyc_taxi_gold.dq_metrics SET TAGS (
  'camada' = 'gold', 'dominio' = 'nyc_taxi', 'fonte' = 'nyc_tlc',
  'contem_pii' = 'nao', 'projeto' = 'nyc_taxi_lakehouse');

ALTER TABLE workspace.nyc_taxi_gold.taxi_trips_rejected SET TAGS (
  'camada' = 'gold', 'dominio' = 'nyc_taxi', 'fonte' = 'nyc_tlc',
  'contem_pii' = 'nao', 'projeto' = 'nyc_taxi_lakehouse');
