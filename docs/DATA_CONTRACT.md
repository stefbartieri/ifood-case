# Contrato de dados — `workspace.nyc_taxi_gold.taxi_trips`

Este documento descreve o que a camada de consumo garante, o que ela não
garante e o que caracteriza uma mudança quebrando compatibilidade. Vale para a
tabela `workspace.nyc_taxi_gold.taxi_trips` e para as tabelas de apoio listadas
no fim.

**Público**: quem consulta a camada de consumo por SQL ou constrói análises,
painéis e relatórios sobre ela.

## Grão e cobertura

Uma linha por corrida concluída, das frotas **yellow** e **green** de Nova York.

- **Janela temporal**: embarques (`tpep_pickup_datetime`) de **2023-01-01
  00:00:00** até **2023-05-31 23:59:59**, em intervalo meio-aberto
  `[2023-01-01, 2023-06-01)`.
- **Volume atual**: 15.651.177 corridas.
- **Chave**: não há chave natural confiável na origem. Consultas devem tratar a
  tabela como fato sem identificador único de corrida.

## Schema

| Coluna | Tipo | Nulo? | Semântica |
|---|---|---|---|
| `VendorID` | INT | não | Fornecedor do registro na origem. 1 = Creative Mobile Technologies, 2 = Curb/VeriFone |
| `passenger_count` | INT | não | Passageiros informados pelo motorista. Sempre maior que zero nesta camada |
| `total_amount` | DOUBLE | não | Valor total cobrado do passageiro, em dólares, sem gorjeta em dinheiro. Nunca negativo nesta camada |
| `tpep_pickup_datetime` | TIMESTAMP_NTZ | não | Início da corrida, horário local de Nova York, sem fuso |
| `tpep_dropoff_datetime` | TIMESTAMP_NTZ | não | Fim da corrida, horário local de Nova York. Sempre posterior ao embarque |
| `taxi_type` | STRING | não | Frota de origem: `yellow` ou `green` |
| `pickup_year_month` | STRING | não | Competência do embarque no formato `AAAA-MM`, derivada de `tpep_pickup_datetime` |
| `pickup_hour` | INT | não | Hora do dia do embarque, de 0 a 23, derivada de `tpep_pickup_datetime` |

Os cinco primeiros nomes reproduzem exatamente a nomenclatura da origem, o que
inclui a manutenção do prefixo `tpep_` também nos registros da frota green,
cuja origem usa `lpep_`.

**Fusos**: a origem entrega horário de parede de Nova York, sem informação de
fuso, e a camada de consumo preserva isso. Nenhuma conversão é aplicada.
Converter distorceria qualquer leitura por hora do dia.

## Garantias de qualidade

Toda linha publicada satisfaz as quatro regras abaixo, aplicadas na ordem. Uma
linha reprovada não é descartada: vai para a quarentena com o motivo.

| Regra | Condição garantida | Linhas reprovadas |
|---|---|---|
| R1 | Embarque dentro da janela de cobertura | 113 |
| R2 | Desembarque posterior ao embarque | 6.595 |
| R3 | `total_amount` maior ou igual a zero | 142.294 |
| R4 | `passenger_count` preenchido e maior que zero | 725.837 |

Cada linha reprovada é atribuída à **primeira** regra que viola, o que torna a
soma exata e verificável:

```
16.526.016 (entrada) = 15.651.177 (publicadas) + 874.839 (reprovadas)
```

Essa identidade é verificada a cada carga e **interrompe a execução** se não
fechar. O aproveitamento atual é de 94,71%.

## Atualização

- **Modo**: sobrescrita completa a cada execução. Não há carga incremental
  nesta camada.
- **Idempotência**: reexecutar produz exatamente o mesmo resultado. Números
  iguais entre execuções são esperados e servem de verificação.
- **Origem**: arquivos mensais publicados pela NYC TLC, preservados sem
  alteração na landing.

## O que este contrato não garante

Estes pontos são conhecidos e deliberadamente preservados — filtrá-los seria
uma decisão de negócio que a camada de consumo não toma sozinha:

- **Sem deduplicação**: não há chave natural confiável, então corridas
  repetidas na origem permanecem.
- **Outliers de distância e valor não são tratados**: a base contém corridas
  com distância implausível, e os valores extremos de `total_amount`
  permanecem desde que não sejam negativos.
- **Códigos não documentados permanecem**: `payment_type = 0` não consta no
  dicionário da origem e ocorre em 428.665 registros da entrada, exatamente os
  mesmos com `passenger_count` nulo. `RatecodeID = 99` significa desconhecido.
- **`passenger_count` é declarado pelo motorista**, não medido. É um dado de
  qualidade variável por natureza.

## Política de mudança

**Quebra compatibilidade** — exige aviso prévio aos consumidores:

- remover ou renomear coluna;
- mudar o tipo de uma coluna;
- mudar o grão da tabela ou a janela de cobertura;
- tornar mais frouxa qualquer garantia de qualidade (por exemplo, passar a
  publicar `total_amount` negativo).

**Não quebra compatibilidade**:

- acrescentar coluna nova ao fim do schema;
- tornar uma garantia mais restritiva;
- mudanças de desempenho, particionamento ou layout físico;
- alteração de comentários e tags no catálogo.

Consultas que dependem de `SELECT *` ou de posição de coluna assumem risco que
este contrato não cobre.

## Tabelas de apoio

| Objeto | Para que serve |
|---|---|
| `workspace.nyc_taxi_gold.dq_metrics` | Contagem por regra e por frota de cada carga, com a marca de tempo da execução. É a fonte da reconciliação |
| `workspace.nyc_taxi_gold.taxi_trips_rejected` | Quarentena: as linhas reprovadas, com `_reject_reason` e a competência do arquivo de origem, para auditoria linha a linha |
| `workspace.nyc_taxi_gold.vw_media_total_amount_mes` | Média mensal de `total_amount` da frota yellow |
| `workspace.nyc_taxi_gold.vw_media_passageiros_hora_maio` | Média de passageiros por hora em maio, nos escopos `frota_completa` e `yellow` |
| `workspace.nyc_taxi_bronze.taxi_trips` | Camada intermediária conformada, com todas as linhas da origem e sem filtro. Não é camada de consumo |
