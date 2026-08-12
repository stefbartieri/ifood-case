# Escalabilidade — o que muda, e a partir de quando

## Princípio

Esta solução processa **16,5 milhões de corridas** ocupando **menos de 1 GB**.
Várias decisões aqui só são corretas nessa faixa: aplicar sobre 260 MB o
arsenal de uma plataforma de petabytes seria custo sem retorno, e um
particionamento agressivo em tabela pequena piora o desempenho em vez de
melhorá-lo.

O risco de dimensionar assim não é técnico, é de comunicação: uma escolha
simples pode ser lida como escolha ingênua. Este documento existe para separar
as duas coisas — ele lista, para cada decisão, **a condição numérica em que
ela mudaria**.

Nada descrito na tabela da seção 3 está implementado. São gatilhos, não
recursos. O que já está implementado aparece na seção 2.

## O que já é decisão de escala

Três escolhas do projeto existem por causa de volume e costumam ser lidas como
detalhe técnico.

**Leitura mês a mês com casts explícitos, nunca inferência sobre o diretório**
([bronze_taxi_trips.py](../src/bronze/bronze_taxi_trips.py)). A origem tem
divergência real de tipos entre janeiro e os meses seguintes — `VendorID` de
INT64 para INT32, `passenger_count` de DOUBLE para INT64, e a coluna de taxa de
aeroporto muda de grafia. Um `read.parquet` sobre a pasta inteira resolveria o
schema por amostragem e o erro apareceria como valor nulo, não como falha. Em
volume, esse é o modo de falha mais caro que existe: silencioso e retroativo.

**Bronze particionada por (`taxi_type`, `source_year_month`) com
`replaceWhere`.** Isto já é reprocessamento incremental: corrigir maio reescreve
apenas a partição de maio, sem tocar nos outros meses. É o mecanismo que
sustenta correção pontual e reprocessamento seletivo quando a base cresce.

**Reconciliação como asserção que derruba a execução**
([gold_taxi_trips.py](../src/gold/gold_taxi_trips.py)). A identidade
`bronze == gold + quarentena` é verificada a cada carga e falha a task se não
fechar. Com 16 milhões de linhas ninguém confere no olho — a qualidade precisa
ser uma condição de parada, não um relatório que alguém talvez leia.

## Gatilhos de mudança

Cada linha traz a condição de disparo, não uma intenção genérica.

| Eixo | Gatilho | Mudança | Por quê |
|---|---|---|---|
| **Dados atrasados** | A origem republica um mês já processado (a TLC corrige arquivos retroativamente) | Reprocessar só a competência afetada via `replaceWhere` e reexecutar a reconciliação; registrar a versão Delta anterior antes da sobrescrita | O custo passa a ser proporcional a um mês, não ao histórico. O mecanismo já existe; o que falta é o procedimento e o registro da versão |
| **Evolução de schema** | A origem adiciona, renomeia ou muda o tipo de uma coluna | Manter o mapeamento canônico falhando em coluna desconhecida, e tratar a inclusão como mudança versionada com contrato de dados | Aceitar coluna nova em silêncio é como o schema de um lake apodrece. Já aconteceu nesta base entre 2023-01 e 2023-02 |
| **Ingestão incremental** | A lista de arquivos deixa de ser conhecida de antemão (hoje são 10 nomes previsíveis) ou a listagem do diretório passa a dominar o tempo da carga | Auto Loader com checkpoint; a documentação recomenda **file notification** em vez de listagem de diretório para a maioria das cargas | Hoje a carga é uma lista fixa com verificação de `Content-Length` por arquivo. Isso não sobrevive a chegada contínua. Nota: a documentação não publica um número de arquivos como limiar — o critério é a previsibilidade da chegada |
| **Particionamento e compactação** | Tabela passa de **1 TB** (abaixo disso a recomendação é não particionar) e cada partição sustenta pelo menos **1 GB** | Entre 1 TB e 100 TB, **liquid clustering** em vez de partição estática; acima de 1 TB, `OPTIMIZE` agendado | A gold tem <1 GB e não é particionada, exatamente por isso. A bronze é particionada com partições de ~27 MB — **bem abaixo** do mínimo de 1 GB —, e isso é deliberado: ali a partição serve de unidade de reprocessamento, não de otimização de leitura. Em produção esses dois papéis se separam |
| **Orquestração** | O job passa a rodar sem alguém acompanhando (agendado em vez de disparado à mão) | Retry por task, timeout/SLA, alerta em falha e detecção explícita de arquivo faltante na origem | Hoje a falha é vista porque alguém está olhando o terminal. A verificação de completude já existe como `assert`; o que falta é o canal de aviso |
| **Distribuição e skew** | A razão entre a maior e a menor partição de mesmo nível passa de uma ordem de grandeza | Deixar de particionar por `taxi_type` e passar a clusterizar pelas colunas realmente usadas nos filtros | Hoje yellow é ~98% e green ~2% do volume: o skew já existe, mas em partições de dezenas de MB é irrelevante. Com uma frota como a de aluguel por aplicativo entraria outra escala — **um único mês dela ocupa 451,9 MiB na origem, contra 45,5 MiB de um mês de yellow e 259,8 MiB de todo o conjunto atual** |

## Efeito na arquitetura de camadas

Volume não muda a **quantidade** de camadas; muda o **como** de cada uma
(particionamento, forma de ingestão, compactação). Quem muda a contagem de
camadas é o número de consumidores.

Hoje a limpeza é um estágio único com um único destino materializado, e por
isso vive na gold — como explicado em
[Por que não há camada silver](../README.md#por-que-não-há-camada-silver).
A camada intermediária passa a se justificar quando a base limpa tem **dois ou
mais consumidores materializados**: duas marts lendo o mesmo conjunto tratado,
ou um modelo dimensional em que fato e dimensões partem da mesma base. A partir
daí, não compartilhar a limpeza significa reescrever as mesmas regras em cada
consumidor — e é isso, não o tamanho do dado, que paga o custo de mais um
estágio materializado.

Ampliar o escopo da fonte costuma disparar esse critério de forma indireta:
incluir uma frota sem contagem de passageiros, por exemplo, não responde às
mesmas perguntas e por isso tende a criar uma segunda mart — e é a segunda
mart, não a nova fonte, que justifica o estágio compartilhado.

Uma base dez vezes maior com um consumidor só continua não precisando de
silver. Uma base do tamanho atual com cinco marts precisa.

## O que não mudaria em nenhuma escala

A lista acima é critério aplicado, não catálogo de padrões. Quatro decisões
permanecem idênticas em qualquer volume:

**Timestamps sem conversão de fuso.** A origem entrega horário de parede de
Nova York, sem timezone. Converter distorceria qualquer análise por hora do
dia — e o erro cresce junto com a base, em vez de aparecer.

**Regras de qualidade explícitas e contadas.** Cada regra tem nome, ordem fixa
e contagem persistida. Aumentar o volume torna isso mais necessário, não menos:
é o que permite comparar a taxa de rejeição de hoje com a de ontem.

**Reconciliação exata.** `entrada == saída + rejeitadas`, verificada a cada
carga. Uma identidade que fecha é a única prova barata de que nenhuma linha se
perdeu no caminho.

**Landing imutável com o arquivo original.** Guardar o byte como veio da fonte
é o que permite reconstruir qualquer camada sem depender da origem continuar
disponível — e o que torna qualquer divergência auditável até o arquivo.
