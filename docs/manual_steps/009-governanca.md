# Governança do catálogo — executar e conferir

O catálogo passa a se explicar sozinho: schemas, volume, tabelas, views e
**todas** as colunas ganham comentário, e as tabelas recebem tags de
classificação. Tudo aplicado como código idempotente, junto do pipeline.

O contrato de dados da camada de consumo está em
[../DATA_CONTRACT.md](../DATA_CONTRACT.md).

---

## 1. Fazer o deploy

O job passa de 6 para 7 tasks (a nova, `governanca`, roda por último).

```bash
databricks bundle deploy
```

Resultado esperado: `Deployment complete!`.

---

## 2. Rodar o pipeline

```bash
databricks bundle run pipeline_nyc_taxi
```

Resultado esperado: **7 tasks** concluídas. A task `governanca` aplica os
comandos e falha por conta própria se sobrar qualquer coluna sem comentário —
há um `assert` de cobertura no fim.

> **Se a task falhar por causa de tags**: não vai acontecer silenciosamente. O
> notebook trata falha de `SET TAGS` sem abortar e imprime quais comandos não
> passaram, porque tags de governança podem não estar habilitadas nesta edição
> da plataforma. Os comentários são aplicados de qualquer forma. Se aparecer
> esse aviso, copie-o — a limitação vira uma linha no README em vez de um
> critério em aberto.

---

## 3. Conferir a cobertura

Este é o critério objetivo da entrega: **nenhuma** coluna sem comentário.

```sql
SELECT table_schema, table_name, column_name
FROM workspace.information_schema.columns
WHERE table_schema IN ('nyc_taxi_bronze', 'nyc_taxi_gold')
  AND (comment IS NULL OR trim(comment) = '')
ORDER BY table_schema, table_name, ordinal_position;
```

Resultado esperado: **0 linhas**.

E a contagem total, para confirmar que estamos falando das 53 colunas certas
(24 da bronze + 8 da camada de consumo + 4 das métricas + 10 da quarentena +
7 das duas views):

```sql
SELECT table_name, COUNT(*) AS colunas
FROM workspace.information_schema.columns
WHERE table_schema IN ('nyc_taxi_bronze', 'nyc_taxi_gold')
GROUP BY table_name ORDER BY table_name;
```

---

## 4. Ver o resultado na prática

```sql
DESCRIBE TABLE EXTENDED workspace.nyc_taxi_gold.taxi_trips;
```

Resultado esperado: a coluna `comment` preenchida nas 8 linhas de coluna, e o
comentário da tabela apontando o contrato de dados.

Vale também abrir **Catalog → workspace → nyc_taxi_gold → taxi_trips** na
interface: é ali que a diferença aparece para quem consome sem ler o
repositório.

---

## 5. Conferir as tags

```sql
SELECT * FROM workspace.information_schema.table_tags
WHERE schema_name IN ('nyc_taxi_bronze', 'nyc_taxi_gold');
```

Resultado esperado: 4 tabelas × 5 tags = 20 linhas, com as chaves `camada`,
`dominio`, `fonte`, `contem_pii` e `projeto`.

Se a consulta falhar ou vier vazia e a task tiver avisado sobre tags, trata-se
da indisponibilidade prevista do recurso nesta edição da plataforma -- registre
a limitação em vez de tratar o item como pendente.

---

## 6. Nada regrediu

Metadado não toca em dado. As views foram recriadas (agora com comentários na
definição), então vale confirmar que continuam devolvendo o mesmo:

```sql
SELECT metrica, valor FROM workspace.nyc_taxi_gold.dq_metrics
WHERE taxi_type = 'total' ORDER BY metrica;

SELECT * FROM workspace.nyc_taxi_gold.vw_media_total_amount_mes
ORDER BY pickup_year_month;

SELECT COUNT(*) AS linhas FROM workspace.nyc_taxi_gold.vw_media_passageiros_hora_maio;
```

Esperado: bronze **16.526.016** · gold **15.651.177** · médias
**27,46 · 27,37 · 28,28 · 28,78 · 29,45** · a view da P2 com **48** linhas.

---

## 7. Idempotência

```bash
databricks bundle run pipeline_nyc_taxi
```

Resultado esperado: 7 tasks verdes de novo. Comentário e tag são sobrescritos
com o mesmo texto, então reexecutar não acumula nem duplica nada.

---

## Checklist de validação

1. Evidência das 7 tasks concluídas (e o aviso de tags, se houver).
2. Resultado da consulta de cobertura (deve ser 0 linhas).
3. Contagem de colunas por tabela.
4. `DESCRIBE TABLE EXTENDED` da camada de consumo.
5. Resultado da consulta de tags.
6. As três consultas de não-regressão.
7. Resultado da segunda execução.
