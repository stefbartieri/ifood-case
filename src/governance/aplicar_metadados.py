# Databricks notebook source
# MAGIC %md
# MAGIC # Governanca — comentarios e tags no catalogo
# MAGIC
# MAGIC Executa `src/governance/catalog_metadata.sql` (fonte unica dos metadados)
# MAGIC via `spark.sql`. Idempotente: reexecutar sobrescreve o mesmo texto.
# MAGIC
# MAGIC O bloco de tags fica isolado no fim do arquivo e roda em modo tolerante:
# MAGIC tags de governanca podem nao estar habilitadas em toda edicao da
# MAGIC plataforma, e uma falha ali nao pode desfazer os comentarios ja aplicados.
# MAGIC
# MAGIC Ao final imprime a cobertura: colunas sem comentario nos schemas do
# MAGIC projeto, que deve ser zero.

# COMMAND ----------

# spark e dbutils sao injetados pelo runtime Databricks.
# ruff: noqa: F821

contexto = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
caminho_notebook = contexto.notebookPath().get()
diretorio = "/Workspace" + caminho_notebook.rsplit("/", 1)[0]
caminho_sql = f"{diretorio}/catalog_metadata.sql"
print(f"Lendo comandos de: {caminho_sql}")

with open(caminho_sql, encoding="utf-8") as arquivo:
    texto = arquivo.read()


def dividir_comandos(sql: str) -> list[str]:
    """Divide o script em comandos.

    Remove as linhas de comentario ANTES de dividir por ';' — um ';' dentro de
    comentario partiria um comando ao meio. Este arquivo e majoritariamente
    texto, entao a ordem das duas operacoes importa.
    """
    sem_comentarios = "\n".join(
        linha for linha in sql.splitlines() if not linha.strip().startswith("--")
    )
    return [c.strip() for c in sem_comentarios.split(";") if c.strip()]


comandos = dividir_comandos(texto)
print(f"{len(comandos)} comando(s) a executar")

# COMMAND ----------

# Comentarios (obrigatorios) e tags (tolerantes a falha).

aplicados, falhas_tag = 0, []

for comando in comandos:
    e_tag = " SET TAGS " in f" {comando} ".upper().replace("\n", " ")
    try:
        spark.sql(comando)
        aplicados += 1
    except Exception as exc:
        if not e_tag:
            raise
        falhas_tag.append((comando.splitlines()[0].strip(), f"{type(exc).__name__}"))

print(f"Comandos aplicados: {aplicados} de {len(comandos)}")
if falhas_tag:
    print(
        f"\nATENCAO: {len(falhas_tag)} comando(s) de TAG falharam. "
        "Os comentarios foram aplicados normalmente. Provavel indisponibilidade "
        "do recurso nesta edicao da plataforma:"
    )
    for primeira_linha, erro in falhas_tag:
        print(f"  [{erro}] {primeira_linha}")

# COMMAND ----------

# Cobertura: nenhuma coluna dos schemas do projeto pode ficar sem comentario.

cobertura = spark.sql(
    """
    SELECT table_schema, table_name, column_name
    FROM workspace.information_schema.columns
    WHERE table_schema IN ('nyc_taxi_bronze', 'nyc_taxi_gold')
      AND (comment IS NULL OR trim(comment) = '')
    ORDER BY table_schema, table_name, ordinal_position
    """
)

sem_comentario = cobertura.count()
total = spark.sql(
    """
    SELECT COUNT(*) AS n
    FROM workspace.information_schema.columns
    WHERE table_schema IN ('nyc_taxi_bronze', 'nyc_taxi_gold')
    """
).collect()[0]["n"]

print(f"Colunas nos schemas do projeto: {total}")
print(f"Colunas SEM comentario: {sem_comentario}")

if sem_comentario:
    cobertura.show(60, truncate=False)

assert sem_comentario == 0, (
    f"{sem_comentario} coluna(s) sem comentario — cobertura parcial e pior que "
    "nenhuma, porque sinaliza documentacao abandonada"
)

print("Cobertura de comentarios: 100 por cento")
