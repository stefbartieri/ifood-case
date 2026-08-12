# Databricks notebook source
# MAGIC %md
# MAGIC # Views de consumo - criacao automatizada
# MAGIC
# MAGIC Executa os comandos de `src/gold/sql/create_views.sql` (fonte unica das
# MAGIC views) via `spark.sql`. Usado como task do job `pipeline_nyc_taxi`
# MAGIC (Asset Bundle), dispensando a execucao manual do SQL no editor.

# COMMAND ----------

# spark e dbutils sao injetados pelo runtime Databricks.
# ruff: noqa: F821

contexto = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
caminho_notebook = contexto.notebookPath().get()
diretorio = "/Workspace" + caminho_notebook.rsplit("/", 1)[0]
caminho_sql = f"{diretorio}/sql/create_views.sql"
print(f"Lendo comandos de: {caminho_sql}")

with open(caminho_sql, encoding="utf-8") as arquivo:
    texto = arquivo.read()

# Remove linhas de comentario ANTES de dividir por ';' - um ';' dentro de
# comentario quebraria um comando ao meio.
texto_sem_comentarios = "\n".join(
    linha for linha in texto.splitlines() if not linha.strip().startswith("--")
)

comandos = [t.strip() for t in texto_sem_comentarios.split(";") if t.strip()]
for comando in comandos:
    primeira_linha = comando.splitlines()[0].strip()
    print(f"Executando: {primeira_linha} ...")
    spark.sql(comando)

print(f"{len(comandos)} comando(s) executado(s)")

# COMMAND ----------

# Verificacao: views existentes no schema gold.
spark.sql("SHOW VIEWS IN workspace.nyc_taxi_gold").show(truncate=False)
