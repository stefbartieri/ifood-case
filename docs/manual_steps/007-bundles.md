# Guia manual 007 - Deploy e execução automatizados (Asset Bundles)

> Alternativa automatizada ao fluxo manual dos guias 002-004: um comando sobe
> os notebooks e cria o job; outro executa o pipeline completo
> (bronze → gold → views → análises). Pré-requisito: landing carregada
> (guia 001).

## 1. Instalar a Databricks CLI

A CLI é um binário standalone (NÃO é pacote pip):

```powershell
winget install Databricks.DatabricksCLI
```

(alternativa: baixar release em https://github.com/databricks/cli/releases)

Feche e reabra o terminal; confirme com `databricks --version` (v0.2xx+).

## 2. Autenticar no workspace

1. Gere um PAT no workspace: **Settings → Developer → Access tokens →
   Generate new token** (nunca commite o token; o `.gitignore` cobre `.env`).
2. No PowerShell:

```powershell
$env:DATABRICKS_HOST = "https://dbc-3e5721dd-fe65.cloud.databricks.com"
$env:DATABRICKS_TOKEN = "<SEU-PAT>"
```

## 3. Validar a configuração do bundle

Na raiz do repositório:

```powershell
databricks bundle validate
```

**[VALIDAÇÃO 1]** Saída esperada: `Name: ifood-case`, `Target: dev` e
`Validation OK!` (sem erros).

## 4. Deploy (sobe notebooks + cria o job)

```powershell
databricks bundle deploy
```

Saída esperada: upload dos arquivos e `Deployment complete!`. No workspace,
o job aparece em **Jobs & Pipelines** com o nome
`[ifood-case] pipeline nyc taxi (bronze -> gold -> views -> analises)` e os
arquivos em `Workspace → Users → <você> → .bundle/ifood-case/`.

## 5. Executar o pipeline completo

```powershell
databricks bundle run pipeline_nyc_taxi
```

O comando acompanha a execução (também visível na UI em Jobs & Pipelines →
Runs). Tempo esperado: alguns minutos (o mesmo dos notebooks manuais).

**[VALIDAÇÃO 2]** As 5 tasks (`bronze`, `gold`, `views`, `analise_p1`,
`analise_p2`) concluem com sucesso (verdes na UI / exit sem erro no CLI).

## 6. Conferir a idempotência (dq_metrics)

No **SQL Editor**:

```sql
SELECT metrica, taxi_type, valor
FROM workspace.nyc_taxi_gold.dq_metrics
ORDER BY taxi_type, metrica;
```

**[VALIDAÇÃO 3]** Valores esperados (idênticos aos já validados): total -
`linhas_bronze` 16526016, `linhas_gold` 15651177; removidas r1 113, r2 6595,
r3 142294, r4 725837 (yellow e green com os parciais conhecidos).

## 7. Checklist de validação

- [ ] `databricks bundle validate` sem erro;
- [ ] Job criado e 5 tasks verdes no `bundle run`;
- [ ] `dq_metrics` com os valores esperados após a execução;
- [ ] (Para outro workspace) novo target em `databricks.yml` + `-t <nome>`
      nos comandos.
