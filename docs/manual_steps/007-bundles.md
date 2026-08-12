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

Feche e reabra o terminal e confirme com `databricks version`. Versão usada e
validada neste projeto: **v1.11.0**.

## 2. Autenticar no workspace

A credencial precisa ser **do host que está em `databricks.yml`** (target
`dev`). Estar logado em outro workspace não serve: a CLI resolve o host pelo
bundle e falha com `cannot configure default credentials`.

**Opção A - OAuth (recomendado):**

```powershell
databricks auth login --host https://<id-do-workspace>.cloud.databricks.com
```

Abre o navegador e grava um perfil em `~/.databrickscfg`.

**Opção B - PAT:** gere em **Settings → Developer → Access tokens → Generate
new token** (nunca commite o token; o `.gitignore` cobre `.env`) e exporte:

```powershell
$env:DATABRICKS_HOST = "https://<id-do-workspace>.cloud.databricks.com"
$env:DATABRICKS_TOKEN = "<SEU-PAT>"
```

**Conferência (qualquer opção):**

```powershell
databricks auth describe -o json
```

O campo `status` deve ser `success` e o `host` deve vir com
`"source": {"type": "bundle"}`. Atenção: esse comando sai com código 0 mesmo
sem credencial - o que vale é o `status`. É exatamente essa checagem que
`.\scripts\projeto.ps1 deploy` faz antes de chamar o bundle.

Para trocar de workspace: mude o `host` do target em `databricks.yml` (ou crie
um target novo e use `-t <nome>`) **e** repita esta seção para o novo host.

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

**[VALIDAÇÃO 2]** As 7 tasks (`ingestao`, `bronze`, `gold`, `views`,
`analise_p1`, `analise_p2`, `governanca`) concluem com sucesso (verdes na UI /
exit sem erro no CLI).

> A task `ingestao` baixa os parquets da origem pública. Se o egresso do
> workspace não alcançar o CDN da TLC (allowlist da Free Edition, que varia
> entre workspaces), ela falha em segundos. Nesse caso carregue a landing da
> sua máquina com `.\scripts\projeto.ps1 landing` (ou `./scripts/projeto.sh
> landing`) e rode de novo: a ingestão detecta a landing íntegra e dispensa a
> rede.

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

- [ ] `databricks auth describe -o json` com `status: success` no host do bundle;
- [ ] `databricks bundle validate` sem erro;
- [ ] Job criado e 7 tasks verdes no `bundle run`;
- [ ] `dq_metrics` com os valores esperados após a execução;
- [ ] (Para outro workspace) host/target ajustado em `databricks.yml` **e**
      `databricks auth login --host <novo-host>` feito para ele.
