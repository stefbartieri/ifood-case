# Guia manual 005 — Publicar o repositório no GitHub (`ifood-case`)

> Publicação do repositório no GitHub: criação do repo, conexão do remote
> e push da main e das branches de trabalho.

## 1. Criar o repositório `ifood-case` no GitHub

1. Acesse <https://github.com/new> (logado na sua conta).
2. **Repository name**: `ifood-case` (nome exigido pelo enunciado).
3. **Visibilidade — a escolha é sua** (o enunciado aceita ambas):
   - **Pública**: avaliadores acessam sem convite; o código fica visível para
     qualquer pessoa.
   - **Privada**: só quem você convidar enxerga; será preciso convidar o
     avaliador (Settings → Collaborators) ou tornar público depois.
4. **NÃO** marque nenhuma opção de inicialização (sem README, sem .gitignore,
   sem license — o repo local já tem tudo; inicializar criaria conflito).
5. Clique em **Create repository**.

## 2. Verificação pré-push (no PowerShell, na raiz do repo)

```powershell
git branch
```

Esperado: somente `main` e `feat/000...` até `feat/005...` (sem branches de
rascunho).

```powershell
git ls-files | Select-String -Pattern "parquet|\.env|data/"
```

Esperado: **nenhuma linha** (nenhum dado ou segredo versionado).

```powershell
git log --oneline --graph | Select-Object -First 20
```

Esperado: merges `feat(000)`..`feat(005)` na main.

## 3. Conectar o remote

```powershell
git remote add origin https://github.com/<SEU-USUARIO>/ifood-case.git
```

(substitua `<SEU-USUARIO>` pelo seu usuário do GitHub)

## 4. Push da main e de todas as branches

```powershell
git push -u origin main
```

Na primeira vez, o **Git Credential Manager** abre o navegador para autenticar
na sua conta GitHub (padrão no Git para Windows). Se preferir, alternativas:
PAT fine-grained (Settings → Developer settings → Tokens) usado como senha, ou
chave SSH (`git remote set-url origin git@github.com:<usuario>/ifood-case.git`).

```powershell
git push origin --all
```

(publica também as `feat/*`, evidenciando o fluxo de uma branch por entrega)

## 5. Verificação pós-push

1. Abra `https://github.com/<SEU-USUARIO>/ifood-case`.
2. Confira: README renderizado na página inicial (diagrama **Mermaid**
   visível), pastas `src/`, `analysis/`, `docs/`, `tests/` e
   arquivos `requirements.txt` e `pyproject.toml`.
3. Em **branches** (dropdown), confira `main` + as 6 `feat/*`.

## 6. Checklist final

- [ ] URL do repositório acessível;
- [ ] README renderizado com o diagrama visível;
- [ ] Branches `main` e `feat/*` publicadas.
