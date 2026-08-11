# Utilitario do projeto (Windows/PowerShell).
# Uso: .\scripts\projeto.ps1 <acao>
# Acoes: setup | dados | qualidade | deploy

param([Parameter(Position = 0)][string]$Acao = "ajuda")

$ErrorActionPreference = "Stop"
$raiz = Split-Path -Parent $PSScriptRoot
Set-Location $raiz
$venvPython = Join-Path $raiz ".venv\Scripts\python.exe"

function Invoke-Setup {
    if (-not (Test-Path $venvPython)) {
        $versao = $null
        foreach ($v in "-3.13", "-3.12") {
            & py $v --version *> $null
            if ($LASTEXITCODE -eq 0) { $versao = $v; break }
        }
        if (-not $versao) {
            throw "Python 3.12/3.13 nao encontrado (py -3.13 / py -3.12). O pyspark nao suporta Python 3.14."
        }
        Write-Host "Criando .venv com py $versao..."
        & py $versao -m venv .venv
    }
    & $venvPython -m pip install --upgrade pip
    & $venvPython -m pip install -r requirements.txt
    Write-Host "`nSetup concluido. Para ativar a venv: .venv\Scripts\Activate.ps1"
}

function Invoke-Dados {
    if (-not (Test-Path $venvPython)) { throw "Venv ausente - rode antes: .\scripts\projeto.ps1 setup" }
    & $venvPython src/ingestion/download_tlc.py
    if ($LASTEXITCODE -ne 0) { throw "Download terminou com erro (veja o log acima)." }
    Write-Host "`n10 parquets em data\landing\ (idempotente: re-execucao nao rebaixa nada)."
}

function Invoke-Qualidade {
    if (-not (Test-Path $venvPython)) { throw "Venv ausente - rode antes: .\scripts\projeto.ps1 setup" }
    & $venvPython -m ruff check .
    if ($LASTEXITCODE -ne 0) { throw "ruff encontrou problemas." }
    & $venvPython -m black --check .
    if ($LASTEXITCODE -ne 0) { throw "black encontrou arquivos fora do padrao (rode: black .)." }
    & $venvPython -m pytest -q
    if ($LASTEXITCODE -ne 0) { throw "pytest falhou." }
    Write-Host "`nQualidade OK: ruff + black + pytest."
}

function Invoke-Deploy {
    if (-not (Get-Command databricks -ErrorAction SilentlyContinue)) {
        throw "Databricks CLI nao encontrada (instale: winget install Databricks.DatabricksCLI)."
    }
    if (-not $env:DATABRICKS_HOST -or -not $env:DATABRICKS_TOKEN) {
        throw "Defina DATABRICKS_HOST e DATABRICKS_TOKEN (PAT) antes: `$env:DATABRICKS_HOST='https://...'; `$env:DATABRICKS_TOKEN='...'"
    }
    databricks bundle validate
    if ($LASTEXITCODE -ne 0) { throw "bundle validate falhou." }
    databricks bundle deploy
    if ($LASTEXITCODE -ne 0) { throw "bundle deploy falhou." }
    databricks bundle run pipeline_nyc_taxi
    if ($LASTEXITCODE -ne 0) { throw "bundle run falhou." }
    Write-Host "`nPipeline executado com sucesso via job pipeline_nyc_taxi."
}

switch ($Acao) {
    "setup" { Invoke-Setup }
    "dados" { Invoke-Dados }
    "qualidade" { Invoke-Qualidade }
    "deploy" { Invoke-Deploy }
    default {
        Write-Host "Uso: .\scripts\projeto.ps1 <acao>"
        Write-Host ""
        Write-Host "  setup      cria a venv (Python 3.12/3.13) e instala requirements.txt"
        Write-Host "  dados      baixa os 10 parquets da TLC para data\landing\ (idempotente)"
        Write-Host "  qualidade  roda ruff + black --check + pytest"
        Write-Host "  deploy     bundle validate + deploy + run (exige CLI e PAT configurados)"
    }
}
