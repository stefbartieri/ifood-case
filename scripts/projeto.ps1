# Utilitario do projeto (Windows/PowerShell).
# Uso: .\scripts\projeto.ps1 <acao>
# Acoes: setup | dados | qualidade | landing | deploy

param([Parameter(Position = 0)][string]$Acao = "ajuda")

$ErrorActionPreference = "Stop"
$raiz = Split-Path -Parent $PSScriptRoot
Set-Location $raiz
$venvPython = Join-Path $raiz ".venv\Scripts\python.exe"

$VolumeRaiz = "dbfs:/Volumes/workspace/nyc_taxi_landing/files"
$Frotas = "yellow", "green"
$Meses = "01", "02", "03", "04", "05"

# Descobre um interpretador 3.13/3.12 via "py --list" (o pyspark nao suporta 3.14).
# Nao usa "py -3.13 --version": no PowerShell 5.1 o stderr do py.exe vira
# NativeCommandError e, com ErrorActionPreference=Stop, aborta o script.
function Get-PythonCompativel {
    $anterior = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try { $lista = & py --list 2>$null } catch { $lista = $null }
    $ErrorActionPreference = $anterior
    if ($lista) {
        foreach ($v in "3.13", "3.12") {
            if ($lista -match [regex]::Escape("-V:$v")) { return "-$v" }
        }
    }
    return $null
}

function Invoke-Setup {
    if (-not (Test-Path $venvPython)) {
        $versao = Get-PythonCompativel
        if (-not $versao) {
            throw "Python 3.12/3.13 nao encontrado (py --list). O pyspark nao suporta Python 3.14 - instale com: py install 3.12"
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

# Arquivos de uma frota no Volume, como mapa nome -> bytes. Vazio quando a
# pasta ainda nao existe (Volume recem-criado).
function Get-LandingNoVolume([string]$frota) {
    $mapa = @{}
    $anterior = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try { $saida = & databricks fs ls "$VolumeRaiz/$frota/2023" --output json 2>$null } catch { $saida = $null }
    $codigo = $LASTEXITCODE
    $ErrorActionPreference = $anterior
    if ($codigo -ne 0 -or -not $saida) { return $mapa }
    foreach ($item in ($saida | ConvertFrom-Json)) { $mapa[$item.name] = [int64]$item.size }
    return $mapa
}

function Invoke-Landing {
    Assert-DatabricksPronto
    if (-not (Test-Path $venvPython)) { throw "Venv ausente - rode antes: .\scripts\projeto.ps1 setup" }

    # Os schemas e o Volume sao recursos do bundle (resources/catalogo.yml),
    # entao quem os cria e o deploy - nao ha criacao duplicada aqui.
    Write-Host "[1/4] Garantindo schemas e Volume no workspace..."
    databricks bundle deploy
    if ($LASTEXITCODE -ne 0) { throw "bundle deploy falhou." }

    Write-Host "`n[2/4] Conferindo os parquets locais..."
    & $venvPython src/ingestion/download_tlc.py
    if ($LASTEXITCODE -ne 0) { throw "Download local terminou com erro (veja o log acima)." }

    Write-Host "`n[3/4] Enviando o que falta para o Volume..."
    $enviados = 0
    foreach ($frota in $Frotas) {
        # "fs cp" nao cria diretorios intermediarios: o bundle cria o Volume,
        # mas {frota}/2023 so passa a existir aqui. "mkdir" cria a arvore
        # inteira e e idempotente, entao roda a cada execucao sem efeito extra.
        databricks fs mkdir "$VolumeRaiz/$frota/2023"
        if ($LASTEXITCODE -ne 0) { throw "Falha ao criar $VolumeRaiz/$frota/2023 no Volume." }
        $noVolume = Get-LandingNoVolume $frota
        foreach ($mes in $Meses) {
            $arquivo = "${frota}_tripdata_2023-$mes.parquet"
            $local = Join-Path $raiz "data/landing/$frota/2023/$arquivo"
            $bytesLocais = (Get-Item $local).Length
            if ($noVolume[$arquivo] -eq $bytesLocais) {
                Write-Host "  [ok]     $arquivo ja no Volume ($bytesLocais bytes)"
                continue
            }
            Write-Host "  [upload] $arquivo ($bytesLocais bytes)"
            databricks fs cp $local "$VolumeRaiz/$frota/2023/$arquivo" --overwrite
            if ($LASTEXITCODE -ne 0) { throw "Falha ao enviar $arquivo." }
            $enviados++
        }
    }

    Write-Host "`n[4/4] Conferindo os totais por frota..."
    # Os invariantes vivem no modulo Python; o script nao os duplica.
    $esperado = & $venvPython -c "import json; from src.ingestion.tlc_source import TOTAIS_ESPERADOS_BYTES as t; print(json.dumps(t))" | ConvertFrom-Json
    foreach ($frota in $Frotas) {
        $noVolume = Get-LandingNoVolume $frota
        $total = ($noVolume.Values | Measure-Object -Sum).Sum
        $alvo = $esperado.$frota
        if ($noVolume.Count -ne $Meses.Count -or $total -ne $alvo) {
            throw "Landing de $frota inconsistente: $($noVolume.Count) arquivos, $total bytes (esperado $($Meses.Count) arquivos, $alvo bytes)."
        }
        Write-Host "  [$frota] $($noVolume.Count) arquivos, $total bytes"
    }
    Write-Host "`nLanding pronta ($enviados arquivo(s) enviado(s)). Agora: .\scripts\projeto.ps1 deploy"
}

# Avisa quando a landing esta incompleta, sem impedir o deploy: a task de
# ingestao baixa da origem quando o egresso do workspace alcanca o CDN da TLC,
# e falha em segundos com mensagem acionavel quando nao alcanca.
function Test-LandingPresente {
    $encontrados = 0
    foreach ($frota in $Frotas) { $encontrados += (Get-LandingNoVolume $frota).Count }
    $total = $Frotas.Count * $Meses.Count
    if ($encontrados -eq $total) {
        Write-Host "[pre-flight] landing completa ($encontrados/$total arquivos no Volume)."
        return
    }
    Write-Warning @"
landing incompleta ($encontrados/$total arquivos no Volume)

  A 1a task do job baixa os dados da origem publica, mas o egresso de internet
  do workspace pode nao alcancar o CDN da TLC (allowlist da Free Edition).

  Se a ingestao falhar por rede, carregue a landing da sua maquina com:
    .\scripts\projeto.ps1 landing
"@
}

# Host do target default, so para montar mensagens de erro acionaveis.
function Get-HostDoBundle {
    $linha = Select-String -Path (Join-Path $raiz "databricks.yml") -Pattern "^\s+host:\s*(\S+)" |
        Select-Object -First 1
    if ($linha) { return $linha.Matches[0].Groups[1].Value }
    return "https://<workspace>.cloud.databricks.com"
}

function Assert-DatabricksPronto {
    if (-not (Get-Command databricks -ErrorAction SilentlyContinue)) {
        throw "Databricks CLI nao encontrada (instale: winget install Databricks.DatabricksCLI)."
    }
    # "auth describe" resolve o host pelo target default do bundle, igual ao
    # deploy, e vale para qualquer metodo (perfil, PAT ou service principal).
    # Ter ~/.databrickscfg nao basta: o perfil pode ser de OUTRO workspace.
    # O comando sai com codigo 0 mesmo sem credencial - quem decide e o "status".
    $anterior = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try { $saida = & databricks auth describe -o json 2>$null } catch { $saida = $null }
    $ErrorActionPreference = $anterior
    $descricao = $null
    if ($saida) {
        try { $descricao = ($saida -join "`n") | ConvertFrom-Json } catch { $descricao = $null }
    }
    if ($descricao -and $descricao.status -eq "success") { return }

    $destino = if ($descricao -and $descricao.details.host) { $descricao.details.host } else { Get-HostDoBundle }
    throw @"
Sem credencial valida para o workspace do bundle ($destino). Escolha uma das opcoes:

  1) OAuth (recomendado, o host ja vem do databricks.yml):
     databricks auth login --host $destino

  2) PAT (token pessoal):
     `$env:DATABRICKS_HOST='$destino'; `$env:DATABRICKS_TOKEN='dapi...'

Se voce ja esta logado em outro workspace, o perfil existente nao serve: a
credencial precisa ser do host acima (ou ajuste o host em databricks.yml).
"@
}

function Invoke-Deploy {
    Assert-DatabricksPronto
    Test-LandingPresente
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
    "landing" { Invoke-Landing }
    "deploy" { Invoke-Deploy }
    default {
        Write-Host "Uso: .\scripts\projeto.ps1 <acao>"
        Write-Host ""
        Write-Host "  setup      cria a venv (Python 3.12/3.13) e instala requirements.txt"
        Write-Host "  dados      baixa os 10 parquets da TLC para data\landing\ (idempotente)"
        Write-Host "  qualidade  roda ruff + black --check + pytest"
        Write-Host "  landing    cria schemas/Volume e carrega a landing da sua maquina"
        Write-Host "  deploy     bundle validate + deploy + run (exige CLI autenticada)"
    }
}
