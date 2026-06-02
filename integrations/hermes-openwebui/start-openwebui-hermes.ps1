$ErrorActionPreference = 'Stop'

function Test-Http([string]$Url, [int]$TimeoutSec = 5) {
    try { return Invoke-WebRequest -Uri $Url -TimeoutSec $TimeoutSec -UseBasicParsing -ErrorAction Stop } catch { return $null }
}

function Wait-Http([string]$Url, [int]$TimeoutSec = 240) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        $response = Test-Http -Url $Url -TimeoutSec 5
        if ($null -ne $response) { return $true }
        Start-Sleep -Seconds 2
    }
    return $false
}

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$WslDistro = if ($env:HERMES_WSL_DISTRO) { $env:HERMES_WSL_DISTRO } else { 'Ubuntu' }
$WslUser = if ($env:HERMES_WSL_USER) { $env:HERMES_WSL_USER } else { 'root' }
$HermesPython = if ($env:HERMES_WSL_PYTHON) { $env:HERMES_WSL_PYTHON } else { '/root/.hermes/hermes-agent/venv/bin/python3' }
$WslScriptDir = (& wsl.exe -d $WslDistro -u $WslUser -- wslpath -a $ScriptDir).Trim()
if ([string]::IsNullOrWhiteSpace($WslScriptDir)) { throw "Unable to convert script directory to a WSL path: $ScriptDir" }

try {
    & wsl.exe -d $WslDistro -u $WslUser -- $HermesPython "$WslScriptDir/auto-select-codex-model.py" | Out-Null
} catch {
    # Model auto-select failure should not block the UI startup path.
}

if ($null -eq (Test-Http -Url 'http://127.0.0.1:8642/health' -TimeoutSec 5)) {
    & wsl.exe -d $WslDistro -u $WslUser -- bash "$WslScriptDir/start-hermes-gateway.sh" | Out-Null
    if (-not (Wait-Http -Url 'http://127.0.0.1:8642/health' -TimeoutSec 180)) { throw 'Hermes gateway failed to start on 8642' }
}

if ($null -eq (Test-Http -Url 'http://127.0.0.1:8650/health' -TimeoutSec 5)) {
    & wsl.exe -d $WslDistro -u $WslUser -- bash "$WslScriptDir/start-hermes-openwebui-bridge.sh" | Out-Null
    if (-not (Wait-Http -Url 'http://127.0.0.1:8650/health' -TimeoutSec 120)) { throw 'Hermes OpenWebUI bridge failed to start on 8650' }
}

try {
    $LocalPython = Join-Path $ScriptDir 'venv\Scripts\python.exe'
    $SyncScript = Join-Path $ScriptDir 'sync-hermes-commands-to-openwebui.py'
    & $LocalPython $SyncScript | Out-Null
} catch {
    # Prompt command sync failure should not block the UI startup path.
}

if ($null -eq (Test-Http -Url 'http://127.0.0.1:8080/health' -TimeoutSec 5)) {
    Get-Process -Name 'open-webui','python' -ErrorAction SilentlyContinue |
        Where-Object { $_.Path -and $_.Path.StartsWith($ScriptDir, [System.StringComparison]::OrdinalIgnoreCase) } |
        Stop-Process -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 2
    Start-Process -FilePath (Join-Path $ScriptDir 'start-openwebui-backend.cmd') -WindowStyle Hidden | Out-Null
    if (-not (Wait-Http -Url 'http://127.0.0.1:8080/health' -TimeoutSec 240)) { throw 'Open WebUI backend failed to start on 8080' }
}

Start-Process 'http://127.0.0.1:8080' | Out-Null
