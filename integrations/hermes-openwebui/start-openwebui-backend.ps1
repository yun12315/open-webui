$ErrorActionPreference = 'Stop'

function Set-DefaultEnv([string]$Name, [string]$Value) {
    $current = [Environment]::GetEnvironmentVariable($Name, 'Process')
    if ([string]::IsNullOrWhiteSpace($current)) {
        Set-Item -Path "Env:$Name" -Value $Value
    }
}

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$DataDir = if ($env:OPENWEBUI_DATA_DIR) { $env:OPENWEBUI_DATA_DIR } else { Join-Path $ScriptDir 'backend-data-hermes-fresh' }
$LogDir = if ($env:OPENWEBUI_LOG_DIR) { $env:OPENWEBUI_LOG_DIR } else { Join-Path $ScriptDir 'logs' }
New-Item -ItemType Directory -Force -Path $DataDir, $LogDir | Out-Null

$BridgeBaseUrl = if ($env:HERMES_OPENWEBUI_BRIDGE_BASE_URL) { $env:HERMES_OPENWEBUI_BRIDGE_BASE_URL.TrimEnd('/') } else { 'http://127.0.0.1:8650' }
$BridgeKey = if ($env:HERMES_OPENWEBUI_BRIDGE_KEY) { $env:HERMES_OPENWEBUI_BRIDGE_KEY } else { 'hermes-openwebui-local-only' }

Set-DefaultEnv 'DATA_DIR' $DataDir
Set-DefaultEnv 'WEBUI_AUTH' 'False'
Set-DefaultEnv 'ENABLE_OLLAMA_API' 'False'
Set-DefaultEnv 'ENABLE_OPENAI_API' 'True'
Set-DefaultEnv 'OPENAI_API_BASE_URL' "$BridgeBaseUrl/v1"
Set-DefaultEnv 'OPENAI_API_KEY' $BridgeKey
Set-DefaultEnv 'DEFAULT_MODELS' 'hermes-agent'
Set-DefaultEnv 'RAG_EMBEDDING_ENGINE' 'openai'
Set-DefaultEnv 'RAG_EMBEDDING_MODEL' 'text-embedding-3-small'
Set-DefaultEnv 'RAG_OPENAI_API_BASE_URL' "$BridgeBaseUrl/v1"
Set-DefaultEnv 'RAG_OPENAI_API_KEY' $BridgeKey
Set-DefaultEnv 'BYPASS_EMBEDDING_AND_RETRIEVAL' 'True'
Set-DefaultEnv 'HF_HUB_DISABLE_SYMLINKS_WARNING' '1'

if ([string]::IsNullOrWhiteSpace($env:WEBUI_SECRET_KEY)) {
    $secretPath = Join-Path $DataDir '.webui_secret_key'
    if (Test-Path -LiteralPath $secretPath) {
        $env:WEBUI_SECRET_KEY = (Get-Content -LiteralPath $secretPath -Raw).Trim()
    } else {
        $bytes = New-Object byte[] 32
        [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
        $env:WEBUI_SECRET_KEY = [Convert]::ToBase64String($bytes)
        Set-Content -LiteralPath $secretPath -Value $env:WEBUI_SECRET_KEY -Encoding UTF8 -NoNewline
    }
}

$OpenWebUIExe = if ($env:OPENWEBUI_EXE) { $env:OPENWEBUI_EXE } else { Join-Path $ScriptDir 'venv\Scripts\open-webui.exe' }
if (-not (Test-Path -LiteralPath $OpenWebUIExe)) {
    throw "OpenWebUI executable not found: $OpenWebUIExe. Set OPENWEBUI_EXE or create a venv beside this script."
}

$LogFile = Join-Path $LogDir 'backend-8080.log'
& $OpenWebUIExe serve --host 127.0.0.1 --port 8080 *>> $LogFile
