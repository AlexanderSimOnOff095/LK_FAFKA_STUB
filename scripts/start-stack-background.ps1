$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$log = Join-Path $repo 'docker-start.log'
Set-Location -LiteralPath $repo
try {
    docker compose up -d --build *>&1 | Out-File -LiteralPath $log -Encoding utf8
    "EXIT_CODE=0" | Add-Content -LiteralPath $log
}
catch {
    $_ | Out-String | Add-Content -LiteralPath $log
    "EXIT_CODE=1" | Add-Content -LiteralPath $log
    exit 1
}
