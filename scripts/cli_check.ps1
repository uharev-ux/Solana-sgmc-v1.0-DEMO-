# CLI matrix: help + exit codes. PowerShell only.
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Project root = parent of scripts/ (one level up from $PSScriptRoot)
$projRoot = Split-Path -Parent $PSScriptRoot
if (-not $projRoot) { $projRoot = (Get-Location).Path }
Set-Location $projRoot

# 1) Main help
try {
    $out = python -m dexscreener_screener.cli -h 2>&1
    if ($LASTEXITCODE -ne 0) { throw "cli -h exit $LASTEXITCODE" }
    if (-not ($out -match "collect|trigger|prune")) { throw "cli -h output unexpected" }
} catch {
    Write-Error "FAIL: cli -h: $_"
    exit 1
}
Write-Host "PASS: cli -h"

# 2) trigger help
try {
    $out = python -m dexscreener_screener.cli trigger -h 2>&1
    if ($LASTEXITCODE -ne 0) { throw "trigger -h exit $LASTEXITCODE" }
} catch {
    Write-Error "FAIL: trigger -h: $_"
    exit 1
}
Write-Host "PASS: trigger -h"

# 3) Negative: nonexistent DB -> exit 1 (suppress stderr so PowerShell does not treat logger output as failure)
$nonexistent = Join-Path $projRoot "nonexistent.sqlite"
if (Test-Path $nonexistent) { Remove-Item $nonexistent -Force }
$ErrorActionPreferencePrev = $ErrorActionPreference
$ErrorActionPreference = "SilentlyContinue"
python -m dexscreener_screener.cli trigger --once --db $nonexistent 2>$null | Out-Null
$exit = $LASTEXITCODE
$ErrorActionPreference = $ErrorActionPreferencePrev
Write-Host "trigger --once --db nonexistent.sqlite => exit $exit (expected 1)"
if ($exit -ne 1) {
    Write-Error "FAIL: expected exit 1 for nonexistent DB, got $exit"
    exit 1
}
Write-Host "PASS: trigger with nonexistent DB returns 1"
Write-Host "CLI_CHECK: OK"
