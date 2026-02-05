# Prune: dry-run, then real prune, then verify schema. PowerShell only.
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$projRoot = Split-Path -Parent $PSScriptRoot
if (-not $projRoot) { $projRoot = (Get-Location).Path }
Set-Location $projRoot

$dbPath = Join-Path $projRoot "debug_live.sqlite"
if (-not (Test-Path $dbPath)) {
    Write-Host "prune_check: debug_live.sqlite not found (run e2e_live first or create empty DB)"
    $null = New-Item -ItemType File -Path $dbPath -Force
    python -c "import sys; sys.path.insert(0, r'$($projRoot -replace '\\','/')'); from dexscreener_screener.storage import Database; Database(r'$($dbPath -replace '\\','/')').close()"
    if ($LASTEXITCODE -ne 0) { Write-Error "FAIL: could not init DB"; exit 1 }
}

$ErrPrev = $ErrorActionPreference
$ErrorActionPreference = "SilentlyContinue"
python -m dexscreener_screener.cli prune --dry-run --db $dbPath 2>$null | Out-Null
$ErrorActionPreference = $ErrPrev
if ($LASTEXITCODE -ne 0) { Write-Error "FAIL: prune --dry-run exit $LASTEXITCODE"; exit 1 }
Write-Host "PASS: prune --dry-run"

$ErrorActionPreference = "SilentlyContinue"
python -m dexscreener_screener.cli prune --db $dbPath 2>$null | Out-Null
$ErrorActionPreference = $ErrPrev
if ($LASTEXITCODE -ne 0) { Write-Error "FAIL: prune exit $LASTEXITCODE"; exit 1 }
Write-Host "PASS: prune"

# Verify schema: tables exist (use script to avoid here-string escaping)
python (Join-Path $PSScriptRoot "prune_schema_check.py") $dbPath 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Error "FAIL: after prune, schema missing tables"
    exit 1
}
Write-Host "PASS: schema intact after prune"
Write-Host "PRUNE_CHECK: OK"
