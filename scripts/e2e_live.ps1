# Live E2E: one collect cycle + strategy + trigger, then SQL summary. PowerShell only.
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$projRoot = Split-Path -Parent $PSScriptRoot
if (-not $projRoot) { $projRoot = (Get-Location).Path }
Set-Location $projRoot

$dbPath = Join-Path $projRoot "debug_live.sqlite"
$pairsFile = Join-Path $projRoot "pairs_one.txt"

if (Test-Path $dbPath) { Remove-Item $dbPath -Force }

$ErrPrev = $ErrorActionPreference
$ErrorActionPreference = "SilentlyContinue"

# One cycle: collect (if pairs file exists) -> strategy -> trigger
if (Test-Path $pairsFile) {
    python -m dexscreener_screener.cli collect --pairs $pairsFile --db $dbPath 2>$null | Out-Null
    if ($LASTEXITCODE -ne 0) { Write-Warning "collect exit $LASTEXITCODE (non-fatal)" }
} else {
    Write-Host "E2E_LIVE: pairs file not found, creating empty DB (schema only)"
    $initPy = "import sys; from pathlib import Path; sys.path.insert(0, r'$($projRoot -replace '\\','/')'); from dexscreener_screener.storage import Database; db = Database(r'$($dbPath -replace '\\','/')'); db.close()"
    python -c $initPy
    if ($LASTEXITCODE -ne 0) { Write-Error "FAIL: empty DB init"; exit 1 }
}

python -m dexscreener_screener.cli strategy --once --db $dbPath 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) { Write-Warning "strategy exit $LASTEXITCODE (non-fatal)" }

python -m dexscreener_screener.cli trigger --once --db $dbPath 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) { Write-Error "FAIL: trigger exit $LASTEXITCODE"; exit 1 }

python (Join-Path $PSScriptRoot "e2e_live_summary.py") $dbPath 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) { Write-Error "FAIL: e2e_live_summary"; exit 1 }

$ErrorActionPreference = $ErrPrev
Write-Host "E2E_LIVE: OK"
