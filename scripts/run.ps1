# One-command run: once (single pipeline) or loop (collect + strategy + trigger every N sec).
# Project root same as debug_all.ps1. Lock used only in loop mode.
# Usage: .\scripts\run.ps1 -Mode once|loop [-DbPath path] [-IntervalSec N] [-PairsFile path]
param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("once","loop")]
    [string]$Mode,
    [string]$DbPath = "dexscreener.sqlite",
    [int]$IntervalSec = 60,
    [string]$PairsFile = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$projRoot = Split-Path -Parent $PSScriptRoot
if (-not $projRoot) { $projRoot = (Get-Location).Path }
Set-Location $projRoot

$dbAbs = if ([System.IO.Path]::IsPathRooted($DbPath)) { $DbPath } else { (Join-Path $projRoot $DbPath) }
Write-Host "RUN MODE=$Mode, DB=$dbAbs, interval=$IntervalSec sec"

function _runOneCycle {
    param([string]$DbPath, [string]$PairsFile, [string]$ProjRoot)
    $dbEsc = $DbPath -replace '\\', '/'
    $rootEsc = $ProjRoot -replace '\\', '/'
    $err = 0
    if ($PairsFile -ne "" -and (Test-Path $PairsFile)) {
        $pairEsc = (Resolve-Path $PairsFile).Path -replace '\\', '/'
        python -c "
import sys
sys.path.insert(0, r'$rootEsc')
from dexscreener_screener.cli import main
sys.argv = ['dexscreener_screener', 'collect', '--pairs', r'$pairEsc', '--db', r'$dbEsc']
sys.exit(main())
" 2>&1
        if ($LASTEXITCODE -ne 0) { $err = $LASTEXITCODE }
    }
    python -c "
import sys
sys.path.insert(0, r'$rootEsc')
from dexscreener_screener.cli import main
sys.argv = ['dexscreener_screener', 'strategy', '--once', '--db', r'$dbEsc']
sys.exit(main())
" 2>&1
    if ($LASTEXITCODE -ne 0) { $err = $LASTEXITCODE }
    python -c "
import sys
sys.path.insert(0, r'$rootEsc')
from dexscreener_screener.cli import main
sys.argv = ['dexscreener_screener', 'trigger', '--once', '--db', r'$dbEsc']
sys.exit(main())
" 2>&1
    if ($LASTEXITCODE -ne 0) { $err = $LASTEXITCODE }
    return $err
}

if ($Mode -eq "loop") {
    $pidToLock = $PID
    python -c "
import sys
sys.path.insert(0, r'$($projRoot -replace '\\', '/')')
from dexscreener_screener.core.lock import try_acquire_db_lock
ok = try_acquire_db_lock(r'$($dbAbs -replace '\\', '/')', pid=$pidToLock)
sys.exit(0 if ok else 1)
" 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Another process holds the lock for this DB. Refusing to start. (Use once mode or another DbPath.)"
        exit 2
    }
    try {
        while ($true) {
            $ec = _runOneCycle -DbPath $dbAbs -PairsFile $PairsFile -ProjRoot $projRoot
            Start-Sleep -Seconds $IntervalSec
        }
    } finally {
        python -c "
import sys
sys.path.insert(0, r'$($projRoot -replace '\\', '/')')
from dexscreener_screener.core.lock import release_db_lock
release_db_lock(r'$($dbAbs -replace '\\', '/')', pid=$pidToLock)
" 2>&1
    }
} else {
    $ec = _runOneCycle -DbPath $dbAbs -PairsFile $PairsFile -ProjRoot $projRoot
    exit $ec
}
