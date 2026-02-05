# Architectural E2E self-check: run all steps, report PASS/FAIL. PowerShell only.
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$projRoot = Split-Path -Parent $PSScriptRoot
if (-not $projRoot) { $projRoot = (Get-Location).Path }
Set-Location $projRoot

$steps = @(
    @{ Name = "1. arch_check";     Cmd = "python scripts/arch_check.py";                    Optional = $false },
    @{ Name = "2. cli_check";     Cmd = "powershell -ExecutionPolicy Bypass -File scripts/cli_check.ps1"; Optional = $false },
    @{ Name = "3. db_check";      Cmd = "python scripts/db_check.py";                      Optional = $false },
    @{ Name = "4. smoke_test";    Cmd = "python scripts/smoke_test.py";                    Optional = $false },
    @{ Name = "5. e2e_live";      Cmd = "powershell -ExecutionPolicy Bypass -File scripts/e2e_live.ps1"; Optional = $true },
    @{ Name = "6. link_check";    Cmd = "python scripts/link_check.py";                   Optional = $false },
    @{ Name = "7. prune_check";   Cmd = "powershell -ExecutionPolicy Bypass -File scripts/prune_check.ps1"; Optional = $true }
)

foreach ($step in $steps) {
    Write-Host ""
    Write-Host ("=== " + $step.Name + " ===")
    try {
        $ErrorActionPreferencePrev = $ErrorActionPreference
        $ErrorActionPreference = "SilentlyContinue"
        Invoke-Expression $step.Cmd 2>&1 | ForEach-Object { Write-Host $_ }
        $ErrorActionPreference = $ErrorActionPreferencePrev
        if ($LASTEXITCODE -ne 0) {
            Write-Host ("FAIL: " + $step.Name + " (exit " + $LASTEXITCODE + ")")
            if (-not $step.Optional) {
                Write-Error ("Stopping at failed step: " + $step.Name)
                exit 1
            }
        } else {
            Write-Host ("PASS: " + $step.Name)
        }
    } catch {
        Write-Host ("FAIL: " + $step.Name + " - " + $_.Exception.Message)
        if (-not $step.Optional) {
            Write-Error ("Stopping at failed step: " + $step.Name)
            exit 1
        }
    }
}
Write-Host ""
Write-Host "=== debug_all finished ==="
