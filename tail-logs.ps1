<# tail-logs.ps1 - Tail a log file (PowerShell)
Usage:
  .\tail-logs.ps1                 # tails ./app.log by default
  .\tail-logs.ps1 -File .\hybrid_sync.log
#>
param(
    [string]$File = ".\app.log"
)

if (-not (Test-Path $File)) {
    Write-Host "File not found: $File"
    exit 1
}

Write-Host "Tailing $File (Ctrl+C to stop)..."
Get-Content $File -Wait -Tail 200
