# run-sync.ps1 - run the hybrid sync main directly (unbuffered)
# Usage: .\run-sync.ps1

Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force | Out-Null
if (Test-Path .\.venv\Scripts\Activate.ps1) {
    . .\.venv\Scripts\Activate.ps1
    Write-Host "Activated virtualenv .venv"
} else {
    Write-Host "Virtualenv not found at .\.venv\Scripts\Activate.ps1. Activate your venv manually before running."
}

Write-Host "Running hybrid sync (all servers in config) - output is unbuffered"
python -u -c "from hybrid_sync import main; main()"
