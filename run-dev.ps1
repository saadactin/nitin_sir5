# run-dev.ps1 - activate venv and start Flask app unbuffered (development)
# Usage: .\run-dev.ps1

# Allow script execution for this process
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force | Out-Null

if (Test-Path .\.venv\Scripts\Activate.ps1) {
    . .\.venv\Scripts\Activate.ps1
    Write-Host "Activated virtualenv .venv"
} else {
    Write-Host "Virtualenv not found at .\.venv\Scripts\Activate.ps1. Activate your venv manually before running."
}

# Avoid Werkzeug reloader confusing output; when running from shell this helps
$env:WERKZEUG_RUN_MAIN = 'true'

Write-Host "Starting Flask app (unbuffered + debug)..."
# Run unbuffered so prints show immediately
python -u .\app.py
