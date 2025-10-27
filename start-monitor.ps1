# Monitor Flask Server
# This script monitors the Flask application and sends email alerts when it goes down
# Run this in a separate PowerShell window

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Flask Server Monitor" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "This script will monitor your Flask server and send email alerts" -ForegroundColor Yellow
Write-Host "when the server goes down or becomes unresponsive." -ForegroundColor Yellow
Write-Host ""
Write-Host "Keep this window open to continue monitoring." -ForegroundColor Green
Write-Host "Press Ctrl+C to stop the monitor." -ForegroundColor Green
Write-Host ""

# Check if requests module is installed
Write-Host "Checking dependencies..." -ForegroundColor Cyan
$pythonPath = ".\myenv1\Scripts\python.exe"

& $pythonPath -c "import requests" 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Installing required 'requests' module..." -ForegroundColor Yellow
    & $pythonPath -m pip install requests -q
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: Failed to install requests module" -ForegroundColor Red
        exit 1
    }
    Write-Host "✓ Requests module installed" -ForegroundColor Green
}

Write-Host "✓ All dependencies ready" -ForegroundColor Green
Write-Host ""
Write-Host "Starting monitor..." -ForegroundColor Cyan
Write-Host ""

# Run the monitor
& $pythonPath monitor_flask_server.py
