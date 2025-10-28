# Production Server Launcher for ACTIN Data Sync
# This script starts the application using Waitress production server

Write-Host "="*70 -ForegroundColor Cyan
Write-Host "🚀 ACTIN DATA SYNC - PRODUCTION SERVER LAUNCHER" -ForegroundColor Green
Write-Host "="*70 -ForegroundColor Cyan
Write-Host ""

# Check if Waitress is installed
Write-Host "🔍 Checking dependencies..." -ForegroundColor Yellow
$waitressInstalled = pip list 2>$null | Select-String "waitress"

if (-not $waitressInstalled) {
    Write-Host "❌ Waitress not installed!" -ForegroundColor Red
    Write-Host "📦 Installing Waitress..." -ForegroundColor Yellow
    pip install waitress==3.0.1
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ Failed to install Waitress" -ForegroundColor Red
        Write-Host "💡 Try manually: pip install waitress" -ForegroundColor Yellow
        exit 1
    }
    Write-Host "✅ Waitress installed successfully" -ForegroundColor Green
} else {
    Write-Host "✅ Waitress is already installed" -ForegroundColor Green
}

Write-Host ""

# Check if .env file exists
if (-not (Test-Path ".env")) {
    Write-Host "⚠️  .env file not found!" -ForegroundColor Yellow
    Write-Host "💡 Create .env file with database configuration" -ForegroundColor Yellow
    Write-Host ""
}

# Set production environment variables
Write-Host "⚙️  Configuring production environment..." -ForegroundColor Yellow
$env:FLASK_DEBUG = "0"
$env:APP_HOST = "0.0.0.0"
$env:APP_PORT = "5001"
$env:WAITRESS_THREADS = "4"
$env:WAITRESS_CHANNEL_TIMEOUT = "60"

Write-Host "✅ Environment configured" -ForegroundColor Green
Write-Host ""

# Display configuration
Write-Host "📋 CONFIGURATION:" -ForegroundColor Cyan
Write-Host "   🌐 Host: $env:APP_HOST (accessible from network)"
Write-Host "   🔌 Port: $env:APP_PORT"
Write-Host "   🧵 Threads: $env:WAITRESS_THREADS"
Write-Host "   🔒 Debug Mode: OFF (Production)"
Write-Host ""

# Confirm before starting
Write-Host "⚡ Ready to start production server!" -ForegroundColor Green
Write-Host "Press Ctrl+C to stop the server at any time" -ForegroundColor Yellow
Write-Host ""
Start-Sleep -Seconds 2

# Start the production server
Write-Host "🚀 Starting server..." -ForegroundColor Green
Write-Host ""

try {
    python run_production.py
} catch {
    Write-Host ""
    Write-Host "❌ Server stopped with error: $_" -ForegroundColor Red
    exit 1
}
