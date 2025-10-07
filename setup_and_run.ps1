<#
setup_and_run.ps1

Creates a virtual environment (if missing), installs requirements, starts the Flask app in a new PowerShell window,
and opens the default browser to http://localhost:5000/login.

Double-click the accompanying start_project.bat to run this script from Explorer.
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Write-Host "Starting project launcher..." -ForegroundColor Cyan

$root = Split-Path -Parent $MyInvocation.MyCommand.Definition
Push-Location $root

$venvPath = Join-Path $root 'myenv'
$pythonExe = Join-Path $venvPath 'Scripts\python.exe'

function Exec([string]$cmd) {
    Write-Host $cmd
    & cmd /c $cmd
}

try {
    if (-Not (Test-Path $venvPath)) {
        Write-Host "Virtualenv not found. Creating virtual environment at '$venvPath'..." -ForegroundColor Yellow
        # Create venv
        & python -m venv "$venvPath"
        if (-Not (Test-Path $pythonExe)) {
            Write-Error "Python virtual environment creation failed. Ensure 'python' is on PATH and venv module is available.";
            exit 1
        }
    }

    Write-Host "Using Python from: $pythonExe" -ForegroundColor Green

    # Upgrade pip and install requirements
    Write-Host "Upgrading pip and installing requirements (if needed)..." -ForegroundColor Yellow
    & "$pythonExe" -m pip install --upgrade pip setuptools wheel
    & "$pythonExe" -m pip install -r "${root}\requirements.txt"

    # Start the Flask app in a new PowerShell window so this launcher can finish and the server keeps running.
    Write-Host "Starting Flask app in a new window..." -ForegroundColor Yellow

    $startInfo = @{
        FilePath = 'powershell'
        ArgumentList = @('-NoProfile','-ExecutionPolicy','Bypass','-NoExit','-Command', "& { Set-Location -LiteralPath '$root'; & '$pythonExe' 'app.py' }")
        WorkingDirectory = $root
    }
    Start-Process @startInfo | Out-Null

    Start-Sleep -Seconds 2
    Write-Host "Opening browser to http://localhost:5000/login" -ForegroundColor Cyan
    Start-Process 'http://localhost:5000/login'

    Write-Host "Launcher finished. Server is running in a separate window. Close that window to stop the server." -ForegroundColor Green
} catch {
    Write-Host "Error: $_" -ForegroundColor Red
    Write-Host "Press Enter to exit..."
    [void][System.Console]::ReadLine()
    exit 1
} finally {
    Pop-Location
}
