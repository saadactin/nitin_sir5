@echo off
REM Monitor Flask Server - Batch Script
REM This script monitors the Flask application and sends email alerts when it goes down

echo ========================================
echo   Flask Server Monitor
echo ========================================
echo.
echo This script will monitor your Flask server and send email alerts
echo when the server goes down or becomes unresponsive.
echo.
echo Keep this window open to continue monitoring.
echo Press Ctrl+C to stop the monitor.
echo.

REM Check if virtual environment exists
if not exist "myenv1\Scripts\python.exe" (
    echo ERROR: Virtual environment not found!
    echo Please ensure myenv1 directory exists.
    pause
    exit /b 1
)

REM Install requests module if needed
echo Checking dependencies...
myenv1\Scripts\python.exe -c "import requests" 2>nul
if errorlevel 1 (
    echo Installing required 'requests' module...
    myenv1\Scripts\python.exe -m pip install requests -q
    if errorlevel 1 (
        echo ERROR: Failed to install requests module
        pause
        exit /b 1
    )
    echo [OK] Requests module installed
)

echo [OK] All dependencies ready
echo.
echo Starting monitor...
echo.

REM Run the monitor
myenv1\Scripts\python.exe monitor_flask_server.py

pause
