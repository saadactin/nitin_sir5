@echo off
echo ========================================
echo Starting HANA Docker Container
echo ========================================
echo.

REM Check if Docker is running
docker ps >nul 2>&1
if errorlevel 1 (
    echo ERROR: Docker is not running!
    echo Please start Docker Desktop and try again.
    pause
    exit /b 1
)

echo [1/3] Starting HANA container...
docker-compose up -d

if errorlevel 1 (
    echo ERROR: Failed to start HANA container
    pause
    exit /b 1
)

echo.
echo [2/3] Waiting for HANA to initialize...
echo This may take 5-10 minutes on first run...
echo.

REM Wait a bit for container to start
timeout /t 5 /nobreak >nul

REM Check logs
echo Checking HANA status...
docker logs hana-express --tail 20

echo.
echo ========================================
echo HANA Container Started!
echo ========================================
echo.
echo Next steps:
echo 1. Wait for HANA to finish initializing (check logs: docker logs -f hana-express)
echo 2. Run: python setup_hana_sample_db.py
echo.
echo To view logs: docker logs -f hana-express
echo To stop: docker-compose down
echo.
pause

