@echo off
REM Script to run polling tests with mock API server (Windows)

echo ==========================================
echo POLLING TEST RUNNER
echo ==========================================

REM Check if mock API is running
curl -s http://localhost:5001/api/health >nul 2>&1
if errorlevel 1 (
    echo [INFO] Mock API server not running. Starting it...
    start /B python mock_incremental_api.py
    timeout /t 3 /nobreak >nul
    echo [OK] Mock API server should be running now
) else (
    echo [OK] Mock API server is already running
)

REM Run tests
echo.
echo Running tests...
echo ==========================================
python test_polling_incremental_api.py

if errorlevel 1 (
    echo.
    echo [ERROR] Tests failed!
    exit /b 1
) else (
    echo.
    echo [OK] All tests completed successfully!
    exit /b 0
)

