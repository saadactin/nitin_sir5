@echo off
REM Test Runner Script for Windows
REM Run all tests to verify project health

echo ==========================================
echo Project Health Check Test Suite
echo ==========================================
echo.

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo Error: Python not found
    exit /b 1
)

REM Check if required packages are installed
echo Checking dependencies...
python -c "import requests" 2>nul
if errorlevel 1 (
    echo Installing required packages...
    pip install requests
)

REM Run quick check first
echo.
echo Running Quick Health Check...
echo ----------------------------------------
python test_quick_check.py

REM Run full test suite
echo.
echo Running Full Test Suite...
echo ----------------------------------------
python test_project_health.py

echo.
echo ==========================================
echo Tests Complete
echo ==========================================
pause

