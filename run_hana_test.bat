@echo off
echo ========================================
echo HANA Incremental Sync Test
echo ========================================
echo.

echo Checking prerequisites...
echo.

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python is not installed or not in PATH
    pause
    exit /b 1
)

REM Check if .env exists
if not exist .env (
    echo [ERROR] .env file not found!
    echo Please create .env with HANA and ClickHouse settings
    pause
    exit /b 1
)

echo [OK] Python found
echo [OK] .env file exists
echo.

echo Starting test...
echo This will:
echo   1. Insert 3 hospitals into HANA
echo   2. Sync to ClickHouse (initial)
echo   3. Add 2 more hospitals to HANA
echo   4. Run incremental sync
echo   5. Verify all data
echo.
pause

python test_hana_incremental_sync_complete.py

if errorlevel 1 (
    echo.
    echo [ERROR] Test failed!
    pause
    exit /b 1
) else (
    echo.
    echo [SUCCESS] All tests passed!
    pause
)

