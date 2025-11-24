@echo off
REM Run HANA Migration to ClickHouse
REM This will migrate all data from exported HANA files to JARVIS_DB
REM Uses the new unified migration system

echo ========================================
echo HANA Export to ClickHouse Import
echo ========================================
echo.
echo This will import ALL data from:
echo   Z_TEST_PRODUCTION_20250925
echo to ClickHouse database:
echo   JARVIS_DB
echo.
echo The process will continue until ALL data is copied.
echo.
pause

python migrate_hana_to_clickhouse.py

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ========================================
    echo IMPORT COMPLETED SUCCESSFULLY!
    echo ========================================
) else (
    echo.
    echo ========================================
    echo IMPORT COMPLETED WITH ERRORS
    echo Check hana_import.log for details
    echo ========================================
)

pause


