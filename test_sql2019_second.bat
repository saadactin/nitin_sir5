@echo off
echo.
echo ========================================================================
echo TESTING CONNECTION TO SQL2019_SECOND NAMED INSTANCE
echo ========================================================================
echo.

echo This script will run several tests to diagnose connection issues with
echo the SQL2019_Second named instance.

echo.
echo [1] Testing SQL Browser service...
sc query SQLBrowser > NUL
if %ERRORLEVEL% EQU 0 (
    echo SQL Browser service exists.
    
    sc query SQLBrowser | findstr "RUNNING" > NUL
    if %ERRORLEVEL% EQU 0 (
        echo ✓ SQL Browser service is RUNNING
    ) else (
        echo X SQL Browser service is NOT running
        echo.
        echo Would you like to start the SQL Browser service? (Y/N)
        set /p startService=
        if /i "%startService%"=="Y" (
            echo Starting SQL Browser service...
            net start SQLBrowser
        )
    )
) else (
    echo X SQL Browser service not found!
)

echo.
echo [2] Testing network connectivity to SQL Browser...
echo Checking if UDP port 1434 is accessible...
netstat -an | findstr "1434" | findstr "UDP"
if %ERRORLEVEL% EQU 0 (
    echo ✓ UDP port 1434 is active
) else (
    echo ? UDP port 1434 not found in active connections
    echo   This might be normal if no connections are active
)

echo.
echo [3] Checking firewall rules...
netsh advfirewall firewall show rule name="SQL Server Browser Service" > NUL 2>&1
if %ERRORLEVEL% EQU 0 (
    echo ✓ SQL Server Browser firewall rule exists
) else (
    echo X No specific SQL Browser firewall rule found
    echo   You might need to add a firewall rule for UDP port 1434
)

echo.
echo [4] Testing connection to SQL2019_Second using Python...
echo Running Python connection test script...
python test_sql2019_second.py

echo.
echo ========================================================================
echo SUMMARY AND RECOMMENDATIONS
echo ========================================================================
echo.
echo If you're still having issues connecting to SQL2019_Second:
echo.
echo 1. Make sure SQL Server is running
echo 2. Ensure SQL Browser service is running (net start SQLBrowser)
echo 3. Check that UDP port 1434 is allowed through firewall
echo 4. For direct connection, use:
echo    Server: localhost,14344 (with explicit port)
echo    OR
echo    Server: localhost\SQL2019_Second (with SQL Browser service running)
echo.
echo In the application, try adding the server as "localhost,14344" if
echo "localhost\SQL2019_Second" doesn't work.
echo.
echo ========================================================================

pause