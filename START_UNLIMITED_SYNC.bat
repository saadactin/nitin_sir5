@echo off
REM Quick start script for unlimited API sync testing

echo ========================================================================
echo    UNLIMITED API SYNC - QUICK START
echo ========================================================================
echo.

echo Step 1: Checking if old tables need cleanup...
python drop_all_api_tables.py
echo.

echo ========================================================================
echo Step 2: Instructions to start servers
echo ========================================================================
echo.
echo You need to open 2 separate PowerShell terminals:
echo.
echo TERMINAL 1 - Mock SSE Server:
echo    cd c:\Users\SaadSayyed\Desktop\test2
echo    python mock_sse_server.py
echo.
echo TERMINAL 2 - Flask App:
echo    cd c:\Users\SaadSayyed\Desktop\test2\nitin_sir5
echo    python app.py
echo.
echo ========================================================================
echo Step 3: After both servers are running
echo ========================================================================
echo.
echo 1. Open browser: http://localhost:5001/play
echo 2. Click 'Sync Server' button on your API source
echo 3. Watch the terminal logs for:
echo    - "Connected to SSE stream"
echo    - "Initial data batch: 10 records"
echo    - "New data record: ID 29, 30, 31..."
echo.
echo ========================================================================
echo Step 4: Verify in ClickHouse
echo ========================================================================
echo.
echo After 30 seconds, run this query:
echo    SELECT count(*), max(id) FROM test7.crm;
echo.
echo You should see:
echo    - Count growing (10, 11, 12, 13...)
echo    - Max ID increasing (28, 29, 30, 31...)
echo.
echo ========================================================================
echo Expected Results
echo ========================================================================
echo.
echo ✅ 10+ rows in ClickHouse (not just 1!)
echo ✅ Columns: id, rollno, timestamp (NOT current_data_*)
echo ✅ New records every 5 seconds
echo ✅ Sync never stops (runs forever!)
echo.
echo ========================================================================
echo Press any key to exit...
pause >nul
