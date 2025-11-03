# Polling & Incremental Sync Testing

This directory contains test infrastructure for verifying that the polling functionality correctly syncs incremental data from APIs to ClickHouse.

## Quick Start

### Option 1: Quick Test (Recommended)

```bash
python quick_test_polling.py
```

This script will:
- ✅ Start the mock API server automatically
- ✅ Run basic sync tests
- ✅ Simulate polling behavior
- ✅ Show results

### Option 2: Full Test Suite

1. **Start Mock API Server:**
   ```bash
   python mock_incremental_api.py
   ```
   Server runs on `http://localhost:5001` and adds 5 records every 5 seconds.

2. **Run Tests:**
   ```bash
   python test_polling_incremental_api.py
   ```

## What Gets Tested

### 1. Mock Incremental API (`mock_incremental_api.py`)
- Simulates an API that adds new records over time
- Adds **5 records every 5 seconds** automatically
- Provides REST endpoints for testing
- Perfect for testing polling functionality

### 2. Test Suite (`test_polling_incremental_api.py`)
Tests cover:
- ✅ Initial sync gets all existing records
- ✅ Polling detects and syncs new records
- ✅ Deduplication prevents duplicate records
- ✅ Continuous polling over time
- ✅ Data integrity verification
- ✅ API endpoint functionality

### 3. Quick Test Script (`quick_test_polling.py`)
- Automated end-to-end test
- Starts mock API automatically
- Runs short polling simulation
- Shows results

## Testing in the Web UI

1. **Start Mock API:**
   ```bash
   python mock_incremental_api.py
   ```

2. **Add API Source in UI:**
   - Source Name: `test_incremental`
   - API URL: `http://localhost:5001/api/data`
   - Auth Type: `None`
   - Data Path: `data` (or leave empty)
   - Target DB: `ClickHouse` → `test_polling_db`
   - ✅ **Enable Polling** (check this!)
   - Poll Interval: `6` seconds
   - Click "Add & Start Sync"

3. **Observe:**
   - System polls API every 6 seconds
   - Mock API adds 5 records every 5 seconds
   - New records should be synced automatically
   - Check ClickHouse for growing record count

## File Descriptions

| File | Description |
|------|-------------|
| `mock_incremental_api.py` | Mock API server that simulates incremental data |
| `test_polling_incremental_api.py` | Comprehensive test suite |
| `quick_test_polling.py` | Quick automated test script |
| `POLLING_TEST_GUIDE.md` | Detailed testing guide |
| `run_polling_tests.bat` | Windows test runner script |
| `run_polling_tests.sh` | Linux/Mac test runner script |

## Expected Behavior

### Record Addition Timeline
```
Time  | Mock API Adds | Total Records | Polling Syncs
------|---------------|--------------|---------------
0s    | 5 records     | 5            | Initial sync (5)
5s    | 5 records     | 10           | Poll #1 (5 new)
10s   | 5 records     | 15           | Poll #2 (5 new)
15s   | 5 records     | 20           | Poll #3 (5 new)
```

### Polling Interval Settings
- **Recommended:** Poll interval should be slightly longer than record addition interval
- **Example:** If API adds records every 5 seconds, use poll interval of 6-10 seconds
- **Why?** Ensures each poll catches a new batch of records

## Verification

### Check Mock API Stats
```bash
curl http://localhost:5001/api/data/stats
```

### Check ClickHouse Records
```sql
USE test_polling_db;
SELECT count() FROM incremental_test_data;
SELECT * FROM incremental_test_data ORDER BY id DESC LIMIT 10;
```

### Check API Records
```bash
curl http://localhost:5001/api/data | python -m json.tool
```

## Troubleshooting

### Port Already in Use
```
Error: Address already in use
```
**Fix:** Change port in `mock_incremental_api.py` (line with `app.run(port=5001)`)

### ClickHouse Connection Failed
**Fix:** Check ClickHouse is running and connection settings

### No Records Synced
**Fix:** 
- Verify polling is enabled in source config
- Check poll interval is correct
- Verify API URL is correct
- Check logs for errors

## Production Use

When ready for production:
1. Replace mock API URL with real API endpoint
2. Configure proper authentication (OAuth, API keys, etc.)
3. Adjust poll interval based on:
   - API rate limits
   - Expected data growth
   - System resources
4. Monitor sync logs and performance

## Next Steps

1. ✅ Test basic polling functionality
2. ✅ Verify incremental sync works
3. 🔄 Test with real Zoho API
4. 🔄 Optimize poll intervals
5. 🔄 Monitor production polling

---

**Note:** The mock API continues running until stopped (Ctrl+C). For production, use real API endpoints.

