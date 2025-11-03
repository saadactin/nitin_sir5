# Polling Test Guide

This guide explains how to test the polling functionality with an incremental API that simulates real-world data growth.

## Overview

The polling system continuously checks an API endpoint for new data and syncs it to ClickHouse. This test suite simulates an API that adds 5 new records every 5 seconds, similar to how Zoho CRM or other APIs might add new data over time.

## Components

### 1. Mock Incremental API Server (`mock_incremental_api.py`)

A Flask server that simulates an API with incremental data:
- **Adds 5 records every 5 seconds** automatically
- Provides REST endpoints for testing
- Runs on `http://localhost:5001`

#### Endpoints:
- `GET /api/data` - Get all records
- `GET /api/data/latest?limit=N` - Get latest N records
- `GET /api/data/stats` - Get statistics
- `GET /api/health` - Health check
- `POST /api/data/reset` - Reset all data (for testing)
- `POST /api/data/add` - Manually add records

#### Record Structure:
```json
{
  "id": "REC-000001",
  "name": "John Smith",
  "email": "user1@example.com",
  "company": "TechCorp",
  "age": 35,
  "salary": 75000,
  "department": "Engineering",
  "created_at": "2025-01-15T10:30:00",
  "status": "active",
  "score": 85.5
}
```

### 2. Test Suite (`test_polling_incremental_api.py`)

Comprehensive test cases covering:
1. **Initial Sync** - First sync gets all existing records
2. **Polling Detects New Records** - New records are detected and synced
3. **Deduplication** - Records aren't duplicated
4. **Continuous Polling** - Polling works over extended periods
5. **Data Integrity** - Data matches between API and ClickHouse
6. **API Endpoints** - All endpoints work correctly

## Setup Instructions

### Step 1: Start the Mock API Server

```bash
python mock_incremental_api.py
```

The server will:
- Start on `http://localhost:5001`
- Begin adding 5 records every 5 seconds automatically
- Print log messages showing when records are added

### Step 2: Run the Tests

```bash
# Windows
python test_polling_incremental_api.py

# Or use the helper script
run_polling_tests.bat
```

### Step 3: Verify Results

The tests will:
- Create a test database `test_polling_db` in ClickHouse
- Create a table `incremental_test_data`
- Sync data from the mock API
- Verify data integrity

## Manual Testing

### Test 1: Basic Polling Setup

1. **Start Mock API:**
   ```bash
   python mock_incremental_api.py
   ```

2. **In the Web UI, add a new API source:**
   - Source Name: `test_incremental_api`
   - API Endpoint URL: `http://localhost:5001/api/data`
   - Authentication Type: `None`
   - Data Path: `data` (or leave empty for auto-detect)
   - Target Database Type: `ClickHouse`
   - Target Database Name: `test_polling_db`
   - **Enable Polling**: ✅ Check this
   - **Poll Interval**: `6` seconds (slightly more than API's 5-second interval)

3. **Click "Add & Start Sync"**

4. **Observe the sync:**
   - The system should continuously poll the API
   - Every 6 seconds, it will check for new records
   - New records will be synced to ClickHouse
   - The mock API adds 5 records every 5 seconds

5. **Verify in ClickHouse:**
   ```sql
   USE test_polling_db;
   SELECT count() FROM incremental_test_data;
   SELECT * FROM incremental_test_data ORDER BY id DESC LIMIT 10;
   ```

### Test 2: Verify Incremental Sync

1. **Check initial count:**
   ```bash
   curl http://localhost:5001/api/data/stats
   ```

2. **Wait 10 seconds** (API adds 10 records)

3. **Check API count again:**
   ```bash
   curl http://localhost:5001/api/data/stats
   ```

4. **Verify ClickHouse has new records:**
   ```sql
   SELECT count() FROM test_polling_db.incremental_test_data;
   ```

### Test 3: Data Reset and Re-sync

1. **Reset mock API data:**
   ```bash
   curl -X POST http://localhost:5001/api/data/reset
   ```

2. **Check stats:**
   ```bash
   curl http://localhost:5001/api/data/stats
   # Should show 0 records
   ```

3. **Wait 15 seconds** (API adds 15 records)

4. **Verify new records appear:**
   ```bash
   curl http://localhost:5001/api/data/stats
   # Should show 15 records
   ```

5. **Check ClickHouse syncs new records** (if polling is active)

## Expected Behavior

### Polling Interval
- If poll interval is **6 seconds** and API adds records every **5 seconds**:
  - Poll #1: Syncs records 1-5
  - Poll #2: Syncs records 6-10 (5 new)
  - Poll #3: Syncs records 11-15 (5 new)
  - And so on...

### Deduplication
- The polling system uses the `id` field to track which records have been synced
- Duplicate records (same ID) are not inserted again
- Each record ID is stored in a `seen_ids` set

### Error Handling
- If API is temporarily unavailable, polling retries after the interval
- Network errors are logged and retried
- Invalid responses are logged with detailed error messages

## Troubleshooting

### Mock API Not Starting
```
Error: Address already in use
```
**Solution:** Another process is using port 5001. Either:
- Stop the other process
- Change the port in `mock_incremental_api.py` (line: `app.run(port=NEW_PORT)`)

### ClickHouse Connection Failed
```
ERROR: Cannot connect to ClickHouse database
```
**Solution:** 
- Verify ClickHouse is running
- Check connection settings in `db_utils.py` or config file
- Test connection: `clickhouse-client --query "SELECT 1"`

### No Records Being Synced
**Possible causes:**
1. Polling not enabled in source configuration
2. Poll interval too long (longer than record addition interval)
3. API endpoint URL incorrect
4. Data path incorrect (should be `data` for this mock API)

**Solution:**
- Check source configuration in UI
- Verify API URL: `http://localhost:5001/api/data`
- Check logs for error messages

### Duplicate Records
**Possible causes:**
- Deduplication by ID not working
- ID field missing or incorrect

**Solution:**
- Verify records have an `id` field
- Check polling logs for deduplication messages
- Verify `id_column` parameter is set correctly

## Advanced Testing

### Test with Real Zoho-like Data

1. Modify `mock_incremental_api.py` to generate Zoho-like records:
   ```python
   def generate_record():
       return {
           "id": f"23863200000{record_counter:06d}",
           "Full_Name": "...",
           "Company": "...",
           "Email": "...",
           # etc.
       }
   ```

2. Use Zoho OAuth authentication
3. Test with actual Zoho API endpoints

### Test Polling Performance

1. Increase records per interval in mock API:
   ```python
   RECORDS_PER_INTERVAL = 100  # More records
   ADD_INTERVAL = 5
   ```

2. Monitor:
   - Sync speed (records per second)
   - ClickHouse insertion rate
   - Memory usage
   - Network traffic

### Test Error Scenarios

1. **API Unavailable:**
   - Stop mock API server
   - Verify polling retries correctly
   - Restart API and verify sync resumes

2. **Invalid Response:**
   - Modify mock API to return errors
   - Verify error handling

3. **Rate Limiting:**
   - Simulate 429 responses
   - Verify polling handles it correctly

## Test Results Interpretation

### Successful Test Run
```
[TEST 1] Initial Sync - PASSED
[TEST 2] Polling Detects New Records - PASSED
[TEST 3] Polling Deduplication - PASSED
[TEST 4] Continuous Polling - PASSED
[TEST 5] Data Integrity - PASSED
[TEST 6] API Endpoints - PASSED
```

### Common Issues

**Issue:** Tests fail with connection errors
- **Fix:** Ensure both mock API and ClickHouse are running

**Issue:** No new records detected
- **Fix:** Check poll interval is less than record addition interval

**Issue:** Data mismatch
- **Fix:** Verify data path is correct (`data` for this mock API)

## Production Deployment

When deploying to production:
1. Replace mock API URL with real API endpoint
2. Configure proper authentication (OAuth, API keys, etc.)
3. Adjust poll interval based on:
   - API rate limits
   - Expected data growth rate
   - System resources
4. Monitor:
   - Sync logs
   - ClickHouse table growth
   - API response times
   - Error rates

## Next Steps

1. ✅ Test basic polling functionality
2. ✅ Verify incremental sync works
3. ✅ Test with real Zoho API
4. ✅ Monitor production polling
5. ✅ Optimize poll intervals based on usage

---

For questions or issues, check the main project documentation or logs.

