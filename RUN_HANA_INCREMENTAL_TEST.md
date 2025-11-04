# Run HANA Incremental Sync Test

## Test Overview

This test verifies the complete HANA to ClickHouse incremental sync workflow:

1. ✅ **Insert initial data** into HANA (3 hospitals)
2. ✅ **Initial sync** from HANA to ClickHouse
3. ✅ **Add more data** to HANA (2 more hospitals)
4. ✅ **Incremental sync** to verify new data is transferred
5. ✅ **Verify** all data in ClickHouse

## Prerequisites

### 1. Environment Variables

Make sure `.env` has:
```bash
# HANA Configuration
HANA_HOST=localhost
HANA_PORT=39013
HANA_USERNAME=SYSTEM
HANA_PASSWORD=YourPassword123

# ClickHouse Configuration
CLICKHOUSE_HOST=74.225.251.123
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=root
```

### 2. HANA Must Be Running

Check HANA container:
```bash
docker ps --filter "name=hana-express"
```

If not running:
```bash
docker-compose up -d
```

Wait for HANA to be ready:
```bash
docker logs hana-express
```
Look for: **"Startup finished!"**

### 3. Required Python Libraries

```bash
pip install hdbcli clickhouse-driver python-dotenv
```

## Run the Test

### Quick Run
```bash
python test_hana_incremental_sync_complete.py
```

### Expected Output

```
================================================================================
COMPLETE TEST: HANA to ClickHouse Incremental Sync
================================================================================

[OK] HANA Config: localhost:39013
[OK] ClickHouse Config: 74.225.251.123:9000

================================================================================
STEP 1: Connect to HANA
================================================================================
[OK] Connected to HANA

================================================================================
STEP 2: Ensure HANA Database/Schema/Table Exist
================================================================================
[OK] Created database: HOSPITAL_DB
[OK] Created schema: HOSPITAL_SCHEMA
[OK] Table HOSPITALS ready
[OK] Cleared existing data

================================================================================
STEP 3: Insert Initial Data into HANA
================================================================================
[OK] Inserted 3 initial hospitals
[OK] HANA now has 3 hospitals

================================================================================
STEP 4: Connect to ClickHouse
================================================================================
[OK] Connected to ClickHouse

================================================================================
STEP 5: Initial Sync from HANA to ClickHouse
================================================================================
[OK] Created ClickHouse table
[OK] Initial sync complete: 3 rows migrated
[OK] ClickHouse has 3 records
[OK] Incremental sync configured

================================================================================
STEP 6: Add More Data to HANA (Incremental Test)
================================================================================
[OK] Added 2 new hospitals
[OK] HANA now has 5 hospitals (was 3, added 2)

================================================================================
STEP 7: Run Incremental Sync
================================================================================
[OK] Incremental sync complete: 2 new records synced

================================================================================
STEP 8: Verify Final Data in ClickHouse
================================================================================
[OK] SUCCESS! Expected at least 5 records, got 5
[OK] SUCCESS! Found new records with IDs: [4, 5]

================================================================================
✅ ALL TESTS PASSED!
================================================================================
```

## What Gets Tested

### ✅ Initial Sync
- Creates HANA database `HOSPITAL_DB`
- Creates schema `HOSPITAL_SCHEMA`
- Creates table `HOSPITALS`
- Inserts 3 initial hospital records
- Syncs all 3 to ClickHouse
- Creates ClickHouse table with proper schema

### ✅ Incremental Sync
- Adds 2 new hospital records to HANA (IDs 4 and 5)
- Runs incremental sync
- Verifies only new records are synced (2 records)
- Verifies final count is 5 (3 initial + 2 new)

### ✅ Data Verification
- Checks total record count in ClickHouse
- Lists all records
- Verifies new records (IDs 4, 5) are present

## Test Data

### Initial Hospitals (3):
1. City General Hospital - New York, NY - 500 beds
2. Sunset Medical Center - Los Angeles, CA - 350 beds
3. Riverside Community Hospital - Chicago, IL - 275 beds

### New Hospitals Added (2):
4. North Regional Medical - Boston, MA - 450 beds
5. South Valley Hospital - Miami, FL - 300 beds

## Troubleshooting

### Error: "HANA connection failed"
- Check HANA is running: `docker ps --filter "name=hana-express"`
- Check HANA is ready: `docker logs hana-express`
- Verify `.env` has correct HANA settings

### Error: "ClickHouse connection failed"
- Verify `.env` has correct ClickHouse settings
- Check ClickHouse is accessible

### Error: "No new records found" in incremental sync
- HANA table needs a timestamp column (`CREATED_AT`)
- Or incremental sync might use ID-based detection
- Check `sync_metadata` table in ClickHouse

### Error: "Table already exists"
- This is OK - test will clear and recreate
- Or manually drop: `DROP TABLE IF EXISTS HOSPITAL_SCHEMA_HOSPITALS`

## Files Created/Used

- **HANA**: `HOSPITAL_DB.HOSPITAL_SCHEMA.HOSPITALS`
- **ClickHouse**: `test1.HOSPITAL_SCHEMA_HOSPITALS` (or your configured database)

## Cleanup (Optional)

After test, you can clean up:

```sql
-- In HANA
DROP TABLE "HOSPITAL_SCHEMA"."HOSPITALS";
DROP SCHEMA "HOSPITAL_SCHEMA";
DROP DATABASE "HOSPITAL_DB";

-- In ClickHouse
DROP TABLE test1.HOSPITAL_SCHEMA_HOSPITALS;
DROP TABLE test1.sync_metadata;  -- If you want to remove sync metadata too
```

---

**Run the test to verify incremental sync works end-to-end!** ✅

