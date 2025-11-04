# ✅ Complete HANA Incremental Sync Test

## What This Test Does

A comprehensive end-to-end test that verifies:

1. **Data Insertion into HANA**
   - Creates database, schema, and table
   - Inserts 3 initial hospital records

2. **Initial Sync to ClickHouse**
   - Connects HANA to ClickHouse
   - Creates ClickHouse table with proper schema
   - Syncs all 3 records from HANA to ClickHouse

3. **Add More Data to HANA**
   - Inserts 2 additional hospital records (IDs 4 and 5)

4. **Incremental Sync**
   - Runs incremental sync
   - Only syncs the 2 new records (not all 5)
   - Updates sync metadata

5. **Verification**
   - Verifies final count is 5 (3 initial + 2 new)
   - Lists all records in ClickHouse
   - Confirms new records are present

## Quick Start

### Windows:
```bash
run_hana_test.bat
```

### Linux/Mac:
```bash
python test_hana_incremental_sync_complete.py
```

## Prerequisites

### 1. HANA Running
```bash
docker ps --filter "name=hana-express"
# Should show container "Up"

# If not running:
docker-compose up -d

# Wait for ready:
docker logs hana-express
# Look for: "Startup finished!"
```

### 2. .env Configuration
```bash
HANA_HOST=localhost
HANA_PORT=39013
HANA_USERNAME=SYSTEM
HANA_PASSWORD=YourPassword123

CLICKHOUSE_HOST=74.225.251.123
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=root
```

### 3. Python Libraries
```bash
pip install hdbcli clickhouse-driver python-dotenv
```

## Test Flow

```
┌─────────────────────────────────────────────────────────┐
│ 1. Connect to HANA                                       │
│    └─> Create HOSPITAL_DB.HOSPITAL_SCHEMA.HOSPITALS      │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│ 2. Insert Initial Data (3 hospitals)                    │
│    - City General Hospital (NY)                          │
│    - Sunset Medical Center (CA)                          │
│    - Riverside Community Hospital (IL)                   │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│ 3. Initial Sync to ClickHouse                            │
│    └─> Creates table, syncs 3 records                   │
│    └─> Configures incremental sync                       │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│ 4. Add More Data to HANA (2 hospitals)                  │
│    - North Regional Medical (MA)                         │
│    - South Valley Hospital (FL)                          │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│ 5. Run Incremental Sync                                  │
│    └─> Only syncs 2 new records (IDs 4, 5)              │
│    └─> Updates last_sync_timestamp                       │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│ 6. Verify Final Data                                     │
│    └─> ClickHouse should have 5 records total            │
│    └─> Records 4 and 5 should be present                 │
└─────────────────────────────────────────────────────────┘
```

## Expected Results

### Initial State
- **HANA**: 3 hospitals (IDs 1, 2, 3)
- **ClickHouse**: 3 hospitals (IDs 1, 2, 3)

### After Adding More Data
- **HANA**: 5 hospitals (IDs 1, 2, 3, 4, 5)
- **ClickHouse**: Still 3 hospitals (IDs 1, 2, 3)

### After Incremental Sync
- **HANA**: 5 hospitals (IDs 1, 2, 3, 4, 5)
- **ClickHouse**: 5 hospitals (IDs 1, 2, 3, 4, 5) ✅

## Success Criteria

✅ HANA connection successful  
✅ Initial data inserted (3 records)  
✅ Initial sync successful (3 records → ClickHouse)  
✅ More data added (2 records → HANA)  
✅ Incremental sync successful (2 new records → ClickHouse)  
✅ Final verification (5 total records in ClickHouse)  
✅ New records (IDs 4, 5) present in ClickHouse  

## Sample Output

```
================================================================================
STEP 3: Insert Initial Data into HANA
================================================================================
[OK] Inserted 3 initial hospitals
[OK] HANA now has 3 hospitals

================================================================================
STEP 5: Initial Sync from HANA to ClickHouse
================================================================================
[OK] Created ClickHouse table
[OK] Initial sync complete: 3 rows migrated
[OK] ClickHouse has 3 records
[OK] Incremental sync configured

================================================================================
STEP 7: Run Incremental Sync
================================================================================
[OK] Incremental sync complete: 2 new records synced

================================================================================
STEP 8: Verify Final Data in ClickHouse
================================================================================
[INFO] ClickHouse now has 5 total records
[OK] SUCCESS! Expected at least 5 records, got 5
[OK] SUCCESS! Found new records with IDs: [4, 5]

================================================================================
✅ ALL TESTS PASSED!
================================================================================
```

## Troubleshooting

### "HANA connection failed"
- ✅ Check HANA is running: `docker ps --filter "name=hana-express"`
- ✅ Check HANA is ready: `docker logs hana-express` (wait for "Startup finished!")
- ✅ Verify `.env` has correct HANA_PORT (should be 39013)

### "No new records found" in incremental sync
- ✅ Check HANA table has `CREATED_AT` timestamp column (auto-created)
- ✅ Wait a few seconds between adding data and syncing
- ✅ Check `sync_metadata` table in ClickHouse

### "Table already exists"
- ✅ This is OK - test will clear existing data
- ✅ Or manually clean: `TRUNCATE TABLE "HOSPITAL_SCHEMA"."HOSPITALS"`

## Files

- **Test Script**: `test_hana_incremental_sync_complete.py`
- **Quick Start**: `run_hana_test.bat` (Windows)
- **Documentation**: `RUN_HANA_INCREMENTAL_TEST.md`

## All Uses .env Variables

✅ **No hardcoded values**  
✅ **HANA config from .env**  
✅ **ClickHouse config from .env**  
✅ **All credentials from .env**  

---

**Run the test to verify incremental sync works perfectly!** 🎉

