# HANA to ClickHouse Migration - Testing Guide

## ✅ Implementation Complete

All requested features have been implemented:

1. ✅ **Incremental Sync** - "Sync Server" button on home page performs incremental sync
2. ✅ **Home Page Cards** - HANA sources display as cards like API and SQL sources
3. ✅ **Test Cases** - Comprehensive test suite created
4. ✅ **Data Validation** - Script to verify correct data migration

## 📋 Test Cases

### Running the Test Suite

```bash
# Run all tests
python test_hana_migration.py

# Run with verbose output
python test_hana_migration.py -v
```

### Test Coverage

The test suite includes:

1. **Connection Tests**
   - HANA connection establishment
   - ClickHouse connection establishment

2. **Schema Extraction Tests**
   - Schema listing (excluding system schemas)
   - Table extraction from schemas
   - Column schema extraction

3. **Type Mapping Tests**
   - HANA → ClickHouse data type conversion
   - All common types covered

4. **Table Naming Tests**
   - `schema_tablename` convention verification

5. **Migration Tests**
   - Table creation in ClickHouse
   - Data migration with batching
   - Metadata column addition
   - Data integrity verification

6. **Incremental Sync Tests**
   - Sync metadata setup
   - Incremental sync functionality

## 🔍 Data Validation

### Manual Validation Script

```bash
# Validate a specific table migration
python validate_hana_migration.py SCHEMA1 CUSTOMERS

# Interactive mode
python validate_hana_migration.py
```

### What It Validates

1. **Row Count Match**
   - Compares HANA vs ClickHouse row counts
   - Reports any differences

2. **Sample Data Comparison**
   - Compares first 5 rows
   - Verifies value correctness
   - Handles NULL values

3. **Metadata Columns**
   - Checks for `_source_schema`
   - Checks for `_source_table`
   - Checks for `_sync_timestamp`
   - Verifies metadata values

4. **NULL Handling**
   - Verifies NULL values are preserved
   - Checks nullable column handling

## 🏠 Home Page Integration

### Card Display

HANA sources now appear as cards on the home page with:
- Source name
- Source type: "Sap Hana"
- Status indicator (Online/Offline)
- Server address
- Action buttons:
  - **"View Tables"** - Opens table browser (HANA-specific)
  - **"Sync Server"** - Runs incremental sync
  - **"Edit"** - Edit source configuration
  - **"Delete"** - Remove source

### Status Checking

HANA sources are checked for connectivity:
- Green status: Connected successfully
- Red status: Connection failed
- Gray status: hdbcli library not installed

## 🔄 Incremental Sync Behavior

### When "Sync Server" is Clicked

1. **First Time** (No sync_metadata):
   - Performs full sync of all tables in first 10 schemas
   - Sets up incremental sync metadata for future runs

2. **Subsequent Runs**:
   - Reads sync_metadata table for configured tables
   - Performs incremental sync based on timestamps
   - Falls back to full sync if incremental fails (no timestamp column)

### Sync Process

```
1. Connect to HANA and ClickHouse
2. Query sync_metadata for enabled tables
3. For each table:
   a. Try incremental sync (timestamp-based)
   b. If fails → perform full sync
   c. Update sync_metadata
4. Log results
```

## 🧪 Testing Checklist

### Before Testing

- [ ] VPN connected (if required)
- [ ] HANA server accessible
- [ ] ClickHouse running and accessible
- [ ] hdbcli library installed
- [ ] ClickHouse driver installed

### Test Steps

1. **Add HANA Source**
   - [ ] Navigate to "Add Source" → "SAP HANA"
   - [ ] Fill in connection details
   - [ ] Click "Test Connection" (should succeed)
   - [ ] Click "Add SAP HANA Source"
   - [ ] Verify card appears on home page

2. **Initial Sync**
   - [ ] Click "View Tables" on HANA source card
   - [ ] Select 1-2 tables to sync
   - [ ] Click "Sync Selected Tables"
   - [ ] Verify tables appear in ClickHouse
   - [ ] Run validation script: `python validate_hana_migration.py SCHEMA TABLE`

3. **Incremental Sync**
   - [ ] Click "Sync Server" on home page
   - [ ] Check logs for "incremental sync" messages
   - [ ] Verify only new/changed rows are synced
   - [ ] Verify row counts in ClickHouse match HANA

4. **Data Validation**
   - [ ] Run: `python validate_hana_migration.py SCHEMA TABLE`
   - [ ] Verify all checks pass:
     - [ ] Row counts match
     - [ ] Sample data matches
     - [ ] Metadata columns present
     - [ ] Metadata values correct

5. **Test Cases**
   - [ ] Run: `python test_hana_migration.py`
   - [ ] Verify all tests pass (some may skip if HANA not accessible)

## 📊 Expected Results

### Successful Migration

```
VALIDATION SUMMARY
======================================================================
✓ Row counts match
✓ Sample data verified
✓ Metadata columns present
✓ Migration validation passed!
```

### Test Suite Output

```
======================================================================
HANA TO CLICKHOUSE MIGRATION - COMPREHENSIVE TEST SUITE
======================================================================

[TEST 1] Testing HANA connection...
✓ HANA connection successful

[TEST 2] Testing ClickHouse connection...
✓ ClickHouse connection successful

[TEST 3] Testing HANA schema extraction...
✓ Found 15 user schemas

...

TEST SUMMARY
======================================================================
Tests run: 12
Successes: 12
Failures: 0
Errors: 0
Skipped: 0
```

## 🔧 Troubleshooting

### Connection Failures

**Problem**: HANA connection fails
- Check VPN connection
- Verify host and port
- Check firewall rules
- Verify credentials

### Missing hdbcli

**Problem**: "hdbcli not available"
- Install: `pip install hdbcli`
- Or on Windows: Install SAP HANA Client

### Row Count Mismatch

**Problem**: ClickHouse has fewer rows than HANA
- Check migration logs for errors
- Verify table was fully synced
- Check for data type conversion issues

### Incremental Sync Not Working

**Problem**: Always does full sync
- Check if sync_metadata table exists
- Verify table has timestamp column
- Check logs for error messages

## ✅ Verification Commands

```bash
# Check HANA connection
python -c "import hdbcli.dbapi; print('OK')"

# Check ClickHouse connection
python -c "from clickhouse_driver import Client; c=Client(host='localhost'); print('OK')"

# Validate migration
python validate_hana_migration.py SCHEMA TABLE

# Run tests
python test_hana_migration.py
```

## 📝 Notes

- Incremental sync requires timestamp columns in source tables
- First sync always does full migration
- Metadata is stored in `sync_metadata` table in ClickHouse
- Table names follow `SCHEMA_TABLENAME` convention
- All tests can run even if HANA is not accessible (they'll skip)

