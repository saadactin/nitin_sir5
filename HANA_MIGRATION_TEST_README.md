# HANA to ClickHouse Migration Test Guide

## Overview

This guide explains how to test the HANA to ClickHouse migration with real database connections.

## Test Script: `test_hana_migration.py`

A standalone test script that:
- ✅ Tests HANA connection
- ✅ Tests ClickHouse connection  
- ✅ Migrates data directly from HANA to ClickHouse
- ✅ Shows progress and statistics
- ✅ Handles empty tables

## Prerequisites

1. Install required libraries:
```bash
pip install hdbcli clickhouse-connect pandas python-dotenv
```

2. Configure `.env` file (optional - script will prompt if not set):
```env
# HANA Configuration
HANA_HOST=your_hana_host
HANA_PORT=30015
HANA_USERNAME=your_username
HANA_PASSWORD=your_password
HANA_DATABASE=your_database  # Optional

# ClickHouse Configuration
CLICKHOUSE_HOST=your_clickhouse_host
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your_password
CLICKHOUSE_DATABASE=JARVIS_DB
```

## Usage

### Option 1: Run Test Script (Recommended for Testing)

```bash
python test_hana_migration.py
```

The script will:
1. Prompt for HANA credentials (if not in .env)
2. Prompt for ClickHouse credentials (if not in .env)
3. Offer three options:
   - **Option 1**: Migrate all tables from all schemas
   - **Option 2**: Migrate specific schemas (comma-separated)
   - **Option 3**: Test connection only (no migration)

### Option 2: Use from Python Code

```python
from migrate_hana_to_clickhouse import migrate_from_hana_direct

# HANA connection config
hana_config = {
    'host': 'your_hana_host',
    'port': 30015,
    'username': 'your_username',
    'password': 'your_password'
}

# Run migration
result = migrate_from_hana_direct(
    hana_config=hana_config,
    target_database='JARVIS_DB',
    schemas_filter=['SCHEMA1', 'SCHEMA2'],  # Optional: specific schemas
    max_tables=10  # Optional: limit number of tables
)

print(f"Success: {result['success']}")
print(f"Tables processed: {result['processed']}")
print(f"Total rows: {result['total_rows']}")
```

### Option 3: Use from UI (Flask App)

1. Navigate to "Add SAP HANA Source" page
2. Fill in HANA connection details:
   - Host Address
   - Port (default: 30015)
   - Username
   - Password
3. Select ClickHouse as target system
4. Select target database
5. **Leave "Exported Files Directory" empty** to use direct connection
6. Click "Start Migration"

## Features

### ✅ Connection Testing
- Tests HANA connection before migration
- Tests ClickHouse connection before migration
- Shows clear error messages if connections fail

### ✅ Data Migration
- Migrates all tables from selected schemas
- Handles empty tables (creates structure)
- Maps HANA data types to ClickHouse types
- Sanitizes column names
- Handles NULL values properly

### ✅ Progress Tracking
- Shows real-time progress
- Displays table-by-table status
- Provides summary statistics

### ✅ Error Handling
- Continues processing even if individual tables fail
- Logs all errors
- Provides detailed error messages

## Migration Process

1. **Connection**: Connects to both HANA and ClickHouse
2. **Schema Discovery**: Lists all available schemas in HANA
3. **Table Discovery**: Lists all tables in each schema
4. **Schema Mapping**: Extracts column definitions from HANA
5. **Type Mapping**: Maps HANA types to ClickHouse types:
   - INTEGER → Int32
   - BIGINT → Int64
   - DECIMAL → Decimal64
   - VARCHAR → String
   - DATE → Date
   - TIMESTAMP → DateTime
   - etc.
6. **Table Creation**: Creates tables in ClickHouse with mapped schema
7. **Data Migration**: Migrates data row by row
8. **Verification**: Provides summary of migrated data

## Example Output

```
============================================================
HANA to ClickHouse Migration Test
============================================================

📝 Configuration:
------------------------------------------------------------
  HANA: 192.168.1.100:30015 (user: HANA_USER)
  ClickHouse: localhost:9000 (database: JARVIS_DB)
------------------------------------------------------------

✅ Connected to HANA: 192.168.1.100:30015
✅ Connected to ClickHouse: localhost/JARVIS_DB

🚀 Starting HANA to ClickHouse migration
📊 Found 3 schema(s) to process

📦 Processing schema: SCHEMA1
  Found 5 table(s)

  📋 Processing table: SCHEMA1.TABLE1 (1)
  ✅ Created table: JARVIS_DB.HANA_SCHEMA1_TABLE1
  ✅ Migrated 1,234 rows from SCHEMA1.TABLE1 to JARVIS_DB.HANA_SCHEMA1_TABLE1

  📋 Processing table: SCHEMA1.TABLE2 (2)
  ✅ Created table: JARVIS_DB.HANA_SCHEMA1_TABLE2
  ℹ️ Table SCHEMA1.TABLE2 is empty - structure created

============================================================
🎉 Migration Summary:
   Total tables processed: 5
   ✅ Successful: 5
   ❌ Failed: 0
   📊 Total rows migrated: 1,234
============================================================
```

## Troubleshooting

### Connection Issues

**HANA Connection Failed:**
- Check firewall rules
- Verify HANA server is running
- Check port number (default: 30015)
- Verify credentials

**ClickHouse Connection Failed:**
- Check ClickHouse server is running
- Verify port (default: 9000 for native, 8123 for HTTP)
- Check database exists or will be created

### Migration Issues

**No Tables Found:**
- Check schema names (case-sensitive)
- Verify user has SELECT permissions
- Check if schemas are filtered out

**Type Mapping Errors:**
- Some HANA types may map to String by default
- Check logs for specific type mapping issues

**Empty Tables:**
- Empty tables are still created with structure
- This is expected behavior

## Logs

Migration progress is logged to:
- Console output (stdout)
- `hana_migration_test.log` file

## Notes

- Migration runs in background when called from UI
- Large tables may take time to migrate
- Network latency affects migration speed
- All tables are created even if empty
- Column names are sanitized for ClickHouse compatibility

