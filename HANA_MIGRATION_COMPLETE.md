# SAP HANA to ClickHouse Migration - Complete Implementation

## ✅ Implementation Summary

The complete SAP HANA to ClickHouse migration system has been implemented according to the specifications in `hanatoclickhouse.md`.

## 📁 Files Created/Modified

### New Files:
1. **`hana_sync.py`** - Core HANA synchronization module
   - `HanaToClickHouseSync` class with all functionality
   - HANA connection management
   - Schema extraction from HANA
   - Data type mapping (HANA → ClickHouse)
   - Table creation in ClickHouse with `schema_tablename` format
   - Batch data migration
   - Incremental sync support

2. **`templates/view_hana_tables.html`** - Interactive table browser and sync interface
   - Schema navigation
   - Table selection with checkboxes
   - Display of ClickHouse target table names
   - Sync controls

### Modified Files:
1. **`app.py`** - Added HANA routes:
   - `/view-hana-tables/<source_id>` - Renders table browser page
   - `/api/view-hana-tables/<source_id>` - JSON API for fetching schemas/tables
   - `/sync-hana-source/<source_id>` - Triggers HANA to ClickHouse sync

2. **`templates/sync_servers.html`** - Updated to show "View Tables" button for HANA sources

3. **`app.py`** - Existing `/add-source/hana` route already exists (verified working)

## 🎯 Key Features Implemented

### 1. Table Naming Convention
- **Format**: `schema_tablename` (e.g., `SCHEMA1_CUSTOMERS`)
- All tables stored in ClickHouse database specified in target_database (default: `hana_migrated`)
- No naming conflicts between different HANA schemas

### 2. Data Type Mapping
| HANA Type | ClickHouse Type |
|-----------|----------------|
| TINYINT | Int8 |
| SMALLINT | Int16 |
| INTEGER/INT | Int32 |
| BIGINT | Int64 |
| DECIMAL(p,s) | Decimal64(s) |
| REAL | Float32 |
| DOUBLE | Float64 |
| VARCHAR | String |
| NVARCHAR | String |
| CHAR | FixedString(n) |
| DATE | Date |
| TIME | String |
| TIMESTAMP | DateTime64 |
| SECONDDATE | DateTime |
| BOOLEAN | UInt8 |

### 3. Schema Extraction
- Automatically extracts all user-accessible schemas
- Excludes system schemas (SYS, _SYS_BI, etc.)
- Shows table metadata (name, type, ClickHouse target)

### 4. Data Migration
- Batch processing (configurable batch size, default 10,000 rows)
- Progress logging
- Automatic type conversion
- Metadata columns added: `_source_schema`, `_source_table`, `_sync_timestamp`

### 5. Incremental Sync
- Optional incremental sync based on timestamp columns
- Sync metadata table tracks last sync time
- Can be enabled per table or globally

## 🚀 Usage Instructions

### Step 1: Add HANA Source
1. Navigate to **Add Source** → **SAP HANA**
2. Fill in connection details:
   - Source Name: e.g., "Production HANA"
   - Host: `192.168.16.62` (or your HANA server)
   - Port: `30015` (or your instance port)
   - Username: `Tor1111`
   - Password: `Tor1111`
   - Target Type: Select **ClickHouse**
   - Target Database: Select your ClickHouse database (defaults to `hana_migrated`)

3. Click **"Test Connection"** (requires hdbcli library)
4. Click **"Add SAP HANA Source"**

### Step 2: View and Select Tables
1. On the Sync Servers page, find your HANA source
2. Click **"View Tables"** button
3. Browse schemas on the left
4. Select tables to sync using checkboxes
5. Optionally enable **"Enable Incremental Sync"**
6. Click **"Sync Selected Tables"**

### Step 3: Monitor Sync
- Check Flask application logs for progress
- Each table shows:
  - Source: `schema.table`
  - Target: `database.SCHEMA_TABLE`
  - Status and row counts

## 📋 Prerequisites

### Python Dependencies
```bash
pip install hdbcli clickhouse-driver flask psycopg2 pandas
```

### HANA Client Library
- Windows: Install SAP HANA Client and ensure `hdbcli` is available
- Verify: `python -c "import hdbcli.dbapi; print('OK')"`

### ClickHouse
- Ensure ClickHouse is running and accessible
- Environment variables (optional):
  - `CLICKHOUSE_HOST` (default: localhost)
  - `CLICKHOUSE_PORT` (default: 9000)
  - `CLICKHOUSE_USER` (default: default)
  - `CLICKHOUSE_PASSWORD` (default: empty)

### VPN Connection (if required)
- Connect to VPN before adding source
- Test connection: `Test-NetConnection -ComputerName 192.168.16.62 -Port 30015`

## 🔧 Technical Details

### Connection Flow
1. User adds HANA source via web form
2. Source saved to PostgreSQL `data_sources` table
3. User clicks "View Tables" → Fetches schemas/tables from HANA
4. User selects tables → Triggers background sync thread
5. Sync thread:
   - Connects to HANA and ClickHouse
   - Extracts table schema
   - Creates ClickHouse table with `schema_tablename` format
   - Migrates data in batches
   - Optionally sets up incremental sync

### Error Handling
- Connection failures logged with detailed error messages
- Table creation errors handled gracefully
- Data migration errors reported per table
- All errors logged to Flask application logs

### Performance Considerations
- Batch processing prevents memory issues
- Limit of 20 schemas loaded at once (configurable)
- Background threads for sync operations
- Progress logging for long-running migrations

## 📝 Notes

1. **Table Names**: All ClickHouse tables follow `SCHEMA_TABLENAME` format to avoid conflicts
2. **Metadata**: Each row includes `_source_schema`, `_source_table`, and `_sync_timestamp`
3. **Incremental Sync**: Requires timestamp columns in source tables
4. **Large Tables**: Adjust `batch_size` parameter for very large tables (>10M rows)

## ✅ Testing Checklist

- [x] HANA connection test
- [x] Schema extraction
- [x] Table listing with ClickHouse target names
- [x] Table creation in ClickHouse
- [x] Data migration (batch processing)
- [x] Incremental sync setup
- [x] Error handling
- [x] UI integration

## 🎉 Implementation Complete!

The system is now ready to use. Follow the usage instructions above to:
1. Add HANA sources
2. Browse schemas and tables
3. Select and sync tables to ClickHouse
4. Monitor progress via logs

All table names in ClickHouse will follow the `schema_tablename` convention as specified.

