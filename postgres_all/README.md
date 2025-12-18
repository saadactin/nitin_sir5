# PostgreSQL Migration - Complete Package

This folder contains all files related to PostgreSQL to ClickHouse migration.

## Files Included

### Core Migration Files

1. **`pg_migration.py`**
   - Main migration wrapper module
   - Located in `migrations/` folder in the main project
   - Handles type mapping, schema creation, and data migration
   - Provides functions:
     - `map_postgresql_to_clickhouse_type()` - Maps PostgreSQL types to ClickHouse types
     - `get_postgresql_tables()` - Gets all tables from PostgreSQL
     - `get_table_schema()` - Gets column information for a table
     - `get_primary_key_columns()` - Gets primary key columns
     - `create_clickhouse_table()` - Creates ClickHouse tables
     - `migrate_table_data()` - Migrates data with duplicate detection
     - `run_pg_migration()` - Main migration function

2. **`migrate_pg_to_clickhouse.py`**
   - Core migration script
   - Located in `scripts/` folder in the main project
   - Standalone script that can be run directly
   - Migrates all tables from PostgreSQL public schema to ClickHouse with `HR_` prefix
   - Supports incremental updates (skips existing records)
   - Loads credentials from Jarvis_cred database

3. **`postgres.html`**
   - UI template for PostgreSQL migration
   - Located in `templates/` folder in the main project
   - Provides web interface for:
     - Testing PostgreSQL connection
     - Saving connection credentials
     - Starting migration

4. **`credential_manager.py`**
   - Credential management utility
   - Located in `migrations/` folder in the main project
   - Used by migration scripts to load credentials from Jarvis_cred database
   - Provides functions:
     - `setup_jarvis_cred_database()` - Sets up credential database
     - `save_credentials_to_db()` - Saves credentials to database
     - `get_credentials_from_db()` - Retrieves credentials from database
     - `get_all_migration_credentials()` - Gets both PG and CH credentials

## How It Works

1. **Connection Setup**: User configures PostgreSQL and ClickHouse connections via the web UI
2. **Credential Storage**: Credentials are saved to `Jarvis_cred` PostgreSQL database
3. **Migration Process**:
   - Fetches all tables from PostgreSQL public schema
   - For each table:
     - Maps PostgreSQL data types to ClickHouse types
     - Creates ClickHouse table with `HR_` prefix (if doesn't exist)
     - Migrates data (incremental: only new rows if table exists)
   - Uses primary keys for duplicate detection when available

## Features

- ✅ Automatic type mapping (PostgreSQL → ClickHouse)
- ✅ Incremental migration (only new/changed records)
- ✅ Primary key-based duplicate detection
- ✅ Full row comparison fallback (when no primary key)
- ✅ Batch insertion (1000 rows per batch)
- ✅ Error handling and logging
- ✅ Web UI integration

## Usage

### Via Web UI
1. Navigate to `/postgres` route
2. Enter PostgreSQL connection details
3. Test and save connection
4. Ensure ClickHouse connection is configured
5. Click "Start Migration"

### Direct Script Execution
```python
from migrate_pg_to_clickhouse import main

# With credentials from database
main(pg_host='localhost', pg_port=5432, pg_username='user', pg_password='pass')
```

## Table Naming Convention

- PostgreSQL tables: `table_name`
- ClickHouse tables: `HR_table_name` (with `HR_` prefix)

## Dependencies

- `psycopg2` - PostgreSQL connection
- `clickhouse-connect` - ClickHouse connection
- `logging` - Logging functionality

## Notes

- All tables are migrated from PostgreSQL `public` schema
- Tables are created in ClickHouse with `HR_` prefix
- Migration is incremental by default (only new records)
- Primary keys are used for duplicate detection when available

