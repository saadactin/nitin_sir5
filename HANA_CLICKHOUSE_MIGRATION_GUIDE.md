# HANA to ClickHouse Migration Guide

## Overview

This guide explains how to use the test script (`test_hana_to_clickhouse_migration.py`) to migrate data from SAP HANA to ClickHouse database.

## Prerequisites

### 1. Required Python Packages

```bash
pip install hdbcli clickhouse-driver pandas python-dotenv
```

### 2. SAP HANA Access

- Valid HANA server IP/hostname
- HANA port (typically 30015)
- Username and password with read permissions
- Network access to HANA server (VPN connection if needed)

### 3. ClickHouse Access

- ClickHouse server running and accessible
- Username and password
- Database permissions to create databases and tables

## Configuration

### Option 1: Environment Variables (Recommended)

Create a `.env` file in the project root:

```env
# HANA Configuration
HANA_HOST=192.168.16.62
HANA_PORT=30015
HANA_USERNAME=your_username
HANA_PASSWORD=your_password

# ClickHouse Configuration
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your_password
CLICKHOUSE_DATABASE=hana_migrated
```

### Option 2: Direct Configuration in Script

Edit the `HANA_CONFIG` and `CLICKHOUSE_CONFIG` dictionaries in `test_hana_to_clickhouse_migration.py`:

```python
HANA_CONFIG = {
    'host': '192.168.16.62',
    'port': 30015,
    'username': 'your_username',
    'password': 'your_password'
}

CLICKHOUSE_CONFIG = {
    'host': 'localhost',
    'port': 9000,
    'user': 'default',
    'password': 'your_password',
    'database': 'hana_migrated'
}
```

## Running the Migration Test

### Step 1: Test Connections

The script will automatically test connections to both databases:

```bash
python test_hana_to_clickhouse_migration.py
```

### Step 2: Review Connection Test Results

The script will:
1. ✅ Test HANA connection
2. ✅ Test ClickHouse connection
3. ✅ Discover HANA schemas and tables
4. ✅ Perform full migration

### Step 3: Verify Migration

After migration completes, verify data:

```bash
# Connect to ClickHouse
clickhouse-client --host localhost --user default --password

# Check migrated database
SHOW DATABASES;
USE hana_migrated;
SHOW TABLES;

# Verify row counts
SELECT COUNT(*) FROM your_table_name;
```

## Understanding the Migration Process

### 1. Schema Discovery

The script automatically:
- Connects to HANA
- Discovers all user-accessible schemas
- Lists all tables in each schema
- Retrieves column metadata for each table

### 2. Data Type Mapping

HANA data types are automatically mapped to ClickHouse equivalents:

| HANA Type | ClickHouse Type |
|-----------|---------------|
| INTEGER, INT | Int32 |
| BIGINT | Int64 |
| DECIMAL | Decimal64 |
| VARCHAR, NVARCHAR | String |
| CHAR | FixedString |
| DATE | Date |
| TIMESTAMP | DateTime64 |
| BOOLEAN | UInt8 |

### 3. Table Creation

ClickHouse tables are created with:
- Original column names and mapped data types
- Nullable columns for nullable HANA columns
- Additional metadata columns:
  - `_source_schema`: Original HANA schema name
  - `_source_table`: Original HANA table name
  - `_sync_timestamp`: Migration timestamp

### 4. Data Migration

Data is migrated in batches:
- Default batch size: 10,000 rows
- Progress logged in real-time
- Row counts validated after migration
- Errors are logged for troubleshooting

### 5. Table Naming

ClickHouse table names follow the pattern:
```
{SCHEMA_NAME}_{TABLE_NAME}
```

Example: HANA table `SALES.ORDERS` becomes `SALES_ORDERS` in ClickHouse

## Migration Settings

You can customize migration behavior in `MIGRATION_SETTINGS`:

```python
MIGRATION_SETTINGS = {
    'batch_size': 10000,              # Rows per batch
    'test_schemas': None,              # None = all, or ['SCHEMA1', 'SCHEMA2']
    'test_tables': None,               # None = all, or ['TABLE1', 'TABLE2']
    'skip_empty_tables': True,        # Skip tables with 0 rows
    'validate_migration': True        # Verify row counts match
}
```

## Troubleshooting

### Connection Issues

**Problem: Cannot connect to HANA**
- ✅ Verify VPN is connected (if required)
- ✅ Check HANA server IP and port are correct
- ✅ Test connection with: `telnet 192.168.16.62 30015`
- ✅ Verify username/password are correct
- ✅ Check firewall rules allow HANA port
- ✅ Ensure HANA server is running

**Problem: Cannot connect to ClickHouse**
- ✅ Verify ClickHouse server is running
- ✅ Check host/port are correct (9000 for native, 8123 for HTTP)
- ✅ Verify username/password
- ✅ Test with: `clickhouse-client --host localhost --port 9000`

### Migration Issues

**Problem: Some tables fail to migrate**
- Check the log file `hana_clickhouse_test.log` for detailed errors
- Verify HANA user has SELECT permissions on all tables
- Check for data type conversion issues
- Verify ClickHouse has sufficient disk space

**Problem: Row count mismatch**
- Some tables may have NULL values or special characters
- Check log for specific error messages
- Verify data types are compatible

**Problem: Migration is slow**
- Reduce batch size if memory is limited
- Increase batch size for better performance
- Check network latency between HANA and ClickHouse
- Consider migrating specific schemas/tables only

### Data Type Issues

**Problem: Data type mapping errors**
- Check log for unsupported HANA data types
- Review `map_hana_to_clickhouse_type()` function in `hana_sync.py`
- Some complex types (arrays, JSON) may need manual handling

## Best Practices

### 1. Pre-Migration Checklist

- [ ] HANA server is accessible
- [ ] ClickHouse server is running
- [ ] Network connectivity tested
- [ ] Credentials are correct
- [ ] Sufficient disk space in ClickHouse
- [ ] Backup of ClickHouse database (if exists)

### 2. Migration Strategy

1. **Start Small**: Test with 1-2 tables first
2. **Validate**: Check row counts and sample data
3. **Scale Up**: Migrate schemas incrementally
4. **Monitor**: Watch logs and system resources

### 3. Performance Optimization

- Use appropriate batch sizes (10,000-50,000 rows)
- Migrate during off-peak hours
- Consider parallel migrations for multiple schemas
- Monitor network bandwidth

### 4. Data Verification

After migration, verify:
- ✅ Row counts match
- ✅ Data types are correct
- ✅ Sample data looks correct
- ✅ No NULL values unexpectedly converted
- ✅ Date/timestamp values are accurate

## Integration with Application

Once you have real HANA credentials, you can use the application's web interface:

1. Navigate to "Add Source" → "SAP HANA"
2. Enter HANA connection details:
   - Host: `192.168.16.62`
   - Port: `30015`
   - Username: Your HANA username
   - Password: Your HANA password
3. Set target type to "ClickHouse"
4. Configure ClickHouse environment variables
5. Click "Sync" to start migration

The application uses the same `hana_sync.py` module as this test script.

## Support

For issues or questions:
1. Check `hana_clickhouse_test.log` for detailed error messages
2. Review HANA and ClickHouse server logs
3. Verify network connectivity and firewall rules
4. Test connections manually before running migration

## Example Output

```
======================================================================
HANA TO CLICKHOUSE MIGRATION TEST SCRIPT
======================================================================

Starting tests...

======================================================================
TEST 1: Testing HANA Connection
======================================================================
Attempting to connect to HANA at 192.168.16.62:30015
✅ HANA Connection: SUCCESS
   Connected as: YOUR_USERNAME
   Server timestamp: 2024-01-15 10:30:45

======================================================================
TEST 2: Testing ClickHouse Connection
======================================================================
Attempting to connect to ClickHouse at localhost:9000
✅ ClickHouse Connection: SUCCESS
   Version: 23.12.1.1
   Current database: hana_migrated

======================================================================
TEST 3: Discovering HANA Schemas
======================================================================
✅ Found 5 schema(s):
   1. SCHEMA1
   2. SCHEMA2
   ...

======================================================================
MIGRATION TEST SUMMARY
======================================================================
Total tables processed: 25
Successful migrations: 25
Failed migrations: 0
Success rate: 100.0%
```

---

**Note**: This is a test script to verify the migration process. For production migrations, ensure proper backup, testing, and validation procedures are followed.

