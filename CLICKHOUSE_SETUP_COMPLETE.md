# ✅ ClickHouse Configuration - COMPLETE

## Your Current Setup

Your ClickHouse environment variables are now correctly configured:

```bash
CLICKHOUSE_HOST=74.225.251.123
CLICKHOUSE_PORT=9000
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=root
```

## ✅ Connection Test Results

- **Status**: ✅ **CONNECTED SUCCESSFULLY**
- **ClickHouse Version**: 25.9.4.58
- **Host**: 74.225.251.123:9000
- **User**: default

## Available Databases

- ✅ **JARVIS_DB** - 2 tables (example_table, test_table)
- default - 0 tables
- jarvis - 0 tables  
- test1 - 0 tables

## What Was Fixed

### Issue 1: Incorrect Host Format
- **Before**: `CLICKHOUSE_HOST=http://74.225.251.123`
- **After**: `CLICKHOUSE_HOST=74.225.251.123`
- **Why**: The `http://` prefix should not be included for native protocol

### Issue 2: Wrong Port
- **Before**: `CLICKHOUSE_PORT=8123` (HTTP interface)
- **After**: `CLICKHOUSE_PORT=9000` (Native protocol)
- **Why**: The `clickhouse_driver` Python library requires native protocol (port 9000), not HTTP (port 8123)

## Understanding the Ports

| Port | Purpose | Used By |
|------|---------|---------|
| **9000** | Native Protocol | ✅ Python sync system (clickhouse_driver) |
| **8123** | HTTP Interface | Web UI (like you're viewing now) |

## How to Use

### 1. In the Web UI - Add API Source

When adding an API source that syncs to ClickHouse:

- **Target Database Type**: `ClickHouse`
- **Target Database Name**: `JARVIS_DB` (or any database name - it will be created automatically)

### 2. Verify Connection Anytime

Run this command to test:
```bash
python test_clickhouse_connection.py
```

### 3. Check Your Data

Your Zoho CRM Leads or other API data will sync to:
```
Database: JARVIS_DB (or the name you specify)
Table: [table_name you specify in the UI]
```

## Example: Setting Up Zoho CRM to ClickHouse

1. Go to "Add API Source" in web UI
2. Fill in:
   - Source Name: `zoho_leads`
   - API URL: `https://www.zohoapis.in/crm/v8/Leads?fields=Full_Name,Company,Email,Phone`
   - Auth Type: `Zoho OAuth 2.0`
   - Target DB: `ClickHouse`
   - **Target Database Name**: `JARVIS_DB` (or `zoho_leads_db`)
   - Target Table: `zoho_leads` (or leave default)
   - Enable Polling: ✅ (optional)
3. Click "Add & Start Sync"
4. Data will sync to ClickHouse!

## Troubleshooting

### If Connection Fails

1. **Check Port 9000 Access**:
   ```powershell
   Test-NetConnection -ComputerName 74.225.251.123 -Port 9000
   ```

2. **Verify Environment Variables**:
   ```bash
   python -c "import os; from dotenv import load_dotenv; load_dotenv(); print('HOST:', os.getenv('CLICKHOUSE_HOST')); print('PORT:', os.getenv('CLICKHOUSE_PORT'))"
   ```

3. **Test Connection**:
   ```bash
   python test_clickhouse_connection.py
   ```

### If Port 9000 is Blocked

If your firewall only allows port 8123:
- Contact your server administrator to open port 9000
- Or ask them to configure ClickHouse to accept native connections on a different accessible port

## Your Environment Variables Summary

```bash
# Required for ClickHouse connection
CLICKHOUSE_HOST=74.225.251.123
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=root

# Optional - default database to use
CLICKHOUSE_DATABASE=JARVIS_DB
```

## ✅ Status: Ready to Use!

Your ClickHouse connection is working perfectly. You can now:
- ✅ Sync API data to ClickHouse
- ✅ Use polling for incremental data
- ✅ View data in ClickHouse web UI
- ✅ Query data using SQL

---

**Next Steps:**
1. Add API sources in the web UI
2. Configure Zoho OAuth for Zoho CRM
3. Enable polling for automatic syncing
4. View synced data in ClickHouse!

