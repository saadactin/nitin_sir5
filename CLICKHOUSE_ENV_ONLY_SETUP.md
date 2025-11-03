# ClickHouse Environment Variables Setup

## ✅ Project Now Uses ONLY .env Variables

The entire project has been updated to use **ONLY** the 4 ClickHouse environment variables from your `.env` file. There are **NO hardcoded values** anywhere in the codebase.

## Required Environment Variables

Add these **4 variables** to your `.env` file:

```bash
CLICKHOUSE_HOST=74.225.251.123
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=root
```

## What Changed

### ✅ Files Updated:

1. **`db_utils.py`**
   - `load_clickhouse_config()` now **REQUIRES** all 4 environment variables
   - **NO hardcoded fallbacks** (no more `localhost`, `9000`, `default`, etc.)
   - Raises clear error if any variable is missing

2. **`hana_sync.py`**
   - `connect_clickhouse()` now uses `load_clickhouse_config()` from `.env`
   - Removed hardcoded `localhost`, `9000`, `default` values

3. **`app.py`**
   - All ClickHouse connections now use `load_clickhouse_config()`
   - Removed hardcoded default values in HANA sync route

### ✅ Already Correct (No Changes Needed):

- `api_sync.py` - Already uses `load_clickhouse_config()` ✅
- `api_polling.py` - Already uses `load_clickhouse_config()` ✅

## How It Works

1. **All ClickHouse connections** go through `db_utils.load_clickhouse_config()`
2. **This function** reads from `.env` file using `dotenv`
3. **Validates** that all 4 required variables are present
4. **Returns** the config dict with host, port, user, password
5. **No fallbacks** - if variables are missing, it raises a clear error

## Error Handling

If any required variable is missing, you'll get a clear error like:

```
ValueError: CLICKHOUSE_HOST environment variable is required.
Please set it in your .env file: CLICKHOUSE_HOST=your_host
```

This makes it **impossible to accidentally use wrong settings** - you MUST configure the `.env` file correctly.

## Testing

Test your configuration:

```bash
python test_clickhouse_connection.py
```

Or test the config loading directly:

```bash
python -c "from db_utils import load_clickhouse_config; from dotenv import load_dotenv; import os; load_dotenv(); config = load_clickhouse_config(); print('✅ Config loaded:', config)"
```

## Verification Checklist

✅ Set `CLICKHOUSE_HOST` in `.env`  
✅ Set `CLICKHOUSE_PORT` in `.env` (use `9000` for native protocol)  
✅ Set `CLICKHOUSE_USER` in `.env`  
✅ Set `CLICKHOUSE_PASSWORD` in `.env` (can be empty string `""`)  
✅ No hardcoded values in code  
✅ All connections use `load_clickhouse_config()`  

## Important Notes

1. **Port 9000** is for native protocol (required by `clickhouse_driver`)
2. **Port 8123** is for HTTP interface (web UI) - not used by sync code
3. **Password can be empty** - just set `CLICKHOUSE_PASSWORD=` with no value
4. **Restart your Flask app** after changing `.env` to pick up new values

## Summary

🎯 **Single Source of Truth**: All ClickHouse settings come from `.env` only  
🚫 **No Hardcoded Values**: All fallbacks removed  
✅ **Clear Errors**: Missing variables raise descriptive errors  
🔒 **Safe**: Can't accidentally use wrong connection settings  

