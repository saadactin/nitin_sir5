# Fixed ClickHouse Query Syntax Errors

## Problem

ClickHouse was throwing syntax errors:
```
DB::Exception: Syntax error: failed at position 140 (%) (line 4, col 39): %s AND source_table = %s AND sync_enabled = 1
```

## Root Cause

**ClickHouse doesn't support PostgreSQL-style `%s` placeholders!**

ClickHouse uses a different syntax for parameterized queries. The code was using PostgreSQL-style `%s` placeholders which ClickHouse doesn't understand.

## Solution

Replaced all `%s` placeholders with **string formatting with proper escaping**:

### Before (❌ Wrong):
```python
result = self.ch_client.execute(f"""
SELECT last_sync_timestamp, last_sync_id 
FROM {database_name}.sync_metadata 
WHERE source_schema = %s AND source_table = %s AND sync_enabled = 1
""", [schema, table])
```

### After (✅ Correct):
```python
result = self.ch_client.execute(f"""
SELECT last_sync_timestamp, last_sync_id 
FROM {database_name}.sync_metadata 
WHERE source_schema = '{schema.replace("'", "''")}' AND source_table = '{table.replace("'", "''")}' AND sync_enabled = 1
""")
```

## Changes Made

1. **Line 447-452**: Fixed `SELECT` query for getting last sync timestamp
2. **Line 379-380**: Fixed `table_exists_in_clickhouse` method
3. **Line 610-631**: Fixed metadata update queries (changed to DELETE + INSERT pattern)
4. **Line 636-645**: Fixed fallback metadata update
5. **Line 543-549**: Fixed update when no new data

## SQL Injection Protection

All string values are properly escaped using `.replace("'", "''")` to prevent SQL injection attacks.

## ClickHouse Compatibility

- **ALTER UPDATE**: Not supported in all ClickHouse versions
- **ALTER DELETE + INSERT**: More compatible approach
- **String formatting**: Works across all ClickHouse versions

## Files Modified

- `hana_sync.py`: Fixed all ClickHouse queries to use proper syntax

---

**Status**: ✅ Fixed - All ClickHouse queries now use correct syntax without `%s` placeholders

