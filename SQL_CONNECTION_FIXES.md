# SQL Connection Fixes

## Summary of Issues and Fixes

### Issue 1: SQLAlchemy Error with String Queries
**Error Message**: `Not an executable object: 'SELECT @@SERVERNAME'`

**Fix**: 
- Added explicit use of SQLAlchemy's `text()` function when executing SQL strings
- Updated the `get_sqlalchemy_engine` function to properly use `text()` for executing SQL strings

### Issue 2: Named Instance Connection for SQL2019_Second
**Error Message**: `The client cannot connect to the server because the requested instance was not available`

**Fix**:
- Added special handling for the SQL2019_Second named instance
- Used direct port 14344 instead of named instance resolution
- Updated both `get_sql_connection` and `get_sqlalchemy_engine` functions with this handling

## Implementation Details

### 1. SQLAlchemy Fix
For SQLAlchemy connections, we ensure that all raw SQL strings are wrapped in the `text()` function:
```python
# Before:
result = connection.execute("SELECT @@SERVERNAME")

# After:
from sqlalchemy import text
result = connection.execute(text("SELECT @@SERVERNAME"))
```

### 2. SQL2019_Second Named Instance Fix
For the SQL2019_Second named instance, we now directly use port 14344:
```python
# Special handling for SQL2019_Second instance
if is_named_instance and "SQL2019_SECOND" in server.upper():
    host = server.split("\\")[0]
    server = f"{host},14344"
    is_named_instance = False  # Now using direct port
    logging.info(f"Using direct port connection for SQL2019_Second: {server}")
```

## Verification
The fixes were tested using:
1. The test_sql2019_second.py diagnostic script
2. A new test_connections.py script that tests both pyodbc and SQLAlchemy connections

Multiple connection methods for SQL2019_Second were verified as working:
- Standard named instance with proper backslash escaping
- Direct port connection to port 14344
- Named instance with encryption disabled
- Combinations of the above

The test results confirm that both issues have been fixed and connections now work properly.

## Future Maintenance
- The direct port approach for SQL2019_Second is more reliable than named instance resolution
- If SQL Server instances are moved or reconfigured, the port numbers might need to be updated
- Consider adding more robust error handling and fallback mechanisms for SQL Server connection failures