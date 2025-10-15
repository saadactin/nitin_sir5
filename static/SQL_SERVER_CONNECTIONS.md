# SQL Server Connection Guide

This document explains how SQL Server connections work in our application, particularly focusing on named instances and port handling.

## Connection Types

### 1. Default Instance (No Named Instance)
- **Format**: `server_name` or `ip_address`
- **Example**: `localhost` or `192.168.1.100`
- **Port**: Automatically uses default port 1433

### 2. Named Instance
- **Format**: `server_name\instance_name` or `ip_address\instance_name`
- **Example**: `localhost\SQL2019_Second` or `192.168.1.100\SQLEXPRESS`
- **Port**: Automatically resolved using SQL Browser service

### 3. Special Case for SQL2019_Second
- **Problem**: SQL2019_Second instance is configured to use port 14344 but sometimes has issues with SQL Browser service resolution
- **Solution Options**:
  - Use `localhost\SQL2019_Second` format (requires SQL Browser service running)
  - Or use `localhost,14344` (explicit port specification) which bypasses SQL Browser service
- **Troubleshooting**: Run the included `test_sql2019_second.bat` script to diagnose and fix connection issues

## How Port Detection Works

When you connect to a SQL Server, the following happens behind the scenes:

1. **Default Instance**: The system connects directly to port 1433 (SQL Server default port)

2. **Named Instance**: The system:
   - Queries the SQL Server Browser service (UDP port 1434)
   - The SQL Browser service returns the port for the requested named instance
   - The connection is then made to that specific port

3. **Explicit Port**: If you specify a server with an explicit port like `server,port`, it will use that port directly

## Requirements for Named Instance Connection

For named instances to work correctly:

1. SQL Server Browser service must be running on the target server
2. Firewall must allow:
   - UDP port 1434 (for SQL Browser queries)
   - The dynamic TCP port used by the named instance

## Troubleshooting Connection Issues

If you're having trouble connecting to a named instance:

1. **Verify SQL Browser Service**: Ensure the SQL Server Browser service is running on the target server

2. **Check Firewall**: Make sure both UDP 1434 and the instance's TCP port are allowed through any firewalls

3. **Use Diagnostic Endpoint**: Use our diagnostic endpoint to test connections and get detailed information:
   ```
   /api/diagnose-sql-server/<server_name>
   ```

4. **Common Error Messages**:
   - "Error Locating Server/Instance Specified" - Usually means the SQL Browser service isn't available or the instance doesn't exist
   - "Login timeout expired" - The server was found but the connection attempt timed out
   - "Cannot connect to server\instance" - The instance name might be incorrect or not accessible

## Best Practices

1. **Use Named Instances**: Let the system automatically handle port detection for named instances

2. **Windows Authentication**: When possible, use Windows Authentication for increased security

3. **Connection Testing**: Always test your connection when setting up a new server