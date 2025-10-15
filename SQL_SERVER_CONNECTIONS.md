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
- **Solution**: Our application now has built-in special handling for SQL2019_Second instances
  - **Automatic**: Just specify `server\SQL2019_Second` and we'll use port 14344 automatically
  - **Manual**: You can still use `server,14344` (explicit port specification) if needed
- **Troubleshooting**: Run the included `test_sql2019_second.py` script to diagnose connection issues

### 4. Multiple Servers and Instances
- The application supports connecting to any number of servers and instances
- Use `manage_server.py --add-interactive` to add new servers easily
- Each server can have its own authentication and configuration settings

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

## Managing Multiple Servers

### Adding a New Server

#### Option 1: Interactive Wizard (Recommended)

```powershell
python manage_server.py --add-interactive
```

This will:
1. Attempt to discover local SQL Server instances
2. Guide you through adding a new server
3. Test the connection automatically
4. Show available databases

#### Option 2: Command Line

```powershell
python manage_server.py --add SERVER_NAME HOST USERNAME PASSWORD
```

Examples:
```powershell
# Windows Authentication with default instance
python manage_server.py --add local_sql localhost windows windows

# SQL Authentication with named instance
python manage_server.py --add dev_sql2019 "localhost\SQL2019_Second" sa StrongPassword123
```

### Testing Connections

```powershell
# Test a specific server
python manage_server.py --test SERVER_NAME

# Specialized test for SQL2019_Second
python test_sql2019_second.py
```

### Managing Servers

```powershell
# List all servers
python manage_server.py --list

# Delete a server
python manage_server.py --delete SERVER_NAME
```

## Best Practices

1. **Server Management**: Use the interactive wizard (`--add-interactive`) when adding new servers

2. **Named Instances**: Let the system automatically handle port detection for named instances

3. **Windows Authentication**: When possible, use Windows Authentication for increased security

4. **Connection Testing**: Always test your connection when setting up a new server

5. **Multiple Servers**: Use descriptive names for servers to easily identify them in logs and reports

## Running Synchronization

### Synchronizing a Single Server

```powershell
# Sync a specific server once
python run_sync_worker.py --server SERVER_NAME --once

# Sync specific databases from a server
python run_sync_worker.py --server SERVER_NAME --databases db1,db2 --once
```

### Synchronizing All Servers

```powershell
# Sync all servers once
python run_sync_worker.py --all --once

# Run continuous sync for all servers (scheduled job)
python run_sync_worker.py
```

### Advanced Synchronization Options

```powershell
# Enable debug logging
python run_sync_worker.py --server SERVER_NAME --debug

# Run as a scheduled job with specific database targeting
python run_sync_worker.py --server SERVER_NAME --databases db1,db2
```

## Configuration Options

Each server entry in the configuration supports the following options:

- `server`: The server name or address (with instance name if applicable)
- `username`: SQL Server authentication username (or 'windows' for Windows Authentication)
- `password`: SQL Server authentication password (or 'windows' for Windows Authentication)
- `target_postgres_db`: Target PostgreSQL database for sync (default: 'postgres')
- `skip_databases`: List of databases to exclude from synchronization
- `sync_mode`: Synchronization mode (default: 'hybrid')
- `check_new_databases`: Whether to automatically discover and sync new databases