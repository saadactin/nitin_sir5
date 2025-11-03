# ClickHouse Environment Variables Setup

## For Your Remote ClickHouse Instance

Based on your ClickHouse server at `http://74.225.251.123:8123`, here are the environment variables you need to set.

## Required Environment Variables

Add these to your `.env` file (create it if it doesn't exist):

```bash
# ============================================
# ClickHouse Configuration
# ============================================
# ClickHouse server host/IP
CLICKHOUSE_HOST=74.225.251.123

# ClickHouse native protocol port (NOT HTTP port)
# The code uses clickhouse_driver which requires native protocol (port 9000)
# NOT the HTTP interface port (8123)
CLICKHOUSE_PORT=9000

# ClickHouse username (shown as "default" in your UI)
CLICKHOUSE_USER=default

# ClickHouse password (leave empty if no password, or use your actual password)
CLICKHOUSE_PASSWORD=

# Optional: ClickHouse database name (default database to use)
CLICKHOUSE_DATABASE=JARVIS_DB
```

## Important Notes

### 1. Port Difference
- **HTTP Interface (8123)**: Used for web UI and HTTP requests
- **Native Protocol (9000)**: Used by `clickhouse_driver` Python library
- **You need port 9000** for the sync system to work, NOT 8123

### 2. Password
If your ClickHouse instance has a password:
```bash
CLICKHOUSE_PASSWORD=your_actual_password_here
```

If no password (default setup):
```bash
CLICKHOUSE_PASSWORD=
# Or just omit the line
```

### 3. Database Name
The system will create databases automatically, but you can specify a default:
```bash
CLICKHOUSE_DATABASE=JARVIS_DB
```

## Verification Steps

### 1. Check if Port 9000 is Accessible

Test native protocol connection:
```bash
# Windows PowerShell
Test-NetConnection -ComputerName 74.225.251.123 -Port 9000

# Or using telnet (if available)
telnet 74.225.251.123 9000
```

### 2. Test Connection with Python

Create a test file `test_clickhouse_connection.py`:
```python
from clickhouse_driver import Client
from db_utils import load_clickhouse_config

# Load config
config = load_clickhouse_config()
print(f"Connecting to ClickHouse:")
print(f"  Host: {config['host']}")
print(f"  Port: {config['port']}")
print(f"  User: {config['user']}")
print(f"  Password: {'***' if config['password'] else '(empty)'}")

try:
    client = Client(
        host=config['host'],
        port=config['port'],
        user=config['user'],
        password=config['password']
    )
    
    # Test connection
    result = client.execute('SELECT 1')
    print("\n✅ Connection successful!")
    
    # List databases
    databases = client.execute('SHOW DATABASES')
    print("\nAvailable databases:")
    for db in databases:
        print(f"  - {db[0]}")
        
except Exception as e:
    print(f"\n❌ Connection failed: {e}")
    print("\nTroubleshooting:")
    print("  1. Verify CLICKHOUSE_HOST is correct")
    print("  2. Verify CLICKHOUSE_PORT is 9000 (not 8123)")
    print("  3. Check if firewall allows port 9000")
    print("  4. Verify username and password are correct")
```

Run it:
```bash
python test_clickhouse_connection.py
```

## Complete .env File Example

If you're starting fresh, here's a complete `.env` file template:

```bash
# ============================================
# PostgreSQL Configuration
# ============================================
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=your_pg_database
PG_USERNAME=your_pg_username
PG_PASSWORD=your_pg_password

# ============================================
# ClickHouse Configuration
# ============================================
CLICKHOUSE_HOST=74.225.251.123
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DATABASE=JARVIS_DB

# ============================================
# Application Settings
# ============================================
SECRET_KEY=your_secret_key_here
```

## Troubleshooting

### Issue: Connection timeout on port 9000

**Solution 1**: Your firewall might only allow HTTP (8123). Ask your server administrator to:
- Open port 9000 for native protocol
- Or configure ClickHouse to accept native connections on a different port

**Solution 2**: Check if ClickHouse is configured to listen on port 9000:
```bash
# On the ClickHouse server, check config
# Usually in /etc/clickhouse-server/config.xml
# Look for: <tcp_port>9000</tcp_port>
```

### Issue: Authentication failed

**Solution**: 
1. If password is required, make sure `CLICKHOUSE_PASSWORD` is set correctly
2. Check ClickHouse user permissions in `/etc/clickhouse-server/users.xml`

### Issue: Database not found

**Solution**: The system will create databases automatically. You can also create it manually:
```sql
CREATE DATABASE IF NOT EXISTS JARVIS_DB;
```

## Quick Setup Script

Run this to verify your setup:
```bash
python -c "
import os
print('Checking ClickHouse Environment Variables:')
print('=' * 50)
vars = ['CLICKHOUSE_HOST', 'CLICKHOUSE_PORT', 'CLICKHOUSE_USER', 'CLICKHOUSE_PASSWORD']
for var in vars:
    value = os.environ.get(var, '(not set)')
    if 'PASSWORD' in var and value != '(not set)':
        value = '***' if value else '(empty)'
    print(f'{var}: {value}')
"
```

## Next Steps

1. ✅ Add environment variables to `.env` file
2. ✅ Verify port 9000 is accessible
3. ✅ Test connection with test script
4. ✅ Start syncing data!

---

**Note**: The HTTP port (8123) is only for the web interface. The sync system needs the native protocol port (9000) to work properly.

