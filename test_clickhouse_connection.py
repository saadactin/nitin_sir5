"""
Test ClickHouse Connection
Verifies that ClickHouse environment variables are set correctly
"""
from clickhouse_driver import Client
from db_utils import load_clickhouse_config
import os

# Load .env file if dotenv is available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

def test_clickhouse_connection():
    """Test ClickHouse connection using environment variables"""
    print("=" * 70)
    print("CLICKHOUSE CONNECTION TEST")
    print("=" * 70)
    
    # Check environment variables
    print("\n1. Checking Environment Variables:")
    print("-" * 70)
    
    env_vars = {
        'CLICKHOUSE_HOST': os.environ.get('CLICKHOUSE_HOST'),
        'CLICKHOUSE_PORT': os.environ.get('CLICKHOUSE_PORT'),
        'CLICKHOUSE_USER': os.environ.get('CLICKHOUSE_USER'),
        'CLICKHOUSE_PASSWORD': os.environ.get('CLICKHOUSE_PASSWORD')
    }
    
    for var, value in env_vars.items():
        if value:
            display_value = value if 'PASSWORD' not in var else ('***' if value else '(empty)')
            print(f"   [OK] {var} = {display_value}")
        else:
            print(f"   [WARNING] {var} = (not set - will use defaults)")
    
    # Load config
    print("\n2. Loading ClickHouse Configuration:")
    print("-" * 70)
    try:
        config = load_clickhouse_config()
        print(f"   Host: {config['host']}")
        print(f"   Port: {config['port']}")
        print(f"   User: {config['user']}")
        print(f"   Password: {'***' if config['password'] else '(empty)'}")
    except Exception as e:
        print(f"   ❌ Error loading config: {e}")
        return False
    
    # Test connection
    print("\n3. Testing Connection:")
    print("-" * 70)
    try:
        print(f"   Connecting to {config['host']}:{config['port']}...")
        client = Client(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            password=config['password'],
            connect_timeout=10
        )
        
        # Test query
        result = client.execute('SELECT 1')
        print(f"   [OK] Connection successful!")
        
        # Get ClickHouse version
        version = client.execute('SELECT version()')
        print(f"   [OK] ClickHouse version: {version[0][0]}")
        
        # List databases
        print("\n4. Available Databases:")
        print("-" * 70)
        databases = client.execute('SHOW DATABASES')
        for db in databases:
            db_name = db[0]
            # Skip system databases for cleaner output
            if db_name not in ['system', 'information_schema', 'INFORMATION_SCHEMA']:
                # Get table count
                try:
                    table_count = client.execute(f"SELECT count() FROM system.tables WHERE database = '{db_name}'")
                    count = table_count[0][0] if table_count else 0
                    marker = "[HAS TABLES]" if count > 0 else ""
                    print(f"   {marker} {db_name} ({count} tables)")
                except:
                    print(f"   [DB] {db_name}")
        
        # Test specific database if provided
        test_db = os.environ.get('CLICKHOUSE_DATABASE')
        if test_db:
            print(f"\n5. Testing Database '{test_db}':")
            print("-" * 70)
            try:
                client.execute(f'USE {test_db}')
                tables = client.execute(f"SHOW TABLES")
                if tables:
                    print(f"   ✅ Database '{test_db}' exists")
                    print(f"   📊 Tables in {test_db}:")
                    for table in tables:
                        table_name = table[0]
                        try:
                            row_count = client.execute(f'SELECT count() FROM {test_db}.{table_name}')
                            count = row_count[0][0] if row_count else 0
                            print(f"      - {table_name}: {count} rows")
                        except:
                            print(f"      - {table_name}")
                else:
                    print(f"   ✅ Database '{test_db}' exists (empty - no tables)")
            except Exception as e:
                print(f"   ⚠️  Database '{test_db}' not found or error: {e}")
                print(f"   💡 The system will create it automatically when needed")
        
        print("\n" + "=" * 70)
        print("[OK] ALL TESTS PASSED - ClickHouse is ready!")
        print("=" * 70)
        return True
        
    except Exception as e:
        print(f"   [ERROR] Connection failed!")
        print(f"\n   Error: {e}")
        print("\n" + "=" * 70)
        print("TROUBLESHOOTING:")
        print("=" * 70)
        print("1. Verify CLICKHOUSE_HOST is correct:")
        print(f"   Current: {config.get('host', 'not set')}")
        print("   Expected: 74.225.251.123")
        print("\n2. Verify CLICKHOUSE_PORT is correct:")
        print(f"   Current: {config.get('port', 'not set')}")
        print("   Expected: 9000 (native protocol, NOT 8123)")
        print("\n3. Verify network connectivity:")
        print("   Test: Test-NetConnection -ComputerName 74.225.251.123 -Port 9000")
        print("\n4. Verify credentials:")
        print(f"   User: {config.get('user', 'not set')}")
        print("   Password: Check if password is required")
        print("\n5. Check firewall settings:")
        print("   Port 9000 must be open for native protocol")
        print("\n6. If only HTTP (8123) is accessible:")
        print("   Contact server admin to open port 9000")
        print("=" * 70)
        return False

if __name__ == '__main__':
    success = test_clickhouse_connection()
    exit(0 if success else 1)

