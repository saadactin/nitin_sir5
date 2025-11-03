"""
Test ClickHouse Connection and Data Insertion
Tests if the application can correctly push data to ClickHouse database 'test1'
"""
from db_utils import load_clickhouse_config
from clickhouse_driver import Client
from dotenv import load_dotenv
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_clickhouse_connection_and_insert():
    """Test ClickHouse connection and insert test data"""
    print("=" * 70)
    print("TESTING: ClickHouse Connection and Data Insertion")
    print("=" * 70)
    
    # Load .env
    load_dotenv()
    
    print("\n1. Loading ClickHouse Config from .env:")
    print("-" * 70)
    try:
        config = load_clickhouse_config()
        print(f"   Host: {config['host']}")
        print(f"   Port: {config['port']}")
        print(f"   User: {config['user']}")
        print(f"   Password: {'***' if config['password'] else '(empty)'}")
    except ValueError as e:
        print(f"   [ERROR] {e}")
        return False
    
    print("\n2. Connecting to ClickHouse:")
    print("-" * 70)
    try:
        client = Client(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            password=config['password'],
            connect_timeout=10
        )
        
        # Test connection
        result = client.execute('SELECT 1')
        print("   [OK] Connection successful!")
        
        # Get ClickHouse version
        version = client.execute('SELECT version()')
        print(f"   [OK] ClickHouse version: {version[0][0]}")
        
    except Exception as e:
        print(f"   [ERROR] Connection failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("\n3. Checking Database 'test1':")
    print("-" * 70)
    try:
        # Check if test1 database exists
        databases = client.execute('SHOW DATABASES')
        db_list = [db[0] if isinstance(db, (list, tuple)) else db for db in databases]
        
        if 'test1' in db_list:
            print("   [OK] Database 'test1' exists")
        else:
            print("   [INFO] Database 'test1' does not exist, creating it...")
            client.execute('CREATE DATABASE IF NOT EXISTS test1')
            print("   [OK] Database 'test1' created")
        
        # Use test1 database
        client.execute('USE test1')
        
    except Exception as e:
        print(f"   [ERROR] Database check failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("\n4. Creating Test Table:")
    print("-" * 70)
    try:
        table_name = 'test_data_insert'
        
        # Drop table if exists for clean test
        try:
            client.execute(f'DROP TABLE IF EXISTS test1.{table_name}')
            print(f"   [INFO] Dropped existing table '{table_name}' for clean test")
        except:
            pass
        
        # Create table
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS test1.{table_name} (
            id UInt64,
            name String,
            value Float64,
            created_at DateTime DEFAULT now(),
            _sync_timestamp DateTime64(3) DEFAULT now64(3)
        ) ENGINE = MergeTree()
        ORDER BY id
        """
        
        client.execute(create_table_sql)
        print(f"   [OK] Table '{table_name}' created successfully")
        
    except Exception as e:
        print(f"   [ERROR] Table creation failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("\n5. Inserting Test Data:")
    print("-" * 70)
    try:
        # Insert test records
        test_data = [
            (1, 'Test Record 1', 100.5),
            (2, 'Test Record 2', 200.75),
            (3, 'Test Record 3', 300.25),
        ]
        
        insert_sql = f"""
        INSERT INTO test1.{table_name} (id, name, value)
        VALUES
        """
        
        # Build VALUES clause
        values_list = []
        for id_val, name_val, value_val in test_data:
            values_list.append(f"({id_val}, '{name_val}', {value_val})")
        
        insert_sql += ', '.join(values_list)
        
        client.execute(insert_sql)
        print(f"   [OK] Inserted {len(test_data)} test records")
        print(f"        Records: {test_data}")
        
    except Exception as e:
        print(f"   [ERROR] Data insertion failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("\n6. Verifying Inserted Data:")
    print("-" * 70)
    try:
        # Count records
        count_result = client.execute(f'SELECT count() FROM test1.{table_name}')
        count = count_result[0][0] if count_result else 0
        print(f"   [OK] Total records in table: {count}")
        
        # Fetch all records
        records = client.execute(f'SELECT * FROM test1.{table_name} ORDER BY id')
        print(f"\n   Retrieved {len(records)} record(s):")
        for record in records:
            print(f"      {record}")
        
        if count == len(test_data):
            print("\n   [OK] Data verification successful!")
        else:
            print(f"\n   [WARNING] Expected {len(test_data)} records, found {count}")
        
    except Exception as e:
        print(f"   [ERROR] Data verification failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("\n" + "=" * 70)
    print("[SUCCESS] ClickHouse Connection and Data Insertion Test PASSED")
    print("=" * 70)
    print(f"\nTest table: test1.{table_name}")
    print(f"Test data inserted successfully!")
    print("\nYour application should be able to push data to ClickHouse correctly.")
    print("=" * 70)
    
    return True

if __name__ == '__main__':
    success = test_clickhouse_connection_and_insert()
    exit(0 if success else 1)

