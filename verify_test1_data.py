"""
Verify data exists in test1 database
"""
from db_utils import load_clickhouse_config
from clickhouse_driver import Client
from dotenv import load_dotenv

load_dotenv()

config = load_clickhouse_config()
client = Client(
    host=config['host'],
    port=config['port'],
    user=config['user'],
    password=config['password']
)

print("=" * 70)
print("VERIFYING DATA IN test1 DATABASE")
print("=" * 70)

# List all tables in test1
try:
    client.execute('USE test1')
    tables = client.execute('SHOW TABLES')
    
    print(f"\nTables in 'test1' database: {len(tables)}")
    print("-" * 70)
    
    for table_row in tables:
        table_name = table_row[0] if isinstance(table_row, (list, tuple)) else table_row
        try:
            count = client.execute(f'SELECT count() FROM test1.{table_name}')[0][0]
            print(f"  {table_name}: {count} rows")
            
            # Show first few rows for test_data_insert
            if table_name == 'test_data_insert':
                rows = client.execute(f'SELECT * FROM test1.{table_name} LIMIT 5')
                print(f"    Sample data:")
                for row in rows:
                    print(f"      {row}")
        except Exception as e:
            print(f"  {table_name}: Error - {e}")
            
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 70)

