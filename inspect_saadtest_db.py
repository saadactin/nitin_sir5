"""
Inspect the contents of the saadtest database in ClickHouse
"""
import os
from dotenv import load_dotenv
from clickhouse_driver import Client

# Load environment variables
load_dotenv()
print("[OK] Loaded .env file\n")

# Get connection details
host = os.getenv('CLICKHOUSE_HOST', 'localhost')
port = int(os.getenv('CLICKHOUSE_PORT', 9000))
user = os.getenv('CLICKHOUSE_USER', 'default')
password = os.getenv('CLICKHOUSE_PASSWORD', '')

print("=" * 60)
print("Inspecting ClickHouse Database: saadtest")
print("=" * 60)
print(f"Host:     {host}")
print(f"Port:     {port}")
print(f"User:     {user}")
print(f"Database: saadtest")
print("=" * 60)

try:
    # Connect to ClickHouse
    client = Client(
        host=host,
        port=port,
        user=user,
        password=password
    )
    print("\n[OK] Connected to ClickHouse\n")
    
    # Show all tables in saadtest database
    print("=" * 60)
    print("TABLES IN 'saadtest' DATABASE:")
    print("=" * 60)
    
    result = client.execute("SHOW TABLES FROM saadtest")
    
    if not result:
        print("(No tables found in this database)")
    else:
        tables = [row[0] for row in result]
        print(f"\nFound {len(tables)} table(s):\n")
        
        for idx, table_name in enumerate(tables, 1):
            print(f"{idx}. {table_name}")
        
        print("\n" + "=" * 60)
        print("TABLE DETAILS:")
        print("=" * 60)
        
        # For each table, show structure and sample data
        for table_name in tables:
            print(f"\n📋 Table: {table_name}")
            print("-" * 60)
            
            # Get table structure
            print("COLUMNS:")
            desc = client.execute(f"DESCRIBE TABLE saadtest.{table_name}")
            for col in desc:
                col_name = col[0]
                col_type = col[1]
                print(f"  - {col_name}: {col_type}")
            
            # Get row count
            count_result = client.execute(f"SELECT COUNT(*) FROM saadtest.{table_name}")
            row_count = count_result[0][0]
            print(f"\nROW COUNT: {row_count}")
            
            # Show sample data (first 5 rows)
            if row_count > 0:
                print(f"\nSAMPLE DATA (first 5 rows):")
                sample = client.execute(f"SELECT * FROM saadtest.{table_name} LIMIT 5")
                
                # Get column names for header
                col_names = [col[0] for col in desc]
                
                # Print header
                print("  " + " | ".join(col_names))
                print("  " + "-" * (len(" | ".join(col_names))))
                
                # Print rows
                for row in sample:
                    print("  " + " | ".join(str(val) if val is not None else 'NULL' for val in row))
            else:
                print("\n(Table is empty)")
            
            print()
    
    print("=" * 60)
    print("[SUCCESS] Inspection complete!")
    print("=" * 60)
    
except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()
