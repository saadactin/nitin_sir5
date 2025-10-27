"""Check specific ClickHouse table data"""
try:
    from clickhouse_driver import Client as CHClient
except ImportError:
    print("✗ clickhouse-driver not installed")
    exit(1)

client = CHClient(
    host='localhost',
    port=9000,
    user='default',
    password=''
)

print("=" * 100)
print("CHECKING CLICKHOUSE TABLES")
print("=" * 100)

# List all databases
print("\n1. All Databases:")
print("-" * 100)
result = client.execute('SHOW DATABASES')
databases = [row[0] for row in result]
for db in databases:
    print(f"  - {db}")

# Check saadtest2 database
print("\n2. Tables in 'saadtest2' database:")
print("-" * 100)
try:
    result = client.execute('SHOW TABLES FROM saadtest2')
    tables = [row[0] for row in result]
    if tables:
        for table in tables:
            print(f"  - {table}")
    else:
        print("  (No tables found)")
except Exception as e:
    print(f"  Error: {e}")

# Check school_data_4 specifically
print("\n3. Checking 'saadtest2.school_data_4':")
print("-" * 100)
try:
    # Check if table exists
    result = client.execute('EXISTS TABLE saadtest2.school_data_4')
    if result[0][0]:
        print("  ✓ Table exists")
        
        # Get structure
        result = client.execute('DESCRIBE saadtest2.school_data_4')
        print("\n  Table Structure:")
        for row in result:
            print(f"    {row[0]}: {row[1]}")
        
        # Get count
        result = client.execute('SELECT COUNT(*) FROM saadtest2.school_data_4')
        count = result[0][0]
        print(f"\n  Row Count: {count}")
        
        if count > 0:
            # Get sample data
            result = client.execute('SELECT * FROM saadtest2.school_data_4 LIMIT 10')
            print(f"\n  First 10 rows:")
            for i, row in enumerate(result, 1):
                print(f"    Row {i}: {row}")
        else:
            print(f"\n  ⚠️  TABLE IS EMPTY!")
            print(f"  This is why you don't see data in the query results.")
    else:
        print("  ✗ Table does not exist")
except Exception as e:
    print(f"  Error: {e}")

# Check saadtest database
print("\n4. Tables in 'saadtest' database:")
print("-" * 100)
try:
    result = client.execute('SHOW TABLES FROM saadtest')
    tables = [row[0] for row in result]
    if tables:
        for table in tables:
            # Get row count
            result = client.execute(f'SELECT COUNT(*) FROM saadtest.{table}')
            count = result[0][0]
            print(f"  - {table} ({count} rows)")
    else:
        print("  (No tables found)")
except Exception as e:
    print(f"  Error: {e}")

print("\n" + "=" * 100)
print("RECOMMENDATION:")
print("=" * 100)
print("If you want to query the test data we created, use:")
print("  SELECT * FROM saadtest.school_data_1")
print("\nIf you want data in saadtest2.school_data_4, you need to upload")
print("an Excel file to that table through the application.")
print("=" * 100)
