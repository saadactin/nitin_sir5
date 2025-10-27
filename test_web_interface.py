"""Test different query formats for ClickHouse web interface"""
try:
    from clickhouse_driver import Client as CHClient
except ImportError:
    print("✗ clickhouse-driver not installed")
    exit(1)

import requests
import json

print("=" * 100)
print("TESTING CLICKHOUSE WEB INTERFACE QUERIES")
print("=" * 100)

# Test 1: Using Python driver
print("\n1. Using Python Driver (clickhouse-driver):")
print("-" * 100)
client = CHClient(host='localhost', port=9000, user='default', password='')
result = client.execute('SELECT * FROM saadtest2.school_data_4 LIMIT 5')
print(f"✓ Result: {len(result)} rows")
for i, row in enumerate(result, 1):
    print(f"  Row {i}: {row}")

# Test 2: Using HTTP interface (same as web interface)
print("\n2. Using HTTP Interface (http://localhost:8123):")
print("-" * 100)

queries = [
    "SELECT * FROM saadtest2.school_data_4 LIMIT 5",
    "SELECT * FROM saadtest2.school_data_4 LIMIT 5 FORMAT JSONCompact",
    "SELECT * FROM saadtest2.school_data_4 LIMIT 5 FORMAT TabSeparated",
]

for query in queries:
    print(f"\nQuery: {query}")
    try:
        response = requests.get(
            'http://localhost:8123/',
            params={'query': query},
            auth=('default', '')
        )
        
        if response.status_code == 200:
            print(f"✓ Status: {response.status_code}")
            result_text = response.text[:500]  # First 500 chars
            print(f"✓ Result preview:\n{result_text}")
        else:
            print(f"✗ Status: {response.status_code}")
            print(f"✗ Error: {response.text}")
    except Exception as e:
        print(f"✗ Error: {e}")

# Test 3: Verify table is not empty
print("\n3. Verifying Table Content:")
print("-" * 100)
result = client.execute('SELECT COUNT(*) FROM saadtest2.school_data_4')
count = result[0][0]
print(f"Total rows in table: {count}")

if count > 0:
    print(f"✅ Table has data!")
    
    # Check if data is properly formatted
    result = client.execute('SELECT * FROM saadtest2.school_data_4 LIMIT 1')
    print(f"\nSample row:")
    row = result[0]
    print(f"  student_id: '{row[0]}' (type: {type(row[0]).__name__})")
    print(f"  student_name: '{row[1]}' (type: {type(row[1]).__name__})")
    print(f"  subject: '{row[2]}' (type: {type(row[2]).__name__})")
    print(f"  grade: '{row[3]}' (type: {type(row[3]).__name__})")
    print(f"  attendance: '{row[4]}' (type: {type(row[4]).__name__})")
else:
    print(f"❌ Table is empty!")

print("\n" + "=" * 100)
print("WEB INTERFACE INSTRUCTIONS:")
print("=" * 100)
print("Try these queries in http://localhost:8123 web interface:")
print("\n1. Basic query:")
print("   SELECT * FROM saadtest2.school_data_4 LIMIT 10")
print("\n2. With column names:")
print("   SELECT student_id, student_name, subject, grade, attendance")
print("   FROM saadtest2.school_data_4 LIMIT 10")
print("\n3. Count check:")
print("   SELECT COUNT(*) FROM saadtest2.school_data_4")
print("\n4. Formatted output:")
print("   SELECT * FROM saadtest2.school_data_4 LIMIT 10 FORMAT Pretty")
print("=" * 100)
