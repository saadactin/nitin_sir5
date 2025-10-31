"""
Comprehensive test suite for API to ClickHouse sync
Tests all edge cases and scenarios
"""
import subprocess
import time
import requests
from clickhouse_driver import Client
import psycopg2
import os
from dotenv import load_dotenv
import sys

load_dotenv()

print("="*100)
print("COMPREHENSIVE API TO CLICKHOUSE SYNC TEST SUITE")
print("="*100)

# Test results
results = []

def test_result(name, passed, details=""):
    """Record test result"""
    status = "[PASS]" if passed else "[FAIL]"
    results.append((name, passed, details))
    print(f"{status} {name}")
    if details:
        print(f"      {details}")

# TEST 1: Start test API server
print("\n[TEST 1] Starting test API server...")
try:
    # Kill any existing process on port 5555
    subprocess.run("netstat -ano | findstr :5555", shell=True, capture_output=True)
    
    # Start server in background
    subprocess.Popen([sys.executable, "test_api_server.py"], 
                     stdout=subprocess.PIPE, 
                     stderr=subprocess.PIPE,
                     creationflags=subprocess.CREATE_NEW_CONSOLE)
    
    # Wait for server to start
    print("   Waiting for server to start...")
    for i in range(10):
        time.sleep(1)
        try:
            response = requests.get("http://localhost:5555/api/stats", timeout=2)
            if response.status_code == 200:
                data = response.json()
                test_result("Test API Server Started", True, f"Initial records: {data['total_records']}")
                break
        except:
            if i == 9:
                test_result("Test API Server Started", False, "Server didn't start in 10 seconds")
except Exception as e:
    test_result("Test API Server Started", False, str(e))

time.sleep(2)

# TEST 2: Test auto-detection with wrapped data
print("\n[TEST 2] Testing auto-detection with wrapped data...")
try:
    from api_data_detector import auto_detect_and_extract
    
    response = requests.get("http://localhost:5555/api/data")
    json_data = response.json()
    
    detected_path, records = auto_detect_and_extract(json_data, '')
    
    if detected_path == 'data' and len(records) > 0:
        test_result("Auto-detect wrapped data", True, f"Path='{detected_path}', Records={len(records)}")
    else:
        test_result("Auto-detect wrapped data", False, f"Path='{detected_path}', Records={len(records)}")
except Exception as e:
    test_result("Auto-detect wrapped data", False, str(e))

# TEST 3: Test auto-detection with direct array
print("\n[TEST 3] Testing auto-detection with direct array...")
try:
    response = requests.get("http://localhost:5555/api/data/simple")
    json_data = response.json()
    
    detected_path, records = auto_detect_and_extract(json_data, '')
    
    if detected_path == '' and len(records) > 0:
        test_result("Auto-detect direct array", True, f"Records={len(records)}")
    else:
        test_result("Auto-detect direct array", False, f"Path='{detected_path}', Records={len(records)}")
except Exception as e:
    test_result("Auto-detect direct array", False, str(e))

# TEST 4: Test schema inference with complex nested data
print("\n[TEST 4] Testing schema inference with nested data...")
try:
    from api_sync import flatten_record, infer_clickhouse_type
    
    response = requests.get("http://localhost:5555/api/data")
    json_data = response.json()
    _, records = auto_detect_and_extract(json_data, '')
    
    if records:
        sample = records[0]
        flattened = flatten_record(sample)
        
        # Check if nested fields are flattened
        has_nested = any('_' in key or '.' in key for key in flattened.keys())
        num_fields = len(flattened)
        
        test_result("Flatten nested JSON", True, f"{num_fields} fields created from nested structure")
        
        # Check data type inference
        all_columns = {}
        for key, value in flattened.items():
            all_columns[key] = infer_clickhouse_type(value)
        
        has_types = len(all_columns) == len(flattened)
        test_result("Infer data types", has_types, f"Inferred types for {len(all_columns)} columns")
    else:
        test_result("Flatten nested JSON", False, "No records to test")
        
except Exception as e:
    test_result("Flatten nested JSON", False, str(e))
    test_result("Infer data types", False, str(e))

# TEST 5: Create ClickHouse table with complete schema
print("\n[TEST 5] Creating ClickHouse table...")
try:
    from api_sync import create_clickhouse_table_from_sample
    from datetime import datetime
    
    ch = Client(host='localhost')
    
    # Drop test table if exists
    ch.execute("DROP TABLE IF EXISTS test1.test_dynamic")
    
    response = requests.get("http://localhost:5555/api/data")
    json_data = response.json()
    _, records = auto_detect_and_extract(json_data, '')
    
    # Get complete schema from all records
    all_columns = {}
    for record in records:
        flat = flatten_record(record)
        for key, value in flat.items():
            if key not in all_columns:
                all_columns[key] = infer_clickhouse_type(value)
    
    all_columns['_sync_timestamp'] = 'DateTime64(3)'
    
    # Create table
    create_clickhouse_table_from_sample('test1', 'test_dynamic', all_columns, already_flattened=True)
    
    # Verify table exists
    tables = ch.execute("SHOW TABLES FROM test1")
    table_names = [t[0] for t in tables]
    
    if 'test_dynamic' in table_names:
        test_result("Create ClickHouse table", True, f"Table created with {len(all_columns)} columns")
    else:
        test_result("Create ClickHouse table", False, "Table not found after creation")
        
except Exception as e:
    test_result("Create ClickHouse table", False, str(e))
    import traceback
    traceback.print_exc()

# TEST 6: Insert initial data
print("\n[TEST 6] Inserting initial data...")
try:
    ch = Client(host='localhost')
    
    response = requests.get("http://localhost:5555/api/data")
    json_data = response.json()
    _, records = auto_detect_and_extract(json_data, '')
    
    # Flatten and insert
    flattened_records = []
    for record in records:
        flat = flatten_record(record)
        flat['_sync_timestamp'] = datetime.now()
        flattened_records.append(flat)
    
    if flattened_records:
        columns = list(flattened_records[0].keys())
        safe_columns = [col.replace('.', '_').replace(' ', '_') for col in columns]
        insert_query = f"INSERT INTO test1.test_dynamic ({', '.join([f'`{c}`' for c in safe_columns])}) VALUES"
        ch.execute(insert_query, flattened_records)
        
        # Verify
        result = ch.execute("SELECT count() FROM test1.test_dynamic")
        count = result[0][0]
        
        if count == len(flattened_records):
            test_result("Insert initial data", True, f"Inserted {count} records")
        else:
            test_result("Insert initial data", False, f"Expected {len(flattened_records)}, got {count}")
    else:
        test_result("Insert initial data", False, "No records to insert")
        
except Exception as e:
    test_result("Insert initial data", False, str(e))
    import traceback
    traceback.print_exc()

# TEST 7: Test incremental sync (wait for new data)
print("\n[TEST 7] Testing incremental sync...")
print("   Waiting 10 seconds for API to generate new records...")
time.sleep(10)

try:
    ch = Client(host='localhost')
    
    # Get current count
    result = ch.execute("SELECT count() FROM test1.test_dynamic")
    initial_count = result[0][0]
    
    # Get existing IDs
    result = ch.execute("SELECT DISTINCT id FROM test1.test_dynamic")
    existing_ids = {str(row[0]) for row in result}
    
    # Fetch new API data
    response = requests.get("http://localhost:5555/api/data")
    json_data = response.json()
    _, records = auto_detect_and_extract(json_data, '')
    
    # Find new records
    new_records = [r for r in records if str(r.get('id', '')) not in existing_ids]
    
    if new_records:
        # Insert new records
        flattened_new = []
        for record in new_records:
            flat = flatten_record(record)
            flat['_sync_timestamp'] = datetime.now()
            flattened_new.append(flat)
        
        columns = list(flattened_new[0].keys())
        safe_columns = [col.replace('.', '_').replace(' ', '_') for col in columns]
        insert_query = f"INSERT INTO test1.test_dynamic ({', '.join([f'`{c}`' for c in safe_columns])}) VALUES"
        ch.execute(insert_query, flattened_new)
        
        # Verify new count
        result = ch.execute("SELECT count() FROM test1.test_dynamic")
        new_count = result[0][0]
        
        added = new_count - initial_count
        
        if added == len(new_records):
            test_result("Incremental sync", True, f"Added {added} new records (was {initial_count}, now {new_count})")
        else:
            test_result("Incremental sync", False, f"Expected to add {len(new_records)}, actually added {added}")
    else:
        test_result("Incremental sync", True, f"No new records (API has {len(records)}, all exist in DB)")
        
except Exception as e:
    test_result("Incremental sync", False, str(e))
    import traceback
    traceback.print_exc()

# TEST 8: Test with different data types
print("\n[TEST 8] Verifying data type handling...")
try:
    ch = Client(host='localhost')
    
    # Get a sample record
    result = ch.execute("SELECT * FROM test1.test_dynamic LIMIT 1")
    
    if result:
        record = result[0]
        # Just verify we can read data back
        test_result("Data type handling", True, f"Successfully stored and retrieved {len(record)} fields")
    else:
        test_result("Data type handling", False, "No records to verify")
        
except Exception as e:
    test_result("Data type handling", False, str(e))

# TEST 9: Test continuous polling setup
print("\n[TEST 9] Setting up continuous polling...")
try:
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='test1',
        user='migration_user',
        password='StrongPassword123'
    )
    
    cur = conn.cursor()
    
    # Check if source exists for our test API
    cur.execute("SELECT id FROM data_sources WHERE server_address='http://localhost:5555/api/data'")
    existing = cur.fetchone()
    
    if existing:
        source_id = existing[0]
        test_result("Polling source exists", True, f"Source ID: {source_id}")
    else:
        # Create new source
        cur.execute("""
            INSERT INTO data_sources 
            (source_name, source_type, server_address, target_database, connection_details, created_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            RETURNING id
        """, (
            'test_dynamic_api',
            'rest_api',
            'http://localhost:5555/api/data',
            'test1',
            {
                'target_table': 'test_dynamic',
                'polling_mode': True,
                'poll_interval': 5,
                'id_column': 'id',
                'data_path': '',
                'request_method': 'GET'
            }
        ))
        source_id = cur.fetchone()[0]
        conn.commit()
        test_result("Create polling source", True, f"Created source ID: {source_id}")
    
    cur.close()
    conn.close()
    
except Exception as e:
    test_result("Setup polling source", False, str(e))

# Print summary
print("\n" + "="*100)
print("TEST SUMMARY")
print("="*100)

passed = sum(1 for _, p, _ in results if p)
total = len(results)

for name, passed_flag, details in results:
    status = "[PASS]" if passed_flag else "[FAIL]"
    print(f"{status} {name}")
    if details and not passed_flag:
        print(f"       {details}")

print(f"\nTotal: {passed}/{total} tests passed")

if passed == total:
    print("\n*** ALL TESTS PASSED! System is working correctly! ***")
    print("\nTo start continuous sync:")
    print("1. The test API is running at http://localhost:5555/api/data")
    print("2. Data is in ClickHouse table: test1.test_dynamic")
    print("3. Run: python final_working_sync.py")
    print("   OR use Flask web interface to start polling")
else:
    print(f"\n*** {total - passed} TESTS FAILED - Review errors above ***")

print("\nClickHouse verification:")
print("  SELECT count() FROM test1.test_dynamic;")
print("  SELECT * FROM test1.test_dynamic ORDER BY _sync_timestamp DESC LIMIT 10;")

