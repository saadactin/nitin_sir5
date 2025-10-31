"""
Final working solution - will sync ANY accessible API
Checks multiple ports and uses the first working one
"""
import requests
import psycopg2
import os
import sys
from dotenv import load_dotenv
from clickhouse_driver import Client
from datetime import datetime
import time

load_dotenv()

print("="*80)
print("FINAL API TO CLICKHOUSE SYNC SOLUTION")
print("="*80)

# Check multiple possible API endpoints
possible_apis = [
    "http://localhost:4000/api/data",
    "http://localhost:4001/api/data",
    "http://localhost:4002/api/data",
    "http://localhost:4003/api/data",
    "http://localhost:4004/api/data",
    "http://localhost:4005/api/data",
    "http://localhost:4006/api/data",
    "http://localhost:4007/api/data",
]

working_api = None
print("\nStep 1: Finding working API...")
for api_url in possible_apis:
    try:
        response = requests.get(api_url, timeout=2)
        if response.status_code == 200:
            data = response.json()
            print(f"[OK] Found working API: {api_url}")
            working_api = api_url
            break
    except:
        pass

if not working_api:
    print("[FATAL] No working API found on ports 4000-4007!")
    print("Please start your API server first.")
    sys.exit(1)

# Import sync functions
from api_data_detector import auto_detect_and_extract
from api_sync import flatten_record, infer_clickhouse_type

# Get sample data
response = requests.get(working_api)
api_data = response.json()

detected_path, records = auto_detect_and_extract(api_data, '')
print(f"[OK] Auto-detected data path: '{detected_path}' with {len(records)} records")

if not records:
    print("[FATAL] No records in API response!")
    sys.exit(1)

# Use test1 database and crm table
TARGET_DB = "test1"
TARGET_TABLE = "crm"
ID_COLUMN = "id"

print(f"\nStep 2: Setting up ClickHouse table: {TARGET_DB}.{TARGET_TABLE}")

ch = Client(host='localhost')

# Get complete schema from all records
all_columns = {}
for record in records[:100]:
    flat = flatten_record(record)
    for key, value in flat.items():
        if key not in all_columns:
            all_columns[key] = infer_clickhouse_type(value)

all_columns['_sync_timestamp'] = 'DateTime64(3)'

print(f"[OK] Schema has {len(all_columns)} columns")

# Drop and recreate table
try:
    ch.execute(f"DROP TABLE IF EXISTS {TARGET_DB}.{TARGET_TABLE}")
    print("[OK] Dropped old table")
except:
    pass

# Create table
columns_def = []
for col_name, col_type in all_columns.items():
    safe_name = col_name.replace('.', '_').replace(' ', '_')
    columns_def.append(f"`{safe_name}` {col_type}")

create_query = f"""
CREATE TABLE IF NOT EXISTS {TARGET_DB}.{TARGET_TABLE} (
    {', '.join(columns_def)}
) ENGINE = MergeTree()
ORDER BY tuple()
"""

ch.execute(create_query)
print(f"[OK] Table created: {TARGET_DB}.{TARGET_TABLE}")

# Insert initial data
print("\nStep 3: Inserting initial data...")
flattened_records = []
for record in records:
    flat = flatten_record(record)
    flat['_sync_timestamp'] = datetime.now()
    flattened_records.append(flat)

if flattened_records:
    columns = list(flattened_records[0].keys())
    safe_columns = [col.replace('.', '_').replace(' ', '_') for col in columns]
    insert_query = f"INSERT INTO {TARGET_DB}.{TARGET_TABLE} ({', '.join([f'`{c}`' for c in safe_columns])}) VALUES"
    ch.execute(insert_query, flattened_records)
    print(f"[OK] Inserted {len(flattened_records)} records")

# Verify
result = ch.execute(f"SELECT count() FROM {TARGET_DB}.{TARGET_TABLE}")
print(f"[OK] Current row count: {result[0][0]}")

# Start continuous polling
print("\nStep 4: Starting continuous polling...")
print("="*80)
print(f"LIVE SYNC: {working_api} -> {TARGET_DB}.{TARGET_TABLE}")
print("Press Ctrl+C to stop")
print("="*80)

poll_count = 0
try:
    while True:
        poll_count += 1
        print(f"\n[Poll #{poll_count}] {datetime.now().strftime('%H:%M:%S')}")
        
        # Fetch new data
        response = requests.get(working_api, timeout=5)
        data = response.json()
        _, records = auto_detect_and_extract(data, '')
        
        # Get existing IDs
        result = ch.execute(f"SELECT DISTINCT {ID_COLUMN} FROM {TARGET_DB}.{TARGET_TABLE}")
        existing_ids = {str(row[0]) for row in result}
        
        # Find new records
        new_records = [r for r in records if str(r.get(ID_COLUMN, '')) not in existing_ids]
        
        if new_records:
            # Flatten and insert
            flattened = []
            for rec in new_records:
                flat = flatten_record(rec)
                flat['_sync_timestamp'] = datetime.now()
                flattened.append(flat)
            
            if flattened:
                ch.execute(insert_query, flattened)
                print(f"  [+] Inserted {len(flattened)} NEW records")
        else:
            print(f"  [ ] No new records (API has {len(records)}, DB has {len(existing_ids)})")
        
        # Show current count
        result = ch.execute(f"SELECT count() FROM {TARGET_DB}.{TARGET_TABLE}")
        print(f"  [Total] {result[0][0]} rows in ClickHouse")
        
        time.sleep(5)
        
except KeyboardInterrupt:
    print("\n\n[STOPPED] Polling stopped by user")
    result = ch.execute(f"SELECT count() FROM {TARGET_DB}.{TARGET_TABLE}")
    print(f"Final count: {result[0][0]} rows")

