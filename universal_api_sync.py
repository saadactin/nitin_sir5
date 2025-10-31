"""
Universal API to ClickHouse sync - handles ANY JSON structure
Works with static or dynamic data, nested objects, any data types
"""
import requests
from clickhouse_driver import Client
from datetime import datetime
import time
import sys

def infer_clickhouse_type(value):
    """Infer ClickHouse data type from Python value"""
    if value is None:
        return "Nullable(String)"
    elif isinstance(value, bool):
        return "Bool"
    elif isinstance(value, int):
        if value > 2147483647 or value < -2147483648:
            return "Int64"
        return "Int32"
    elif isinstance(value, float):
        return "Float64"
    elif isinstance(value, str):
        if len(value) > 1000:
            return "String"
        return "String"
    elif isinstance(value, (list, dict)):
        return "String"  # Store as JSON string
    else:
        return "String"

def flatten_record(obj, parent_key='', sep='_'):
    """Flatten nested JSON into single-level dict"""
    items = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_record(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                # Convert lists to JSON string
                import json
                items.append((new_key, json.dumps(v)))
            else:
                items.append((new_key, v))
    else:
        items.append((parent_key, obj))
    return dict(items)

def auto_detect_data_path(json_data):
    """Auto-detect where the array of records is in JSON"""
    if isinstance(json_data, list):
        return '', json_data
    
    if isinstance(json_data, dict):
        for key in ['data', 'results', 'items', 'records']:
            if key in json_data and isinstance(json_data[key], list):
                return key, json_data[key]
        
        # Search recursively
        for key, value in json_data.items():
            if isinstance(value, list) and len(value) > 0:
                return key, value
    
    return '', []

def create_table_from_records(ch, database, table, records):
    """Create ClickHouse table from records"""
    # Flatten all records to get complete schema
    all_columns = {}
    for record in records[:100]:  # Sample first 100
        flat = flatten_record(record)
        for key, value in flat.items():
            safe_key = key.replace('.', '_').replace(' ', '_').replace('-', '_')
            if safe_key not in all_columns:
                all_columns[safe_key] = infer_clickhouse_type(value)
    
    # Add metadata
    all_columns['_sync_timestamp'] = 'DateTime64(3)'
    
    # Create table
    ch.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    ch.execute(f"DROP TABLE IF EXISTS {database}.{table}")
    
    columns_def = [f"`{col}` {dtype}" for col, dtype in all_columns.items()]
    
    create_query = f"""
    CREATE TABLE {database}.{table} (
        {', '.join(columns_def)}
    ) ENGINE = MergeTree()
    ORDER BY tuple()
    """
    
    ch.execute(create_query)
    print(f"[OK] Created table {database}.{table} with {len(all_columns)} columns")
    
    return all_columns

def insert_records(ch, database, table, records):
    """Insert records into ClickHouse"""
    if not records:
        return 0
    
    flattened = []
    for record in records:
        flat = flatten_record(record)
        # Add timestamp
        flat['_sync_timestamp'] = datetime.now()
        # Ensure safe column names
        safe_flat = {}
        for key, value in flat.items():
            safe_key = key.replace('.', '_').replace(' ', '_').replace('-', '_')
            safe_flat[safe_key] = value
        flattened.append(safe_flat)
    
    if flattened:
        columns = list(flattened[0].keys())
        insert_query = f"INSERT INTO {database}.{table} ({', '.join([f'`{c}`' for c in columns])}) VALUES"
        ch.execute(insert_query, flattened)
        return len(flattened)
    
    return 0

def sync_api_continuous(api_url, database, table, id_column='id', poll_interval=5):
    """Continuously sync API to ClickHouse"""
    ch = Client(host='localhost')
    
    print(f"="*100)
    print(f"UNIVERSAL API SYNC: {api_url} -> {database}.{table}")
    print(f"="*100)
    
    # Initial sync
    print("\n[INIT] Fetching initial data...")
    response = requests.get(api_url, timeout=10)
    json_data = response.json()
    
    path, records = auto_detect_data_path(json_data)
    print(f"[OK] Detected data path: '{path}' with {len(records)} records")
    
    if not records:
        print("[ERROR] No records found!")
        return
    
    # Create table
    print("\n[SETUP] Creating table...")
    schema = create_table_from_records(ch, database, table, records)
    
    # Insert initial data
    print("\n[SYNC] Inserting initial data...")
    count = insert_records(ch, database, table, records)
    print(f"[OK] Inserted {count} records")
    
    # Start polling
    print(f"\n[POLL] Starting continuous sync (every {poll_interval}s)")
    print("="*100)
    print("Press Ctrl+C to stop\n")
    
    poll_count = 0
    try:
        while True:
            poll_count += 1
            time.sleep(poll_interval)
            
            print(f"[Poll #{poll_count}] {datetime.now().strftime('%H:%M:%S')}", end=' ')
            
            # Fetch new data
            response = requests.get(api_url, timeout=10)
            json_data = response.json()
            _, new_records = auto_detect_data_path(json_data)
            
            # Get existing IDs
            result = ch.execute(f"SELECT DISTINCT `{id_column}` FROM {database}.{table}")
            existing_ids = {str(row[0]) for row in result}
            
            # Find new records
            to_insert = [r for r in new_records if str(r.get(id_column, '')) not in existing_ids]
            
            if to_insert:
                count = insert_records(ch, database, table, to_insert)
                print(f"[+{count} NEW]", end='')
            else:
                print(f"[No new]", end='')
            
            # Show total
            result = ch.execute(f"SELECT count() FROM {database}.{table}")
            total = result[0][0]
            print(f" Total: {total}")
            
    except KeyboardInterrupt:
        print("\n\n[STOPPED] Sync stopped by user")
        result = ch.execute(f"SELECT count() FROM {database}.{table}")
        print(f"Final count: {result[0][0]} rows")

if __name__ == '__main__':
    # Check for test API
    test_api = "http://localhost:5555/api/data"
    
    print("Checking for test API...")
    try:
        response = requests.get(test_api, timeout=2)
        if response.status_code == 200:
            print(f"[OK] Found test API at {test_api}")
            sync_api_continuous(test_api, 'test1', 'test_dynamic', 'id', 5)
        else:
            print("[ERROR] Test API not responding")
    except:
        print("[ERROR] Test API not running!")
        print("Start it with: python test_api_server.py")
        sys.exit(1)

