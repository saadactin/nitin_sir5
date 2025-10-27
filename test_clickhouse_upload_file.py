"""
Test script to upload a sample CSV file to ClickHouse.
This will help verify the upload logic before using the web UI.
"""
import os
import sys
import pandas as pd
from io import StringIO

# Load .env
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("[OK] Loaded .env file\n")
except:
    print("[WARN] python-dotenv not available\n")

# Import clickhouse-driver
try:
    from clickhouse_driver import Client
except ImportError:
    print("[ERROR] clickhouse-driver not installed!")
    sys.exit(1)

# Get connection details
ch_host = os.environ.get('CLICKHOUSE_HOST')
ch_port = os.environ.get('CLICKHOUSE_PORT')
ch_user = os.environ.get('CLICKHOUSE_USER')
ch_password = os.environ.get('CLICKHOUSE_PASSWORD', '')

if not ch_host or not ch_port or not ch_user:
    print("[ERROR] Missing CLICKHOUSE_* environment variables")
    sys.exit(1)

print("="*60)
print("ClickHouse Upload Test")
print("="*60)
print(f"Target: {ch_host}:{ch_port}")
print(f"User: {ch_user}")
print(f"Database: saadtest")
print("="*60 + "\n")

# Create sample CSV data
sample_csv = """id,name,value,description
1,Product A,100,First product
2,Product B,200,Second product
3,Product C,300,Third product"""

print("Sample CSV data:")
print(sample_csv)
print()

# Parse CSV with pandas
df = pd.read_csv(StringIO(sample_csv))
print(f"Parsed {len(df)} rows, {len(df.columns)} columns")
print(f"Columns: {list(df.columns)}")
print()

# Connect to ClickHouse
try:
    client = Client(host=ch_host, port=int(ch_port), user=ch_user, password=ch_password)
    print("[OK] Connected to ClickHouse\n")
except Exception as e:
    print(f"[ERROR] Failed to connect: {e}")
    sys.exit(1)

# Create database if not exists
db_name = 'saadtest'
table_name = 'test_upload_sample'

print(f"Creating database '{db_name}' if not exists...")
try:
    client.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
    print("[OK] Database ready\n")
except Exception as e:
    print(f"[ERROR] Failed to create database: {e}")
    sys.exit(1)

# Prepare table schema - all columns as Nullable(String)
cols = list(df.columns)
col_defs = ", ".join([f"`{c}` Nullable(String)" for c in cols])
create_sql = f"CREATE TABLE IF NOT EXISTS `{db_name}`.`{table_name}` ({col_defs}) ENGINE = MergeTree() ORDER BY tuple()"

print(f"Creating table '{table_name}'...")
print(f"Schema: {col_defs}")
try:
    client.execute(create_sql)
    print("[OK] Table created\n")
except Exception as e:
    print(f"[ERROR] Failed to create table: {e}")
    sys.exit(1)

# Prepare rows - convert all values to strings
rows_to_insert = []
for row in df.values.tolist():
    row_tuple = tuple((None if pd.isna(v) else str(v)) for v in row)
    rows_to_insert.append(row_tuple)

print(f"Inserting {len(rows_to_insert)} rows...")
insert_sql = f"INSERT INTO `{db_name}`.`{table_name}` ({', '.join(['`'+c+'`' for c in cols])}) VALUES"

try:
    client.execute(insert_sql, rows_to_insert)
    print("[OK] Rows inserted\n")
except Exception as e:
    print(f"[ERROR] Failed to insert rows: {e}")
    sys.exit(1)

# Verify data
print("Verifying inserted data...")
try:
    result = client.execute(f"SELECT count() FROM `{db_name}`.`{table_name}`")
    count = result[0][0] if result and isinstance(result[0], tuple) else result[0]
    print(f"[OK] Row count: {count}")
    
    print("\nSample query (LIMIT 5):")
    result = client.execute(f"SELECT * FROM `{db_name}`.`{table_name}` LIMIT 5")
    for row in result:
        print(f"  {row}")
    
    print("\n" + "="*60)
    print("[SUCCESS] ClickHouse upload test completed!")
    print("="*60)
    
except Exception as e:
    print(f"[ERROR] Failed to verify data: {e}")
    sys.exit(1)
