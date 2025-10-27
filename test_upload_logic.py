"""
Quick test script to verify file upload logic
Run this to test if CSV/Excel/TXT reading works
"""
import pandas as pd
import io

# Test 1: CSV file
print("=" * 60)
print("TEST 1: CSV File")
print("=" * 60)
csv_data = """id,name,email
1,John,john@example.com
2,Jane,jane@example.com"""

try:
    df = pd.read_csv(io.StringIO(csv_data))
    print(f"✅ CSV read successfully: {len(df)} rows, {len(df.columns)} columns")
    print(df)
except Exception as e:
    print(f"❌ CSV failed: {e}")

# Test 2: Tab-delimited text file
print("\n" + "=" * 60)
print("TEST 2: Tab-Delimited Text File")
print("=" * 60)
txt_data = """id\tname\temail
1\tJohn\tjohn@example.com
2\tJane\tjane@example.com"""

try:
    df = pd.read_csv(io.StringIO(txt_data), sep='\t')
    print(f"✅ TXT (tab) read successfully: {len(df)} rows, {len(df.columns)} columns")
    print(df)
except Exception as e:
    print(f"❌ TXT (tab) failed: {e}")

# Test 3: Check if openpyxl is installed
print("\n" + "=" * 60)
print("TEST 3: Excel Support Check")
print("=" * 60)
try:
    import openpyxl
    print(f"✅ openpyxl is installed (version: {openpyxl.__version__})")
except ImportError:
    print("❌ openpyxl is NOT installed - Excel files will fail!")
    print("   Run: pip install openpyxl")

# Test 4: Check PostgreSQL connection
print("\n" + "=" * 60)
print("TEST 4: PostgreSQL Connection")
print("=" * 60)
try:
    import psycopg2
    from sqlalchemy import create_engine
    import os
    
    # Try to get connection info
    pg_host = os.getenv('POSTGRES_HOST', 'localhost')
    pg_port = os.getenv('POSTGRES_PORT', '5432')
    pg_db = os.getenv('POSTGRES_DB', 'test1')
    pg_user = os.getenv('POSTGRES_USER', 'migration_user')
    pg_password = os.getenv('POSTGRES_PASSWORD', 'StrongPassword123')
    
    print(f"Connecting to: {pg_user}@{pg_host}:{pg_port}/{pg_db}")
    
    engine = create_engine(
        f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    )
    
    # Try to connect
    with engine.connect() as conn:
        result = conn.execute("SELECT version()")
        version = result.fetchone()[0]
        print(f"✅ PostgreSQL connection successful!")
        print(f"   Version: {version[:50]}...")
        
except Exception as e:
    print(f"❌ PostgreSQL connection failed: {e}")

# Test 5: Test table creation
print("\n" + "=" * 60)
print("TEST 5: Test Table Creation")
print("=" * 60)
try:
    from sqlalchemy import create_engine
    import os
    
    pg_host = os.getenv('POSTGRES_HOST', 'localhost')
    pg_port = os.getenv('POSTGRES_PORT', '5432')
    pg_db = os.getenv('POSTGRES_DB', 'test1')
    pg_user = os.getenv('POSTGRES_USER', 'migration_user')
    pg_password = os.getenv('POSTGRES_PASSWORD', 'StrongPassword123')
    
    engine = create_engine(
        f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    )
    
    # Create test dataframe
    test_df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Test1', 'Test2', 'Test3'],
        'value': [100, 200, 300]
    })
    
    # Try to write to database
    test_df.to_sql('test_upload_table', engine, schema='public', if_exists='replace', index=False)
    
    print(f"✅ Table creation successful!")
    print(f"   Created table: public.test_upload_table with {len(test_df)} rows")
    
    # Try to read it back
    read_df = pd.read_sql('SELECT * FROM public.test_upload_table', engine)
    print(f"✅ Table read successful: {len(read_df)} rows returned")
    
    # Clean up
    with engine.connect() as conn:
        conn.execute("DROP TABLE IF EXISTS public.test_upload_table")
    print(f"✅ Cleanup successful")
    
except Exception as e:
    import traceback
    print(f"❌ Table creation failed: {e}")
    print(traceback.format_exc())

print("\n" + "=" * 60)
print("TESTS COMPLETE")
print("=" * 60)
