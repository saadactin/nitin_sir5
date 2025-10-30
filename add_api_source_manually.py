"""
Manually add API source to database for testing
"""
import psycopg2
import json
from db_utils import load_pg_config

# Configuration
SOURCE_NAME = "CRM Stream"
API_URL = "http://localhost:3000/api/crm/stream"
TARGET_DATABASE = "test4"

connection_details = {
    "api_url": API_URL,
    "auth_type": "none",
    "auth_token": None,
    "basic_username": None,
    "basic_password": None,
    "apikey_header": None,
    "custom_headers": "",
    "request_method": "GET",
    "data_path": "",
    "target_table": "crm_stream",
    "is_sse": True
}

try:
    pg_conf = load_pg_config()
    conn = psycopg2.connect(
        dbname=pg_conf.get('database'),
        user=pg_conf.get('username'),
        password=pg_conf.get('password'),
        host=pg_conf.get('host'),
        port=int(pg_conf.get('port', 5432))
    )
    cursor = conn.cursor()
    
    # Insert the API source
    cursor.execute("""
        INSERT INTO data_sources 
        (source_name, source_type, server_address, username, password, 
         target_type, target_database, connection_details, is_active)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """, (
        SOURCE_NAME,
        'rest_api',
        API_URL,
        None,
        None,
        'clickhouse',
        TARGET_DATABASE,
        json.dumps(connection_details),
        True
    ))
    
    source_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"✅ Successfully added API source '{SOURCE_NAME}' with ID: {source_id}")
    print(f"   URL: {API_URL}")
    print(f"   Target: ClickHouse -> {TARGET_DATABASE}")
    print(f"   SSE: Enabled")
    print("\nNow refresh your homepage to see the new source!")
    
except psycopg2.IntegrityError as e:
    print(f"❌ Error: Source already exists or constraint violation")
    print(f"   Details: {e}")
except Exception as e:
    print(f"❌ Error adding source: {e}")
    import traceback
    traceback.print_exc()
