"""
Fix data_sources table to allow NULL password for API sources
"""
import psycopg2
from db_utils import load_pg_config

print("Fixing data_sources table structure...")

try:
    pg_conf = load_pg_config()
    conn = psycopg2.connect(
        dbname=pg_conf.get('database', 'metrics_sync_tables'),
        user=pg_conf.get('username'),
        password=pg_conf.get('password'),
        host=pg_conf.get('host'),
        port=int(pg_conf.get('port', 5432))
    )
    cur = conn.cursor()
    
    # Make password column nullable (API sources don't need passwords)
    cur.execute("ALTER TABLE data_sources ALTER COLUMN password DROP NOT NULL")
    conn.commit()
    
    print("✓ Password column is now nullable")
    
    cur.close()
    conn.close()
    
    print("\n✓ Table structure updated successfully!")
    
except Exception as e:
    print(f"✗ Error: {e}")
    exit(1)
