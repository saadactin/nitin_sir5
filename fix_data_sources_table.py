"""
Fix data_sources table to allow NULL for database-specific fields
This is needed for API sources which don't have username/password
"""
import psycopg2
from db_utils import load_pg_config

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
    
    print("Altering data_sources table to allow NULL for database fields...")
    
    # Make username nullable
    cursor.execute("""
        ALTER TABLE data_sources 
        ALTER COLUMN username DROP NOT NULL
    """)
    
    print("✅ Made 'username' nullable")
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print("\n✅ Table structure updated successfully!")
    print("   Now API sources can be added without database credentials")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
