"""
Fix the crypto API to use circulating_supply as the ID column
since the 'id' field has duplicates
"""
import psycopg2
import json
from psycopg2.extras import Json

conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='test1',
    user='migration_user',
    password='StrongPassword123'
)

cur = conn.cursor()

# Find the crypto source (crm3)
cur.execute("""
    SELECT id, source_name, connection_details 
    FROM data_sources 
    WHERE server_address = 'http://localhost:4007/api/data'
""")

source = cur.fetchone()

if source:
    source_id, name, details = source
    
    # Change ID column to circulating_supply (which is unique and incrementing)
    details['id_column'] = 'circulating_supply'
    
    cur.execute("""
        UPDATE data_sources 
        SET connection_details = %s 
        WHERE id = %s
    """, (Json(details), source_id))
    
    conn.commit()
    print(f"✅ Updated source '{name}' (ID {source_id})")
    print(f"   Changed id_column from 'id' to 'circulating_supply'")
    print(f"\n📊 Now drop the table and restart sync:")
    print(f"   1. DROP TABLE test44.crm3;")
    print(f"   2. Restart Flask")
    print(f"   3. Click 'Sync Server' for {name}")
    print(f"\n✨ The system will now insert all records because circulating_supply is unique!")
else:
    print("❌ Source not found")

cur.close()
conn.close()

