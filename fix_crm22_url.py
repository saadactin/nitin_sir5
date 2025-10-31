"""Fix crm22 source to use correct port 4010 and path"""
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

# Find crm22
cur.execute("""
    SELECT id, source_name, server_address, connection_details 
    FROM data_sources 
    WHERE source_name = 'crm22'
""")

source = cur.fetchone()

if source:
    source_id, name, old_url, details = source
    
    print(f"Found source: {name} (ID {source_id})")
    print(f"Old URL: {old_url}")
    print(f"Old Token URL: {details.get('oauth_token_url')}")
    
    # Update URLs
    new_api_url = "http://localhost:4010/data"
    new_token_url = "http://localhost:4010/pass"
    
    details['oauth_token_url'] = new_token_url
    
    cur.execute("""
        UPDATE data_sources 
        SET server_address = %s, connection_details = %s
        WHERE id = %s
    """, (new_api_url, Json(details), source_id))
    
    conn.commit()
    
    print(f"\n✅ Updated!")
    print(f"New API URL: {new_api_url}")
    print(f"New Token URL: {new_token_url}")
    print(f"\n🔄 Restart Flask to apply changes")
    print(f"The source will show 'Online' and work correctly!")
else:
    print("Source 'crm22' not found")

cur.close()
conn.close()

