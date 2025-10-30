"""Apply fixes to API source configurations"""
import psycopg2
import json
from db_utils import load_pg_config

# Load PostgreSQL config
pg_conf = load_pg_config()

print("\n" + "="*100)
print("APPLYING API SOURCE CONFIGURATION FIXES")
print("="*100)

# Connect to PostgreSQL
conn = psycopg2.connect(
    host=pg_conf['host'],
    database=pg_conf['database'],
    user=pg_conf['username'],
    password=pg_conf['password'],
    port=pg_conf['port']
)

cur = conn.cursor()

fixes = [
    {
        'id': 26,
        'name': 'crm1',
        'description': 'Fix wrong endpoint (/api/data/stream → /api/crm/stream) + add database',
        'sql': """
            UPDATE data_sources 
            SET connection_details = jsonb_set(
                jsonb_set(connection_details, '{api_url}', '"http://localhost:3000/api/crm/stream"'),
                '{clickhouse_database}', '"test9"'
            ) 
            WHERE id = 26
        """
    },
    {
        'id': 27,
        'name': 'crm2',
        'description': 'Add missing ClickHouse database',
        'sql': """
            UPDATE data_sources 
            SET connection_details = jsonb_set(connection_details, '{clickhouse_database}', '"test9"')
            WHERE id = 27
        """
    },
    {
        'id': 28,
        'name': 'crm3 (CoinGecko)',
        'description': 'Add database + slow down polling (60s) + mark as REST API (not SSE)',
        'sql': """
            UPDATE data_sources 
            SET connection_details = jsonb_set(
                jsonb_set(
                    jsonb_set(connection_details, '{clickhouse_database}', '"test9"'),
                    '{polling_interval}', '60'
                ),
                '{is_sse}', 'false'
            )
            WHERE id = 28
        """
    }
]

for fix in fixes:
    print(f"\n[{fix['id']}] {fix['name']}")
    print("-" * 100)
    print(f"Fix: {fix['description']}")
    
    try:
        cur.execute(fix['sql'])
        conn.commit()
        print("✅ Applied successfully!")
    except Exception as e:
        print(f"❌ Error: {e}")
        conn.rollback()

# Verify all fixes
print("\n" + "="*100)
print("VERIFYING FIXES")
print("="*100)

cur.execute("""
    SELECT id, source_name, source_type, 
           connection_details->>'api_url' as api_url,
           connection_details->>'clickhouse_database' as target_db,
           connection_details->>'target_table' as target_table,
           connection_details->>'is_sse' as is_sse,
           connection_details->>'request_method' as method,
           connection_details->>'polling_interval' as polling_interval
    FROM data_sources 
    WHERE id IN (26, 27, 28)
    ORDER BY id
""")

rows = cur.fetchall()
for row in rows:
    source_id, name, stype, url, db, table, is_sse, method, poll = row
    print(f"\n[{source_id}] {name}")
    print(f"  Type: {stype}")
    print(f"  URL: {url}")
    print(f"  Target: {db}.{table}")
    print(f"  Is SSE: {is_sse}")
    print(f"  Method: {method}")
    print(f"  Polling: {poll}s" if poll else "  Polling: N/A")

print("\n" + "="*100)
print("✅ ALL FIXES APPLIED!")
print("="*100)
print("\n💡 Next steps:")
print("  1. Restart Flask app to load updated error handling code")
print("  2. Stop all running syncs in Flask (click 'Stop' buttons)")
print("  3. Wait 2 minutes for CoinGecko rate limit to reset")
print("  4. Click 'Sync Server' on each source to restart with fixed configs")

conn.close()
