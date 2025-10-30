"""
Check API Source Configurations in PostgreSQL
"""
import psycopg2
import json
from db_utils import load_pg_config

# Load PostgreSQL config
pg_conf = load_pg_config()

print("\n" + "="*100)
print("CONNECTING TO POSTGRESQL")
print("="*100)
print(f"Host: {pg_conf['host']}")
print(f"Database: {pg_conf['database']}")
print(f"User: {pg_conf['username']}")

# Connect to PostgreSQL
conn = psycopg2.connect(
    host=pg_conf['host'],
    database=pg_conf['database'],
    user=pg_conf['username'],
    password=pg_conf['password'],
    port=pg_conf['port']
)

cur = conn.cursor()

# Get all API sources
cur.execute("""
    SELECT id, source_name, source_type, is_active, connection_details 
    FROM data_sources 
    ORDER BY id
""")

rows = cur.fetchall()

print("\n" + "="*100)
print(f"FOUND {len(rows)} API SOURCES")
print("="*100)

for row in rows:
    source_id, source_name, source_type, is_active, details = row
    
    print(f"\n[ID: {source_id}] {source_name}")
    print("-" * 100)
    print(f"Type: {source_type}")
    print(f"Active: {is_active}")
    print(f"\nConfiguration:")
    
    # Handle null connection_details
    if details is None:
        print("  null (No configuration)")
        print("\n⚠️  SKIPPING: This is not an API source (connection_details is null)")
        continue
    
    print(json.dumps(details, indent=2))
    
    # Analyze issues
    issues = []
    
    api_url = details.get('api_url', '')
    is_sse = details.get('is_sse', False)
    request_method = details.get('request_method', 'GET')
    polling_interval = details.get('polling_interval', 0)
    
    if source_type == 'sse' or is_sse:
        # Check for SSE issues
        if request_method == 'POST':
            issues.append("⚠️  WRONG METHOD: SSE endpoints should use GET, not POST")
        
        if '/api/data/stream' in api_url:
            issues.append("⚠️  WRONG ENDPOINT: Should be '/api/crm/stream' not '/api/data/stream'")
        
        if 'coingecko.com' in api_url:
            issues.append("⚠️  COINGECKO: Free tier rate limit (10-30 req/min). Reduce polling_interval.")
    
    if source_type == 'api' or (source_type == 'rest_api' and not is_sse):
        # Check for REST API issues
        if 'coingecko.com' in api_url:
            issues.append("⚠️  COINGECKO: Free tier rate limit (10-30 req/min)")
            if polling_interval < 30:
                issues.append(f"⚠️  POLLING TOO FAST: {polling_interval}s interval = {60/polling_interval if polling_interval > 0 else 'infinite'} req/min. Should be 30s+ for free tier.")
        
        if 'coins/market' in api_url and 'api/v3' not in api_url:
            issues.append("⚠️  WRONG COINGECKO PATH: Should be '/api/v3/coins/markets'")
    
    # Check target database
    clickhouse_database = details.get('clickhouse_database') or details.get('target_database')
    table_name = details.get('table_name') or details.get('target_table')
    
    if not clickhouse_database:
        issues.append("❌ MISSING: No target ClickHouse database specified")
    
    if not table_name:
        issues.append("❌ MISSING: No target table name specified")
    
    # Print issues
    if issues:
        print("\n🚨 ISSUES DETECTED:")
        for issue in issues:
            print(f"  {issue}")
        
        # Provide SQL fixes
        print("\n💡 SQL FIX:")
        
        if source_type == 'sse' and 'POST' in str(issues):
            print(f"  UPDATE data_sources SET connection_details = jsonb_set(connection_details, '{{request_method}}', '\"GET\"') WHERE id = {source_id};")
        
        if '/api/data/stream' in api_url:
            fixed_url = api_url.replace('/api/data/stream', '/api/crm/stream')
            print(f"  UPDATE data_sources SET connection_details = jsonb_set(connection_details, '{{api_url}}', '\"{fixed_url}\"') WHERE id = {source_id};")
        
        if 'coingecko' in str(issues).lower() and 'polling' in str(issues).lower():
            print(f"  UPDATE data_sources SET connection_details = jsonb_set(connection_details, '{{polling_interval}}', '60') WHERE id = {source_id};")
    else:
        print("\n✅ Configuration looks good!")

print("\n" + "="*100)
print("SUMMARY")
print("="*100)
print(f"Total sources: {len(rows)}")
print(f"Active sources: {sum(1 for r in rows if r[3])}")
print(f"SSE sources: {sum(1 for r in rows if r[2] == 'sse')}")
print(f"API sources: {sum(1 for r in rows if r[2] == 'api')}")

conn.close()
