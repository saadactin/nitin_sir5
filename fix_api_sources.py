"""
Diagnose and fix API source configuration issues
"""
import psycopg2
import json
from db_utils import load_postgres_config

def diagnose_api_sources():
    print("=" * 80)
    print("🔍 API SOURCE CONFIGURATION DIAGNOSTIC")
    print("=" * 80)
    
    # Connect to PostgreSQL
    try:
        pg_config = load_postgres_config()
        conn = psycopg2.connect(**pg_config)
        cur = conn.cursor()
        print("\n✅ Connected to PostgreSQL\n")
    except Exception as e:
        print(f"\n❌ Failed to connect to PostgreSQL: {e}")
        return
    
    # Get all API sources
    cur.execute("""
        SELECT id, source_name, source_type, connection_details 
        FROM data_sources 
        WHERE source_type = 'api'
        ORDER BY id
    """)
    
    sources = cur.fetchall()
    
    if not sources:
        print("⚠️  No API sources found in database")
        print("\nTo add an API source:")
        print("1. Go to: http://localhost:5001/add-api-source")
        print("2. Fill in the form and click 'Add Source'")
        return
    
    print(f"📊 Found {len(sources)} API source(s):\n")
    print("-" * 80)
    
    issues_found = []
    
    for source_id, name, source_type, conn_details_json in sources:
        conn_details = json.loads(conn_details_json)
        
        api_url = conn_details.get('api_url', 'NOT SET')
        request_method = conn_details.get('request_method', 'GET')
        stream_type = conn_details.get('stream_type', 'rest')
        target_db = conn_details.get('target_database', 'NOT SET')
        target_table = conn_details.get('target_table', 'NOT SET')
        
        print(f"\n🔹 Source #{source_id}: {name}")
        print(f"   URL: {api_url}")
        print(f"   Method: {request_method}")
        print(f"   Type: {stream_type}")
        print(f"   Target: {target_db}.{target_table}")
        
        # Check for common issues
        
        # Issue 1: Wrong HTTP method for SSE
        if stream_type == 'sse' and request_method == 'POST':
            issues_found.append({
                'source_id': source_id,
                'name': name,
                'issue': 'Wrong HTTP method for SSE',
                'fix': 'Change from POST to GET',
                'current': f"Method: {request_method}",
                'should_be': "Method: GET"
            })
            print(f"   ❌ ERROR: SSE streams require GET method, but {request_method} is set!")
        
        # Issue 2: CoinGecko rate limit
        if 'coingecko.com' in api_url.lower():
            print(f"   ⚠️  WARNING: CoinGecko has rate limits")
            print(f"      Free tier: 10-30 requests/minute")
            print(f"      Your sync polls every 10 seconds = 6 requests/minute")
            issues_found.append({
                'source_id': source_id,
                'name': name,
                'issue': 'CoinGecko rate limit',
                'fix': 'Wait for rate limit to reset, or get API key',
                'current': 'Polling every 10 seconds',
                'should_be': 'Consider increasing poll_interval or getting paid API key'
            })
        
        # Issue 3: Wrong URL for localhost
        if '/api/data/stream' in api_url and 'localhost' in api_url:
            print(f"   ⚠️  WARNING: /api/data/stream might not exist")
            print(f"      Mock server uses: /api/crm/stream")
            issues_found.append({
                'source_id': source_id,
                'name': name,
                'issue': 'Possible wrong endpoint',
                'fix': 'Check if endpoint should be /api/crm/stream',
                'current': api_url,
                'should_be': api_url.replace('/api/data/stream', '/api/crm/stream')
            })
        
        # Issue 4: Missing target database/table
        if target_db == 'NOT SET' or target_table == 'NOT SET':
            issues_found.append({
                'source_id': source_id,
                'name': name,
                'issue': 'Missing target database or table',
                'fix': 'Edit source to add target database/table',
                'current': f"{target_db}.{target_table}",
                'should_be': "Valid database and table name"
            })
            print(f"   ❌ ERROR: Missing target database or table!")
        
        print("-" * 80)
    
    # Summary
    print(f"\n" + "=" * 80)
    print(f"📋 SUMMARY")
    print("=" * 80)
    
    if not issues_found:
        print("\n✅ All API sources look good!")
        print("\nIf you're still seeing errors:")
        print("1. CoinGecko: Wait for rate limit to reset (usually 1 minute)")
        print("2. Check Flask logs for specific error messages")
        print("3. Verify APIs are accessible: curl <api_url>")
    else:
        print(f"\n⚠️  Found {len(issues_found)} issue(s):\n")
        
        for i, issue in enumerate(issues_found, 1):
            print(f"{i}. Source: {issue['name']} (ID: {issue['source_id']})")
            print(f"   Issue: {issue['issue']}")
            print(f"   Current: {issue['current']}")
            print(f"   Fix: {issue['fix']}")
            if 'should_be' in issue:
                print(f"   Should be: {issue['should_be']}")
            print()
        
        print("=" * 80)
        print("🔧 HOW TO FIX:")
        print("=" * 80)
        
        # Group by fix type
        method_fixes = [i for i in issues_found if 'Wrong HTTP method' in i['issue']]
        rate_limit_issues = [i for i in issues_found if 'rate limit' in i['issue']]
        url_fixes = [i for i in issues_found if 'wrong endpoint' in i['issue']]
        
        if method_fixes:
            print("\n1️⃣  Fix HTTP Method (POST → GET for SSE):")
            print("   Go to database and update:")
            for issue in method_fixes:
                print(f"   UPDATE data_sources SET connection_details = ")
                print(f"     jsonb_set(connection_details, '{{request_method}}', '\"GET\"')")
                print(f"     WHERE id = {issue['source_id']};")
        
        if rate_limit_issues:
            print("\n2️⃣  Handle CoinGecko Rate Limits:")
            print("   Option A: Wait 1-2 minutes for rate limit to reset")
            print("   Option B: Get CoinGecko API key (paid plans have higher limits)")
            print("   Option C: Increase poll interval to 60 seconds (edit api_sync.py)")
        
        if url_fixes:
            print("\n3️⃣  Fix Endpoint URLs:")
            for issue in url_fixes:
                if 'should_be' in issue:
                    print(f"   Change: {issue['current']}")
                    print(f"   To: {issue['should_be']}")
        
        print("\n4️⃣  After fixing, restart Flask:")
        print("   - Stop Flask (Ctrl+C)")
        print("   - Run: python app.py")
        print("   - Click 'Sync Server' button again")
    
    print("\n" + "=" * 80)
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    diagnose_api_sources()
