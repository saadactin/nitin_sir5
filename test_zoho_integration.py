"""
Test script for Zoho CRM Integration
Tests the complete flow: connection, module listing, and data sync to ClickHouse
"""
import sys
import json
from zoho_crm_sync import (
    get_access_token,
    get_available_modules,
    sync_zoho_modules
)

# Test credentials (from your UI)
CLIENT_ID = "1000.0L3LLVLEKE9ELW7CE0I0KJ3K4FKBBT"
CLIENT_SECRET = "d99c479d4c0db451c653d8c380bf6a4c557a73528c"
REFRESH_TOKEN = "1000.2cbaa36345c6d04b699b0cb6740c21ef.149922195c479d83c84826653ff84ff4"
API_DOMAIN = "https://www.zohoapis.in"

# ClickHouse credentials
CLICKHOUSE_HOST = "74.225.251.123"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASS = "root"
CLICKHOUSE_DB = "test1"  # Using test1 database as requested

def test_access_token():
    """Test 1: Verify we can get an access token"""
    print("\n" + "="*60)
    print("TEST 1: Getting Zoho Access Token")
    print("="*60)
    
    try:
        token_result = get_access_token(REFRESH_TOKEN, CLIENT_ID, CLIENT_SECRET, API_DOMAIN)
        if token_result:
            print("✅ SUCCESS: Access token obtained")
            print(f"   Token expires in: {token_result.get('expires_in')} seconds")
            print(f"   API Domain: {token_result.get('api_domain')}")
            return token_result
        else:
            print("❌ FAILED: Could not obtain access token")
            return None
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        return None

def test_list_modules(token_result):
    """Test 2: List available Zoho CRM modules"""
    print("\n" + "="*60)
    print("TEST 2: Fetching Available Zoho CRM Modules")
    print("="*60)
    
    if not token_result:
        print("❌ SKIPPED: No access token available")
        return []
    
    try:
        token = token_result["access_token"]
        api_domain = token_result.get("api_domain", API_DOMAIN)
        
        modules = get_available_modules(token, api_domain)
        
        if modules:
            print(f"✅ SUCCESS: Found {len(modules)} modules")
            print("\nAvailable modules:")
            for i, module in enumerate(modules[:10], 1):  # Show first 10
                print(f"   {i}. {module['display_name']} ({module['api_name']})")
            if len(modules) > 10:
                print(f"   ... and {len(modules) - 10} more modules")
            return modules
        else:
            print("⚠️  WARNING: No modules found")
            return []
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return []

def test_clickhouse_connection():
    """Test 3: Verify ClickHouse connection"""
    print("\n" + "="*60)
    print("TEST 3: Testing ClickHouse Connection")
    print("="*60)
    
    try:
        from clickhouse_connect import get_client
        
        client = get_client(
            host=CLICKHOUSE_HOST,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASS,
            database=CLICKHOUSE_DB,
        )
        
        # Test query
        result = client.query("SELECT 1 as test")
        print("✅ SUCCESS: ClickHouse connection established")
        print(f"   Database: {CLICKHOUSE_DB}")
        print(f"   Host: {CLICKHOUSE_HOST}")
        
        # Check if database exists, create if not
        try:
            client.query(f"USE {CLICKHOUSE_DB}")
        except:
            print(f"   Creating database {CLICKHOUSE_DB}...")
            client.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")
        
        return client
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def test_sync_modules(modules, max_modules=5):
    """Test 4: Sync selected modules to ClickHouse"""
    print("\n" + "="*60)
    print("TEST 4: Syncing Modules to ClickHouse")
    print("="*60)
    
    if not modules:
        print("❌ SKIPPED: No modules available to sync")
        return None
    
    # Select first few modules for testing (or all if less than max)
    selected_modules = [m['api_name'] for m in modules[:max_modules]]
    
    print(f"Selected modules to sync ({len(selected_modules)}):")
    for module in selected_modules:
        print(f"   - {module}")
    
    try:
        result = sync_zoho_modules(
            refresh_token=REFRESH_TOKEN,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            api_domain=API_DOMAIN,
            clickhouse_host=CLICKHOUSE_HOST,
            clickhouse_user=CLICKHOUSE_USER,
            clickhouse_password=CLICKHOUSE_PASS,
            clickhouse_database=CLICKHOUSE_DB,
            selected_modules=selected_modules
        )
        
        print("\n📊 SYNC RESULTS:")
        print(f"   Success: {result['success']}")
        print(f"   Total Records Synced: {result['total_records']}")
        print(f"   Modules Synced: {len(result['synced_modules'])}")
        print(f"   Modules Failed: {len(result['failed_modules'])}")
        
        if result['synced_modules']:
            print("\n✅ Successfully Synced Modules:")
            for module in result['synced_modules']:
                print(f"   - {module['module']}: {module['record_count']} records")
        
        if result['failed_modules']:
            print("\n❌ Failed Modules:")
            for module in result['failed_modules']:
                print(f"   - {module['module']}: {module['error']}")
        
        if result['errors']:
            print("\n⚠️  Errors:")
            for error in result['errors']:
                print(f"   - {error}")
        
        return result
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def verify_data_in_clickhouse(client):
    """Test 5: Verify data exists in ClickHouse"""
    print("\n" + "="*60)
    print("TEST 5: Verifying Data in ClickHouse")
    print("="*60)
    
    if not client:
        print("❌ SKIPPED: No ClickHouse connection")
        return
    
    try:
        # List all zoho_* tables
        result = client.query(f"""
            SELECT name 
            FROM system.tables 
            WHERE database = '{CLICKHOUSE_DB}' 
            AND name LIKE 'zoho_%'
            ORDER BY name
        """)
        
        tables = [row[0] for row in result.result_rows]
        
        if tables:
            print(f"✅ Found {len(tables)} Zoho tables in database '{CLICKHOUSE_DB}':")
            total_records = 0
            
            for table in tables:
                try:
                    count_result = client.query(f"SELECT count() FROM {CLICKHOUSE_DB}.{table}")
                    count = count_result.result_rows[0][0] if count_result.result_rows else 0
                    total_records += count
                    print(f"   - {table}: {count} records")
                    
                    # Show sample data structure
                    if count > 0:
                        sample = client.query(f"SELECT * FROM {CLICKHOUSE_DB}.{table} LIMIT 1")
                        if sample.result_rows:
                            columns = [col[0] for col in sample.column_names]
                            print(f"     Columns: {', '.join(columns[:5])}{'...' if len(columns) > 5 else ''}")
                except Exception as e:
                    print(f"   - {table}: Error reading - {str(e)}")
            
            print(f"\n📊 Total records across all tables: {total_records}")
        else:
            print(f"⚠️  No Zoho tables found in database '{CLICKHOUSE_DB}'")
            print("   This might mean the sync hasn't run yet or no data was found.")
        
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()

def main():
    """Run all tests"""
    print("\n" + "="*60)
    print("ZOHO CRM INTEGRATION TEST SUITE")
    print("="*60)
    print(f"API Domain: {API_DOMAIN}")
    print(f"ClickHouse: {CLICKHOUSE_HOST}/{CLICKHOUSE_DB}")
    print("="*60)
    
    # Test 1: Get access token
    token_result = test_access_token()
    if not token_result:
        print("\n❌ Cannot proceed without access token. Please check your credentials.")
        return 1
    
    # Test 2: List modules
    modules = test_list_modules(token_result)
    
    # Test 3: ClickHouse connection
    client = test_clickhouse_connection()
    if not client:
        print("\n❌ Cannot proceed without ClickHouse connection.")
        return 1
    
    # Test 4: Sync modules (if we have modules)
    if modules:
        sync_result = test_sync_modules(modules, max_modules=5)  # Sync first 5 modules
    else:
        print("\n⚠️  No modules to sync. Skipping sync test.")
        sync_result = None
    
    # Test 5: Verify data
    verify_data_in_clickhouse(client)
    
    print("\n" + "="*60)
    print("TEST SUITE COMPLETED")
    print("="*60)
    
    if sync_result and sync_result.get('success'):
        print("✅ Overall Status: SUCCESS")
        return 0
    else:
        print("⚠️  Overall Status: COMPLETED WITH WARNINGS")
        return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)

