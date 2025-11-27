"""
Complete test of Zoho CRM integration from the virtual environment
This simulates what the Flask app does
"""
import sys
print(f"Python: {sys.executable}")
print("="*60)

# Test 1: Import zoho_crm_sync
print("\n1. Testing zoho_crm_sync import...")
try:
    from zoho_crm_sync import get_access_token, get_available_modules, sync_zoho_modules
    print("   ✅ zoho_crm_sync imported successfully")
except Exception as e:
    print(f"   ❌ Failed: {e}")
    sys.exit(1)

# Test 2: Import clickhouse_connect
print("\n2. Testing clickhouse_connect import...")
try:
    from clickhouse_connect import get_client
    print("   ✅ clickhouse_connect imported successfully")
except Exception as e:
    print(f"   ❌ Failed: {e}")
    print("   SOLUTION: Run: myenv1\\Scripts\\python.exe -m pip install clickhouse-connect==0.8.0")
    sys.exit(1)

# Test 3: Test Zoho connection
print("\n3. Testing Zoho connection...")
CLIENT_ID = "1000.0L3LLVLEKE9ELW7CE0I0KJ3K4FKBBT"
CLIENT_SECRET = "d99c479d4c0db451c653d8c380bf6a4c557a73528c"
REFRESH_TOKEN = "1000.2cbaa36345c6d04b699b0cb6740c21ef.149922195c479d83c84826653ff84ff4"
API_DOMAIN = "https://www.zohoapis.in"

try:
    token_result = get_access_token(REFRESH_TOKEN, CLIENT_ID, CLIENT_SECRET, API_DOMAIN)
    if token_result:
        print(f"   ✅ Zoho access token obtained")
        modules = get_available_modules(token_result["access_token"], API_DOMAIN)
        print(f"   ✅ Found {len(modules)} modules")
    else:
        print("   ❌ Failed to get access token")
        sys.exit(1)
except Exception as e:
    print(f"   ❌ Error: {e}")
    sys.exit(1)

# Test 4: Test ClickHouse connection
print("\n4. Testing ClickHouse connection...")
CLICKHOUSE_HOST = "74.225.251.123"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASS = "root"
CLICKHOUSE_DB = "jarvis"

try:
    client = get_client(
        host=CLICKHOUSE_HOST,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASS,
        database=CLICKHOUSE_DB,
    )
    result = client.query("SELECT 1 as test")
    print(f"   ✅ ClickHouse connection successful")
    print(f"   ✅ Database: {CLICKHOUSE_DB}")
except Exception as e:
    print(f"   ❌ ClickHouse connection failed: {e}")
    sys.exit(1)

print("\n" + "="*60)
print("✅ ALL TESTS PASSED!")
print("="*60)
print("\nThe virtual environment (myenv1) is ready!")
print("Restart your Flask app and try the UI again.")
print("="*60)

