"""Test script to verify debug endpoints and diagnose data_sources visibility issue"""
import requests
import json

BASE_URL = "http://127.0.0.1:5001"

def login():
    """Login and get session cookies"""
    session = requests.Session()
    # Try to login as admin
    response = session.post(f"{BASE_URL}/login", data={
        "username": "admin",
        "password": "admin123"  # Default admin password
    }, allow_redirects=False)
    
    if response.status_code in [200, 302]:
        print("✓ Login successful")
        return session
    else:
        print(f"✗ Login failed: {response.status_code}")
        # Try alternate password
        response = session.post(f"{BASE_URL}/login", data={
            "username": "admin",
            "password": "admin"
        }, allow_redirects=False)
        if response.status_code in [200, 302]:
            print("✓ Login successful (alternate password)")
            return session
        else:
            print("✗ Could not login with default credentials")
            return None

def test_debug_data_sources(session):
    """Test the /debug/data_sources endpoint"""
    print("\n" + "="*60)
    print("Testing GET /debug/data_sources")
    print("="*60)
    
    response = session.get(f"{BASE_URL}/debug/data_sources")
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"Success: {data.get('success')}")
        print(f"Count: {data.get('count')}")
        print(f"\nData Sources:")
        for ds in data.get('data_sources', []):
            print(f"  - ID: {ds['id']}, Name: {ds['source_name']}, Type: {ds['source_type']}")
            print(f"    Server: {ds['server_address']}, Target: {ds['target_type']}/{ds['target_database']}")
        return data
    else:
        print(f"Error: {response.text}")
        return None

def test_debug_insert_sample(session):
    """Test the /debug/insert-sample endpoint"""
    print("\n" + "="*60)
    print("Testing GET /debug/insert-sample")
    print("="*60)
    
    response = session.get(f"{BASE_URL}/debug/insert-sample?name=test_source_automated", allow_redirects=False)
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.text[:200]}")
    
    if response.status_code == 302:
        print("✓ Redirect successful (likely inserted)")
        return True
    else:
        print(f"✗ Unexpected response")
        return False

def test_home_page(session):
    """Test if home page loads and shows data_sources"""
    print("\n" + "="*60)
    print("Testing GET / (home page)")
    print("="*60)
    
    response = session.get(f"{BASE_URL}/")
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        html = response.text
        # Check if data_sources section is present
        if "data_sources" in html.lower() or "configured sources" in html.lower():
            print("✓ Home page contains data_sources references")
        else:
            print("✗ Home page does not reference data_sources")
        
        # Check for empty state message
        if "no servers or sources configured" in html.lower():
            print("✗ Empty state message detected (no sources displayed)")
        else:
            print("✓ Not showing empty state")
        
        return html
    else:
        print(f"✗ Error loading home page")
        return None

if __name__ == "__main__":
    print("Starting diagnostic tests...")
    
    # Login
    session = login()
    if not session:
        print("\n✗ Cannot proceed without login. Please check credentials.")
        exit(1)
    
    # Test current state
    data = test_debug_data_sources(session)
    
    if data and data.get('count', 0) == 0:
        print("\n⚠ No data_sources found. Inserting sample...")
        test_debug_insert_sample(session)
        
        # Check again
        print("\n" + "="*60)
        print("Checking data_sources after insert")
        print("="*60)
        data = test_debug_data_sources(session)
    
    # Test home page
    test_home_page(session)
    
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    if data:
        count = data.get('count', 0)
        print(f"Total data_sources in DB: {count}")
        if count > 0:
            print("✓ Data sources exist in database")
            print("→ If cards not visible on home page, check template rendering")
        else:
            print("✗ No data sources in database")
            print("→ Insert issue or database connection mismatch")
    
    print("\nDone. Check the output above for issues.")
