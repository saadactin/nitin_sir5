"""
Quick test to verify the API endpoint works correctly
This simulates what the UI does
"""
import requests
import json

# Test credentials
API_DOMAIN = "https://www.zohoapis.in"
CLIENT_ID = "1000.0L3LLVLEKE9ELW7CE0I0KJ3K4FKBBT"
CLIENT_SECRET = "d99c479d4c0db451c653d8c380bf6a4c557a73528c"
REFRESH_TOKEN = "1000.2cbaa36345c6d04b699b0cb6740c21ef.149922195c479d83c84826653ff84ff4"

# Flask app URL (adjust if different)
BASE_URL = "http://localhost:5002"

def test_list_modules():
    """Test the list-modules endpoint"""
    print("Testing /api/zoho/list-modules endpoint...")
    print("="*60)
    
    url = f"{BASE_URL}/api/zoho/list-modules"
    
    payload = {
        "api_domain": API_DOMAIN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN
    }
    
    try:
        print(f"POST {url}")
        print(f"Payload: {json.dumps({**payload, 'client_secret': '***', 'refresh_token': '***'}, indent=2)}")
        
        response = requests.post(
            url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        
        print(f"\nStatus Code: {response.status_code}")
        print(f"Content-Type: {response.headers.get('Content-Type')}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"\n✅ SUCCESS!")
            print(f"Response: {json.dumps(data, indent=2)[:500]}...")
            
            if data.get("success") and data.get("modules"):
                print(f"\n📦 Found {len(data['modules'])} modules")
                print("\nFirst 10 modules:")
                for i, module in enumerate(data['modules'][:10], 1):
                    print(f"  {i}. {module.get('display_name')} ({module.get('api_name')})")
            else:
                print(f"\n⚠️  Response indicates failure: {data.get('error', 'Unknown error')}")
        else:
            print(f"\n❌ FAILED with status {response.status_code}")
            print(f"Response: {response.text[:500]}")
            
    except requests.exceptions.ConnectionError:
        print(f"\n❌ ERROR: Cannot connect to {BASE_URL}")
        print("   Make sure your Flask app is running!")
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_list_modules()

