"""
Test CoinGecko API and show data structure
"""
import requests
import json

# Test the CoinGecko API
url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&per_page=5"

print("🔍 Testing CoinGecko API...")
print(f"URL: {url}\n")

try:
    response = requests.get(url, timeout=10)
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"\n✅ SUCCESS! Got {len(data)} coins\n")
        print("=" * 80)
        print("Sample Record (First Coin):")
        print("=" * 80)
        print(json.dumps(data[0], indent=2))
        
        print("\n" + "=" * 80)
        print("All Field Names:")
        print("=" * 80)
        for key in data[0].keys():
            print(f"  - {key}")
            
        print("\n" + "=" * 80)
        print("📋 To sync this API to ClickHouse:")
        print("=" * 80)
        print("1. Go to: http://localhost:5001/add-api-source")
        print("2. Fill in:")
        print(f"   - API URL: {url}")
        print("   - Request Method: GET")
        print("   - Stream Type: REST API (not SSE)")
        print("   - Target Database: test9")
        print("   - Target Table: crypto_markets")
        print("3. Click 'Add Source'")
        print("4. Click 'Sync Server' button")
        print("5. Watch data sync every 10 seconds!")
        
    else:
        print(f"❌ Error: {response.status_code}")
        print(f"Response: {response.text}")
        
except Exception as e:
    print(f"❌ Error: {e}")
