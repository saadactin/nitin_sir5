"""
Test the schedule_page route directly to verify it loads sources correctly
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app import app
from flask import Flask

def test_schedule_route():
    """Test that schedule_page route loads sources correctly"""
    print("=" * 70)
    print("TESTING SCHEDULE ROUTE")
    print("=" * 70)
    
    with app.app_context():
        with app.test_client() as client:
            # Mock a session
            with client.session_transaction() as sess:
                sess['username'] = 'admin'
                sess['role'] = 'admin'
            
            # Make a GET request to /schedule
            print("\nMaking GET request to /schedule...")
            response = client.get('/schedule', follow_redirects=True)
            
            print(f"Response status: {response.status_code}")
            print(f"Response length: {len(response.data)} bytes")
            
            # Check if response contains expected sources
            response_text = response.data.decode('utf-8')
            
            # Check for server1
            if 'server1' in response_text:
                print("[OK] Found 'server1' in response")
            else:
                print("[FAIL] 'server1' NOT found in response")
            
            # Check for hana1
            if 'hana1' in response_text:
                print("[OK] Found 'hana1' in response")
            else:
                print("[FAIL] 'hana1' NOT found in response")
            
            # Check for hana2
            if 'hana2' in response_text:
                print("[OK] Found 'hana2' in response")
            else:
                print("[FAIL] 'hana2' NOT found in response")
            
            # Check for SQL Server section
            if 'sqlServerSources' in response_text:
                print("[OK] SQL Server sources section exists in HTML")
            else:
                print("[FAIL] SQL Server sources section NOT found")
            
            # Check for HANA section
            if 'hanaSources' in response_text:
                print("[OK] HANA sources section exists in HTML")
            else:
                print("[FAIL] HANA sources section NOT found")
            
            # Check for debug messages
            if 'Debug:' in response_text:
                print("[INFO] Debug messages found in response")
                # Extract debug info
                import re
                debug_matches = re.findall(r'Debug:.*?<\/p>', response_text)
                for match in debug_matches:
                    print(f"  Debug: {match[:100]}...")
            
            # Count source cards
            sql_cards = response_text.count('source-option')
            print(f"\nFound {sql_cards} source option cards in HTML")
            
            return response.status_code == 200

if __name__ == "__main__":
    try:
        result = test_schedule_route()
        if result:
            print("\n[SUCCESS] Route test completed")
        else:
            print("\n[FAILURE] Route test failed")
            sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] Route test error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

