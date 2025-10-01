#!/usr/bin/env python3
"""
Test script to verify authentication routing works properly
"""

import requests
import sys

def test_auth_routing():
    """Test that accessing / redirects to /login when not authenticated"""
    base_url = "http://localhost:5000"
    
    try:
        # Test accessing the home page without authentication
        response = requests.get(f"{base_url}/", allow_redirects=False)
        print(f"Status Code: {response.status_code}")
        print(f"Headers: {dict(response.headers)}")
        
        if response.status_code == 302:  # Redirect
            location = response.headers.get('Location', '')
            print(f"Redirect Location: {location}")
            
            if '/login' in location:
                print("✅ SUCCESS: Unauthenticated users are redirected to login")
                return True
            else:
                print(f"❌ FAILURE: Redirected to {location} instead of login")
                return False
        else:
            print(f"❌ FAILURE: Expected redirect (302), got {response.status_code}")
            return False
            
    except requests.exceptions.ConnectionError:
        print("❌ ERROR: Could not connect to Flask app at http://localhost:5000")
        print("Please start the Flask application first with: python app.py")
        return False
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

if __name__ == "__main__":
    print("Testing authentication routing...")
    print("=" * 50)
    success = test_auth_routing()
    print("=" * 50)
    if success:
        print("Authentication routing test PASSED")
        sys.exit(0)
    else:
        print("Authentication routing test FAILED")
        sys.exit(1)