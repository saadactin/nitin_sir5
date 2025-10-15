#!/usr/bin/env python3
"""
Security Test Script for Flask Authentication
Tests that authentication is properly enforced across all routes
"""

import requests
import sys
from urllib.parse import urljoin

def test_route_protection():
    """Test that all routes are properly protected"""
    base_url = "http://localhost:5000"
    
    # List of routes that should be protected
    protected_routes = [
        "/",
        "/dashboard",
        "/schedule", 
        "/view-schedules",
        "/alerts",
        "/logs",
        "/sync-summary",
        "/add-server",
        "/server/test"
    ]
    
    print("Testing route protection...")
    print("=" * 50)
    
    all_protected = True
    
    try:
        for route in protected_routes:
            url = urljoin(base_url, route)
            response = requests.get(url, allow_redirects=False)
            
            print(f"Testing {route}...")
            print(f"  Status: {response.status_code}")
            
            if response.status_code == 302:  # Redirect
                location = response.headers.get('Location', '')
                if '/login' in location:
                    print(f"  ✅ PROTECTED - Redirects to login")
                else:
                    print(f"  ❌ VULNERABLE - Redirects to {location}")
                    all_protected = False
            elif response.status_code == 200:
                print(f"  ❌ VULNERABLE - Direct access allowed")
                all_protected = False
            else:
                print(f"  ⚠️  UNKNOWN - Status {response.status_code}")
            
            print()
            
    except requests.exceptions.ConnectionError:
        print("❌ ERROR: Could not connect to Flask app at http://localhost:5000")
        print("Please start the Flask application first with: python app.py")
        return False
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False
    
    return all_protected

def test_session_persistence():
    """Test that sessions are properly managed"""
    base_url = "http://localhost:5000"
    
    print("Testing session management...")
    print("=" * 50)
    
    try:
        # Test login
        session = requests.Session()
        
        # First, get login page
        login_response = session.get(f"{base_url}/login")
        if login_response.status_code != 200:
            print("❌ Could not access login page")
            return False
        
        # Try to login with default admin credentials
        login_data = {
            'username': 'admin',
            'password': 'admin123'
        }
        
        login_result = session.post(f"{base_url}/login", data=login_data, allow_redirects=False)
        
        if login_result.status_code == 302:
            location = login_result.headers.get('Location', '')
            if '/' in location and '/login' not in location:
                print("✅ Login successful - redirected to home")
                
                # Test accessing protected route with session
                protected_response = session.get(f"{base_url}/dashboard", allow_redirects=False)
                if protected_response.status_code == 200:
                    print("✅ Session works - can access protected routes")
                    return True
                else:
                    print(f"❌ Session failed - protected route returned {protected_response.status_code}")
                    return False
            else:
                print(f"❌ Login failed - redirected to {location}")
                return False
        else:
            print(f"❌ Login failed - status {login_result.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Session test error: {e}")
        return False

if __name__ == "__main__":
    print("🔒 FLASK SECURITY TEST")
    print("=" * 60)
    
    # Test 1: Route Protection
    protection_ok = test_route_protection()
    
    # Test 2: Session Management  
    session_ok = test_session_persistence()
    
    print("=" * 60)
    print("SECURITY TEST RESULTS:")
    print(f"Route Protection: {'✅ PASS' if protection_ok else '❌ FAIL'}")
    print(f"Session Management: {'✅ PASS' if session_ok else '❌ FAIL'}")
    print("=" * 60)
    
    if protection_ok and session_ok:
        print("🎉 ALL SECURITY TESTS PASSED!")
        sys.exit(0)
    else:
        print("⚠️  SECURITY ISSUES DETECTED!")
        sys.exit(1)
#!/usr/bin/env python3
"""
Security Test Script for Flask Authentication
Tests that authentication is properly enforced across all routes
"""

import requests
import sys
from urllib.parse import urljoin

def test_route_protection():
    """Test that all routes are properly protected"""
    base_url = "http://localhost:5000"
    
    # List of routes that should be protected
    protected_routes = [
        "/",
        "/dashboard",
        "/schedule", 
        "/view-schedules",
        "/alerts",
        "/logs",
        "/sync-summary",
        "/add-server",
        "/server/test"
    ]
    
    print("Testing route protection...")
    print("=" * 50)
    
    all_protected = True
    
    try:
        for route in protected_routes:
            url = urljoin(base_url, route)
            response = requests.get(url, allow_redirects=False)
            
            print(f"Testing {route}...")
            print(f"  Status: {response.status_code}")
            
            if response.status_code == 302:  # Redirect
                location = response.headers.get('Location', '')
                if '/login' in location:
                    print(f"  ✅ PROTECTED - Redirects to login")
                else:
                    print(f"  ❌ VULNERABLE - Redirects to {location}")
                    all_protected = False
            elif response.status_code == 200:
                print(f"  ❌ VULNERABLE - Direct access allowed")
                all_protected = False
            else:
                print(f"  ⚠️  UNKNOWN - Status {response.status_code}")
            
            print()
            
    except requests.exceptions.ConnectionError:
        print("❌ ERROR: Could not connect to Flask app at http://localhost:5000")
        print("Please start the Flask application first with: python app.py")
        return False
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False
    
    return all_protected

def test_session_persistence():
    """Test that sessions are properly managed"""
    base_url = "http://localhost:5000"
    
    print("Testing session management...")
    print("=" * 50)
    
    try:
        # Test login
        session = requests.Session()
        
        # First, get login page
        login_response = session.get(f"{base_url}/login")
        if login_response.status_code != 200:
            print("❌ Could not access login page")
            return False
        
        # Try to login with default admin credentials
        login_data = {
            'username': 'admin',
            'password': 'admin123'
        }
        
        login_result = session.post(f"{base_url}/login", data=login_data, allow_redirects=False)
        
        if login_result.status_code == 302:
            location = login_result.headers.get('Location', '')
            if '/' in location and '/login' not in location:
                print("✅ Login successful - redirected to home")
                
                # Test accessing protected route with session
                protected_response = session.get(f"{base_url}/dashboard", allow_redirects=False)
                if protected_response.status_code == 200:
                    print("✅ Session works - can access protected routes")
                    return True
                else:
                    print(f"❌ Session failed - protected route returned {protected_response.status_code}")
                    return False
            else:
                print(f"❌ Login failed - redirected to {location}")
                return False
        else:
            print(f"❌ Login failed - status {login_result.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Session test error: {e}")
        return False

if __name__ == "__main__":
    print("🔒 FLASK SECURITY TEST")
    print("=" * 60)
    
    # Test 1: Route Protection
    protection_ok = test_route_protection()
    
    # Test 2: Session Management  
    session_ok = test_session_persistence()
    
    print("=" * 60)
    print("SECURITY TEST RESULTS:")
    print(f"Route Protection: {'✅ PASS' if protection_ok else '❌ FAIL'}")
    print(f"Session Management: {'✅ PASS' if session_ok else '❌ FAIL'}")
    print("=" * 60)
    
    if protection_ok and session_ok:
        print("🎉 ALL SECURITY TESTS PASSED!")
        sys.exit(0)
    else:
        print("⚠️  SECURITY ISSUES DETECTED!")
        sys.exit(1)
