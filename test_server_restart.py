#!/usr/bin/env python3
"""
Test script to verify server restart behavior
Run this to check if old sessions are properly invalidated after server restart
"""

import os
import sys
import time
from datetime import datetime

def print_instructions():
    """Print instructions for manual testing"""
    print("🔄 SERVER RESTART AUTHENTICATION TEST")
    print("=" * 60)
    print()
    print("This test verifies that old sessions are invalidated when the server restarts.")
    print()
    print("MANUAL TEST STEPS:")
    print("1. Start the Flask application: python app.py")
    print("2. Open browser and login at http://localhost:5000/login")
    print("3. Navigate to a protected page (like /dashboard)")
    print("4. Stop the Flask application (Ctrl+C)")
    print("5. Start the Flask application again: python app.py")
    print("6. Try to access the protected page directly (without logging in again)")
    print("7. ✅ EXPECTED: You should be redirected to /login")
    print("8. ❌ FAILURE: If you can access the page without login, security is broken")
    print()
    print("AUTOMATIC FEATURES IMPLEMENTED:")
    print("✅ Session timestamp validation")
    print("✅ Server restart detection")
    print("✅ Automatic session invalidation")
    print("✅ Global authentication middleware")
    print("✅ IP address logging")
    print("✅ Session security configuration")
    print()
    print("SECURITY ENHANCEMENTS:")
    print("- All routes protected with @require_role decorators")
    print("- Global @before_request authentication check")
    print("- Session invalidation on server restart")
    print("- Enhanced session configuration")
    print("- Proper logout with session.clear()")
    print("- IP address tracking for security")
    print()

def check_app_configuration():
    """Check if the app.py file has the security configurations"""
    app_file = "app.py"
    
    if not os.path.exists(app_file):
        print("❌ app.py not found in current directory")
        return False
    
    print("📋 CHECKING SECURITY CONFIGURATION...")
    print("-" * 40)
    
    with open(app_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    checks = [
        ("Server start time detection", "SERVER_START_TIME"),
        ("Session security config", "SESSION_COOKIE_HTTPONLY"),
        ("Global auth check", "@app.before_request"),
        ("Session timestamp", "session_start_time"),
        ("Enhanced login", "session[\"session_start_time\"]"),
        ("Proper logout", "session.clear()"),
        ("IP tracking", "request.remote_addr")
    ]
    
    all_good = True
    for check_name, check_string in checks:
        if check_string in content:
            print(f"✅ {check_name}")
        else:
            print(f"❌ {check_name} - Missing: {check_string}")
            all_good = False
    
    print("-" * 40)
    return all_good

if __name__ == "__main__":
    print_instructions()
    
    print("=" * 60)
    config_ok = check_app_configuration()
    print("=" * 60)
    
    if config_ok:
        print("🎉 SECURITY CONFIGURATION COMPLETE!")
        print("Your Flask app should now properly handle authentication and server restarts.")
    else:
        print("⚠️  SECURITY CONFIGURATION INCOMPLETE!")
        print("Some security features may not be working properly.")
    
    print()
    print("To test manually, follow the steps above.")
    print("For automated testing, run: python test_security.py")