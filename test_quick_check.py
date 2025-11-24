"""
Quick Health Check - Fast verification that project is running
Run this for a quick status check
"""

import sys
import os
import requests
from datetime import datetime

# Fix Windows console encoding
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

BASE_URL = os.getenv('FLASK_BASE_URL', 'http://localhost:5001')

def quick_check():
    """Quick health check"""
    print("\n" + "="*50)
    print("QUICK HEALTH CHECK")
    print("="*50 + "\n")
    
    checks = {
        "Server Running": False,
        "Login Page": False,
        "Database": False,
    }
    
    # Check server
    try:
        response = requests.get(f"{BASE_URL}/login", timeout=3)
        checks["Server Running"] = response.status_code == 200
        checks["Login Page"] = "login" in response.text.lower()
    except:
        pass
    
    # Check database
    try:
        from db_utils import get_pg_connection
        conn = get_pg_connection()
        conn.close()
        checks["Database"] = True
    except:
        pass
    
    # Print results
    for check, status in checks.items():
        status_str = "[OK]" if status else "[FAIL]"
        print(f"{status_str} {check}")
    
    all_passed = all(checks.values())
    print(f"\nStatus: {'[OK] HEALTHY' if all_passed else '[FAIL] ISSUES DETECTED'}\n")
    
    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(quick_check())

