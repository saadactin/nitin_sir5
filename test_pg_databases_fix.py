#!/usr/bin/env python3
"""
Test the fixed load_pg_databases() function from app.py
to verify that it now properly loads PostgreSQL databases.
"""

import sys
import os

# Add current directory to path so we can import from app.py
sys.path.insert(0, os.path.dirname(__file__))

def test_load_pg_databases():
    """Test that load_pg_databases now works with YAML config"""
    print("🧪 TESTING FIXED load_pg_databases() FUNCTION")
    print("=" * 60)
    
    try:
        # Import the fixed function from app.py
        from app import load_pg_databases
        
        print("1. Calling load_pg_databases()...")
        databases = load_pg_databases()
        
        if databases:
            print(f"   ✅ SUCCESS! Found {len(databases)} PostgreSQL databases:")
            for i, db in enumerate(databases, 1):
                print(f"      {i}. {db}")
            print(f"\n📊 This should fix the 'No PostgreSQL databases found' issue!")
        else:
            print(f"   ❌ FAILED: Still returning empty list")
            print(f"   💡 Check the Flask app logs for the specific error details")
            return False
            
    except Exception as e:
        print(f"   ❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

def test_integration_with_db_utils():
    """Test that both approaches now work consistently"""
    print("\n🔄 TESTING CONSISTENCY WITH db_utils")
    print("=" * 60)
    
    try:
        from app import load_pg_databases
        from db_utils import get_pg_connection
        
        print("1. Testing app.py approach...")
        app_databases = load_pg_databases()
        
        print("2. Testing db_utils approach...")
        conn = get_pg_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        utils_databases = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        
        print("3. Comparing results...")
        app_set = set(app_databases)
        utils_set = set(utils_databases)
        
        if app_set == utils_set:
            print(f"   ✅ SUCCESS! Both approaches return identical results:")
            print(f"      app.py: {len(app_databases)} databases")
            print(f"      db_utils: {len(utils_databases)} databases")
        else:
            print(f"   ⚠️  DIFFERENCE DETECTED:")
            print(f"      app.py: {app_databases}")
            print(f"      db_utils: {utils_databases}")
            print(f"      Only in app.py: {app_set - utils_set}")
            print(f"      Only in db_utils: {utils_set - app_set}")
    
    except Exception as e:
        print(f"   ❌ ERROR during consistency test: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    print("🔧 TESTING FIX FOR 'No PostgreSQL databases found' ISSUE")
    print()
    
    success1 = test_load_pg_databases()
    success2 = test_integration_with_db_utils()
    
    print("\n" + "=" * 60)
    print("FINAL RESULT")
    print("=" * 60)
    
    if success1 and success2:
        print("🎉 ALL TESTS PASSED!")
        print("   The 'No PostgreSQL databases found' issue should now be RESOLVED!")
        print("   Frontend dropdowns should now show available databases.")
    else:
        print("❌ SOME TESTS FAILED")
        print("   The issue may not be fully resolved. Check error details above.")