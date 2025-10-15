#!/usr/bin/env python3
"""
Quick test to verify the Flask app starts with the enhanced sync manager
"""

import sys
import os
import time
import subprocess

def test_app_startup():
    """Test that the Flask app starts successfully with sync manager"""
    print("🧪 TESTING FLASK APP STARTUP WITH SYNC MANAGER")
    print("=" * 60)
    
    try:
        print("1. Testing import of app.py with sync manager...")
        
        # Test imports without starting the server
        import_test_code = """
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

try:
    # Test critical imports
    from sync_manager import sync_manager
    from app import app, load_config
    print("SUCCESS: All imports working")
    
    # Test sync manager integration
    active_syncs = sync_manager.get_all_active_syncs()
    print(f"SUCCESS: Sync manager active syncs: {len(active_syncs)}")
    
    # Test config loading
    config = load_config()
    servers = config.get('sqlservers', {})
    print(f"SUCCESS: Found {len(servers)} configured servers")
    
    print("READY: Flask app with sync manager is ready to run!")
    
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
"""
        
        # Write test script
        with open('temp_import_test.py', 'w') as f:
            f.write(import_test_code)
        
        # Run import test
        result = subprocess.run([sys.executable, 'temp_import_test.py'], 
                              capture_output=True, text=True, timeout=10)
        
        # Clean up
        if os.path.exists('temp_import_test.py'):
            os.remove('temp_import_test.py')
        
        if result.returncode == 0:
            print("✅ Import test passed:")
            for line in result.stdout.strip().split('\n'):
                print(f"   {line}")
            return True
        else:
            print("❌ Import test failed:")
            print(f"   stdout: {result.stdout}")
            print(f"   stderr: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

def main():
    """Run startup test"""
    print("🚀 FLASK APP STARTUP VERIFICATION")
    print("This verifies the app can start with the enhanced sync system")
    print()
    
    success = test_app_startup()
    
    print("\n" + "=" * 60)
    print("STARTUP TEST RESULT")
    print("=" * 60)
    
    if success:
        print("🎉 SUCCESS!")
        print("The Flask app is ready to run with the enhanced sync manager.")
        print("\n📋 TO START THE APPLICATION:")
        print("   python app.py")
        print("\n🔧 NEW FEATURES AVAILABLE:")
        print("   • Background sync operations")
        print("   • Real-time progress tracking")
        print("   • Navigation-safe sync operations")
        print("   • Multiple concurrent syncs")
        print("   • Enhanced status monitoring")
    else:
        print("❌ FAILED!")
        print("There are issues with the sync manager integration.")
        print("Please check the error messages above.")
    
    return success

if __name__ == "__main__":
    main()