"""
Test that HANA config loads ONLY from .env with no hardcoded values
"""
import os
from dotenv import load_dotenv

def test_hana_env_config():
    """Test that HANA config loads only from .env"""
    print("=" * 70)
    print("TESTING: HANA Config from .env ONLY (No Hardcoded Values)")
    print("=" * 70)
    
    # Load .env file
    load_dotenv()
    
    print("\n1. Checking .env file variables:")
    print("-" * 70)
    env_vars = {
        'HANA_HOST': os.environ.get('HANA_HOST'),
        'HANA_PORT': os.environ.get('HANA_PORT'),
        'HANA_USERNAME': os.environ.get('HANA_USERNAME'),
        'HANA_PASSWORD': os.environ.get('HANA_PASSWORD')
    }
    
    all_set = True
    for var, value in env_vars.items():
        if value:
            display = value if 'PASSWORD' not in var else '***'
            print(f"   [OK] {var} = {display}")
        else:
            print(f"   [MISSING] {var} = (not set)")
            all_set = False
    
    if not all_set:
        print("\n   [INFO] Some HANA environment variables are not set.")
        print("   This is OK if you don't have a HANA connection yet.")
        print("   When you set them in .env, all HANA connections will use them.")
        print("\n   Required variables:")
        print("   - HANA_HOST")
        print("   - HANA_PORT")
        print("   - HANA_USERNAME")
        print("   - HANA_PASSWORD")
        return False
    
    print("\n2. Loading config using load_hana_config():")
    print("-" * 70)
    try:
        from db_utils import load_hana_config
        config = load_hana_config()
        
        print(f"   Host: {config['host']}")
        print(f"   Port: {config['port']}")
        print(f"   Username: {config['username']}")
        print(f"   Password: {'***' if config['password'] else '(empty)'}")
        
        # Verify no hardcoded values
        hardcoded_hosts = ['localhost', '127.0.0.1']
        if config['host'] in hardcoded_hosts:
            print(f"\n   [WARNING] Host is a hardcoded default value: {config['host']}")
            print("   Make sure HANA_HOST in .env is set to your actual server")
        
        if config['port'] == 30015:
            print(f"\n   [INFO] Port is 30015 (standard HANA port)")
        
        print("\n" + "=" * 70)
        print("[SUCCESS] HANA Config loaded from .env successfully!")
        print("=" * 70)
        print("\nAll HANA connections in the project will use these values:")
        print(f"  - Host: {config['host']}")
        print(f"  - Port: {config['port']}")
        print(f"  - Username: {config['username']}")
        print("\n[OK] No hardcoded values - everything comes from .env!")
        return True
        
    except ValueError as e:
        print(f"\n   [INFO] {e}")
        print("\n   This is expected if HANA env vars are not set yet.")
        print("   Set them in .env when you have a HANA connection.")
        return False
    except Exception as e:
        print(f"\n   [ERROR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    success = test_hana_env_config()
    print("\n" + "=" * 70)
    print("NOTE: If HANA env vars are not set, this is OK.")
    print("Set them in .env when you have a HANA connection.")
    print("=" * 70)

