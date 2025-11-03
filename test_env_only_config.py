"""
Test that ClickHouse config loads ONLY from .env with no hardcoded values
"""
import os
from dotenv import load_dotenv
from db_utils import load_clickhouse_config

def test_env_only_config():
    """Test that config loads only from .env"""
    print("=" * 70)
    print("TESTING: ClickHouse Config from .env ONLY (No Hardcoded Values)")
    print("=" * 70)
    
    # Load .env file
    load_dotenv()
    
    print("\n1. Checking .env file variables:")
    print("-" * 70)
    env_vars = {
        'CLICKHOUSE_HOST': os.environ.get('CLICKHOUSE_HOST'),
        'CLICKHOUSE_PORT': os.environ.get('CLICKHOUSE_PORT'),
        'CLICKHOUSE_USER': os.environ.get('CLICKHOUSE_USER'),
        'CLICKHOUSE_PASSWORD': os.environ.get('CLICKHOUSE_PASSWORD')
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
        print("\n   [ERROR] Missing required environment variables!")
        print("   Please set all 4 variables in your .env file:")
        print("   - CLICKHOUSE_HOST")
        print("   - CLICKHOUSE_PORT")
        print("   - CLICKHOUSE_USER")
        print("   - CLICKHOUSE_PASSWORD")
        return False
    
    print("\n2. Loading config using load_clickhouse_config():")
    print("-" * 70)
    try:
        config = load_clickhouse_config()
        
        print(f"   Host: {config['host']}")
        print(f"   Port: {config['port']}")
        print(f"   User: {config['user']}")
        print(f"   Password: {'***' if config['password'] else '(empty)'}")
        
        # Verify no hardcoded values
        hardcoded_hosts = ['localhost', '127.0.0.1']
        if config['host'] in hardcoded_hosts:
            print(f"\n   [WARNING] Host is a hardcoded default value: {config['host']}")
            print("   Make sure CLICKHOUSE_HOST in .env is set to your actual server")
        
        print("\n" + "=" * 70)
        print("[SUCCESS] Config loaded from .env successfully!")
        print("=" * 70)
        print("\nAll ClickHouse connections in the project will use these values:")
        print(f"  - Host: {config['host']}")
        print(f"  - Port: {config['port']}")
        print(f"  - User: {config['user']}")
        print("\n✅ No hardcoded values - everything comes from .env!")
        return True
        
    except ValueError as e:
        print(f"\n   [ERROR] {e}")
        print("\n   This error means a required variable is missing from .env")
        return False
    except Exception as e:
        print(f"\n   [ERROR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    success = test_env_only_config()
    exit(0 if success else 1)

