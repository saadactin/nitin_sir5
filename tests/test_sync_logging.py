#!/usr/bin/env python3
"""
Test sync logging to verify terminal output
"""
import sys
import os
sys.path.append(os.path.dirname(__file__))

from hybrid_sync import process_sql_server_hybrid
from manage_server import load_config

def test_sync_logging():
    print("🔧 Testing sync logging output...")
    print("=" * 60)
    
    # Load configuration
    config = load_config()
    sqlservers = config.get('sqlservers', {})
    
    if not sqlservers:
        print("❌ No SQL servers configured")
        return
    
    # Test with first server
    server_name = list(sqlservers.keys())[0]
    server_conf = sqlservers[server_name]
    
    print(f"🎯 Testing sync for server: {server_name}")
    print(f"📊 Configuration: {server_conf.get('server')}:{server_conf.get('port')}")
    print(f"🎯 Target DB: {server_conf.get('target_postgres_db')}")
    print("=" * 60)
    
    try:
        # This should show all the enhanced logging in terminal
        process_sql_server_hybrid(server_name, server_conf)
        print("✅ Sync test completed successfully!")
    except Exception as e:
        print(f"❌ Sync test failed: {e}")

if __name__ == "__main__":
    test_sync_logging()
#!/usr/bin/env python3
"""
Test sync logging to verify terminal output
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from hybrid_sync import process_sql_server_hybrid
from manage_server import load_config

def test_sync_logging():
    print("🔧 Testing sync logging output...")
    print("=" * 60)
    
    # Load configuration
    config = load_config()
    sqlservers = config.get('sqlservers', {})
    
    if not sqlservers:
        print("❌ No SQL servers configured")
        return
    
    # Test with first server
    server_name = list(sqlservers.keys())[0]
    server_conf = sqlservers[server_name]
    
    print(f"🎯 Testing sync for server: {server_name}")
    print(f"📊 Configuration: {server_conf.get('server')}:{server_conf.get('port')}")
    print(f"🎯 Target DB: {server_conf.get('target_postgres_db')}")
    print("=" * 60)
    
    try:
        # This should show all the enhanced logging in terminal
        process_sql_server_hybrid(server_name, server_conf)
        print("✅ Sync test completed successfully!")
    except Exception as e:
        print(f"❌ Sync test failed: {e}")

if __name__ == "__main__":
    test_sync_logging()
