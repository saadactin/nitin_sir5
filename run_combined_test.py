"""
Combined test - runs server and sync together
"""
import subprocess
import time
import sys
from threading import Thread

def run_mock_server():
    """Run the mock server"""
    subprocess.run([sys.executable, 'mock_sse_server.py'])

def run_sync_test():
    """Run the sync after waiting for server to start"""
    time.sleep(5)  # Wait for server to start
    
    print("\n" + "="*60)
    print("Starting API Sync Test")
    print("="*60 + "\n")
    
    from api_sync import sync_api_to_clickhouse
    
    # Drop existing table
    try:
        from clickhouse_driver import Client
        from db_utils import load_clickhouse_config
        ch_conf = load_clickhouse_config()
        client = Client(
            host=ch_conf['host'],
            port=ch_conf['port'],
            user=ch_conf['user'],
            password=ch_conf['password']
        )
        client.execute('DROP TABLE IF EXISTS test4.crm_deals')
        print("✅ Cleaned up existing table\n")
    except Exception as e:
        print(f"⚠️  Could not clean table: {e}\n")
    
    # Start sync (will run for 20 seconds then we'll check)
    sync_thread = Thread(target=lambda: sync_api_to_clickhouse(
        api_url="http://localhost:3000/api/crm/stream",
        target_database="test4",
        target_table="crm_deals",
        is_sse=True,
        auto_create_table=True
    ), daemon=True)
    sync_thread.start()
    
    # Wait and show progress
    for i in range(20, 0, -1):
        print(f"⏳ Syncing... {i} seconds remaining", end='\r')
        time.sleep(1)
    
    print("\n\n✅ Sync period completed\n")
    
    # Check results
    time.sleep(2)
    subprocess.run([sys.executable, 'check_synced_data.py'])
    
    print("\n\n🛑 Press Ctrl+C to stop the mock server...")

if __name__ == "__main__":
    print("="*60)
    print(" Combined API Sync Test")
    print("="*60)
    print("\nStarting mock server on port 3000...")
    
    # Start server in thread
    server_thread = Thread(target=run_mock_server, daemon=True)
    server_thread.start()
    
    time.sleep(2)
    print("✅ Mock server started\n")
    
    # Run sync test
    try:
        run_sync_test()
        # Keep running
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n👋 Test stopped by user")
