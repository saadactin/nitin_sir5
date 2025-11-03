"""
Quick Test Script for Polling
Starts mock API, runs a short polling test, and shows results
"""
import subprocess
import time
import requests
import sys
from clickhouse_driver import Client
from db_utils import load_clickhouse_config

def check_mock_api():
    """Check if mock API is running"""
    try:
        response = requests.get("http://localhost:5002/api/health", timeout=2)
        return response.status_code == 200
    except:
        return False

def start_mock_api():
    """Start the mock API server"""
    print("[1/4] Starting Mock API Server...")
    try:
        # Try to start in background (Windows)
        import sys
        if sys.platform == 'win32':
            process = subprocess.Popen(
                [sys.executable, "mock_incremental_api.py"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                creationflags=subprocess.CREATE_NEW_CONSOLE
            )
        else:
            process = subprocess.Popen(
                [sys.executable, "mock_incremental_api.py"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
        
        # Wait for API to start
        max_wait = 10
        for i in range(max_wait):
            if check_mock_api():
                print(f"   [OK] Mock API is running (PID: {process.pid})")
                return process
            time.sleep(1)
            print(f"   Waiting for API to start... ({i+1}/{max_wait})")
        
        print(f"   [ERROR] API did not start in {max_wait} seconds")
        return None
    except Exception as e:
        print(f"   [ERROR] Failed to start API: {e}")
        return None

def test_single_sync():
    """Test a single sync operation"""
    print("\n[2/4] Testing Single Sync...")
    
    try:
        from api_sync import sync_api_to_clickhouse_once
        
        # Add some initial records
        print("   Adding 10 records to mock API...")
        requests.post(
            "http://localhost:5002/api/data/add",
            json={"count": 10},
            timeout=5
        )
        time.sleep(1)
        
        # Sync
        print("   Syncing to ClickHouse...")
        result = sync_api_to_clickhouse_once(
            api_url="http://localhost:5002/api/data",
            target_database="test_polling_db",
            target_table="incremental_test_data",
            auth_type="none",
            data_path="data",
            auto_create_table=True
        )
        
        if result['success']:
            print(f"   [OK] Synced {result['records_synced']} records")
            return True
        else:
            print(f"   [ERROR] Sync failed: {result.get('error')}")
            return False
    except Exception as e:
        print(f"   [ERROR] Test failed: {e}")
        return False

def test_polling_short():
    """Test polling for a short duration"""
    print("\n[3/4] Testing Polling (30 seconds)...")
    
    try:
        from api_sync import sync_api_to_clickhouse_once
        
        # Get initial count
        ch_conf = load_clickhouse_config()
        client = Client(
            host=ch_conf.get('host', 'localhost'),
            port=int(ch_conf.get('port', 9000)),
            user=ch_conf.get('user', 'default'),
            password=ch_conf.get('password', '')
        )
        
        count_query = "SELECT count() FROM test_polling_db.incremental_test_data"
        initial_count = client.execute(count_query)[0][0]
        print(f"   Initial records in ClickHouse: {initial_count}")
        
        # Wait and sync multiple times (simulating polling)
        for i in range(3):
            print(f"   Poll {i+1}/3: Waiting 8 seconds for new records...")
            time.sleep(8)  # Wait for API to add records (adds 5 every 5 seconds)
            
            result = sync_api_to_clickhouse_once(
                api_url="http://localhost:5001/api/data",
                target_database="test_polling_db",
                target_table="incremental_test_data",
                auth_type="none",
                data_path="data",
                auto_create_table=False
            )
            
            if result['success']:
                new_count = client.execute(count_query)[0][0]
                new_records = new_count - initial_count
                print(f"      Synced {result['records_synced']} records (new: ~{new_records})")
                initial_count = new_count
            else:
                print(f"      [WARNING] Sync failed: {result.get('error')}")
        
        # Final count
        final_count = client.execute(count_query)[0][0]
        total_new = final_count - initial_count
        print(f"\n   [OK] Polling test completed!")
        print(f"   Final records in ClickHouse: {final_count}")
        print(f"   Total new records synced: {total_new}")
        
        return True
    except Exception as e:
        print(f"   [ERROR] Polling test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def show_results():
    """Show final results"""
    print("\n[4/4] Showing Results...")
    
    try:
        ch_conf = load_clickhouse_config()
        client = Client(
            host=ch_conf.get('host', 'localhost'),
            port=int(ch_conf.get('port', 9000)),
            user=ch_conf.get('user', 'default'),
            password=ch_conf.get('password', '')
        )
        
        # Get stats
        count_query = "SELECT count() FROM test_polling_db.incremental_test_data"
        total_count = client.execute(count_query)[0][0]
        
        # Get sample records
        sample_query = "SELECT * FROM test_polling_db.incremental_test_data LIMIT 5"
        samples = client.execute(sample_query)
        
        # Get API stats
        api_stats = requests.get("http://localhost:5002/api/data/stats", timeout=2).json()
        
        print(f"\n   ClickHouse Records: {total_count}")
        print(f"   API Total Records: {api_stats['total_records']}")
        print(f"\n   Sample Records (first 5):")
        for i, sample in enumerate(samples, 1):
            # Show first few fields
            sample_dict = dict(zip([desc[0] for desc in client.execute("DESCRIBE TABLE test_polling_db.incremental_test_data")], sample))
            print(f"      {i}. ID: {sample_dict.get('id', 'N/A')}, Name: {sample_dict.get('name', 'N/A')}")
        
        return True
    except Exception as e:
        print(f"   [ERROR] Could not show results: {e}")
        return False

def main():
    print("=" * 70)
    print("QUICK POLLING TEST")
    print("=" * 70)
    print("\nThis script will:")
    print("  1. Start mock API server")
    print("  2. Test single sync")
    print("  3. Test polling (simulated)")
    print("  4. Show results")
    print("\n" + "=" * 70)
    
    # Check prerequisites
    print("\nChecking prerequisites...")
    try:
        ch_conf = load_clickhouse_config()
        client = Client(
            host=ch_conf.get('host', 'localhost'),
            port=int(ch_conf.get('port', 9000)),
            user=ch_conf.get('user', 'default'),
            password=ch_conf.get('password', '')
        )
        client.execute("SELECT 1")
        print("[OK] ClickHouse connection successful")
    except Exception as e:
        print(f"[ERROR] ClickHouse connection failed: {e}")
        print("\nPlease ensure ClickHouse is running and accessible.")
        return 1
    
    # Start mock API if not running
    mock_api_process = None
    if not check_mock_api():
        mock_api_process = start_mock_api()
        if not mock_api_process:
            print("\n[ERROR] Could not start mock API server")
            return 1
        time.sleep(2)
    else:
        print("[OK] Mock API is already running")
    
    try:
        # Run tests
        success = True
        
        if not test_single_sync():
            success = False
        
        if not test_polling_short():
            success = False
        
        if not show_results():
            success = False
        
        # Final summary
        print("\n" + "=" * 70)
        if success:
            print("✅ ALL TESTS PASSED!")
            print("\nNext steps:")
            print("  1. Test in the web UI: Add API source with polling enabled")
            print("  2. Monitor the sync in real-time")
            print("  3. Check ClickHouse for new records as they arrive")
        else:
            print("⚠️  SOME TESTS FAILED - Check errors above")
        print("=" * 70)
        
        return 0 if success else 1
    
    finally:
        # Cleanup
        if mock_api_process:
            print("\nStopping mock API server...")
            try:
                mock_api_process.terminate()
                mock_api_process.wait(timeout=5)
                print("[OK] Mock API stopped")
            except:
                print("[WARNING] Could not stop mock API cleanly")

if __name__ == '__main__':
    sys.exit(main())

