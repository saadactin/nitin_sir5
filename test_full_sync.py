"""
Complete End-to-End Test for API Sync
Tests the entire flow from API to ClickHouse
"""
import subprocess
import time
import sys
import os
from clickhouse_driver import Client
from db_utils import load_clickhouse_config

def print_banner(text):
    print("\n" + "="*60)
    print(f"  {text}")
    print("="*60 + "\n")

def check_clickhouse_connection():
    """Check if ClickHouse is accessible"""
    try:
        ch_conf = load_clickhouse_config()
        client = Client(
            host=ch_conf['host'],
            port=ch_conf['port'],
            user=ch_conf['user'],
            password=ch_conf['password']
        )
        client.execute('SELECT 1')
        print("✅ ClickHouse connection successful")
        return True
    except Exception as e:
        print(f"❌ ClickHouse connection failed: {e}")
        return False

def ensure_test_database():
    """Ensure test4 database exists"""
    try:
        ch_conf = load_clickhouse_config()
        client = Client(
            host=ch_conf['host'],
            port=ch_conf['port'],
            user=ch_conf['user'],
            password=ch_conf['password']
        )
        client.execute('CREATE DATABASE IF NOT EXISTS test4')
        print("✅ Database 'test4' ready")
        return True
    except Exception as e:
        print(f"❌ Failed to create database: {e}")
        return False

def drop_test_table():
    """Drop existing test table to start fresh"""
    try:
        ch_conf = load_clickhouse_config()
        client = Client(
            host=ch_conf['host'],
            port=ch_conf['port'],
            user=ch_conf['user'],
            password=ch_conf['password']
        )
        client.execute('DROP TABLE IF EXISTS test4.crm_deals')
        print("✅ Cleaned up any existing test table")
        return True
    except Exception as e:
        print(f"⚠️  Could not drop table (might not exist): {e}")
        return True

def verify_data_in_clickhouse():
    """Verify synced data in ClickHouse"""
    try:
        ch_conf = load_clickhouse_config()
        client = Client(
            host=ch_conf['host'],
            port=ch_conf['port'],
            user=ch_conf['user'],
            password=ch_conf['password']
        )
        
        print_banner("Verifying Data in ClickHouse")
        
        # Check if table exists
        tables = client.execute("SHOW TABLES FROM test4")
        if ('crm_deals',) not in tables:
            print("❌ Table 'crm_deals' was not created!")
            return False
        
        print("✅ Table 'crm_deals' exists")
        
        # Get table structure
        print("\n📋 Table Structure:")
        columns = client.execute("DESCRIBE test4.crm_deals")
        for col_name, col_type, *_ in columns:
            print(f"   {col_name:30} {col_type}")
        
        # Count records
        count = client.execute("SELECT COUNT(*) FROM test4.crm_deals")[0][0]
        print(f"\n📊 Total Records: {count}")
        
        if count == 0:
            print("⚠️  No records found! Sync might not have started yet.")
            return False
        
        # Show sample records
        print("\n📝 Sample Records:")
        records = client.execute("""
            SELECT 
                data_deal_name,
                data_amount,
                data_stage,
                data_account_name,
                _sync_timestamp
            FROM test4.crm_deals
            ORDER BY _sync_timestamp DESC
            LIMIT 5
        """)
        
        print("\n{:<30} {:<15} {:<20} {:<15} {}".format(
            "Deal Name", "Amount", "Stage", "Account", "Synced At"
        ))
        print("-" * 120)
        
        for deal_name, amount, stage, account, sync_time in records:
            print("{:<30} ${:<14,} {:<20} {:<15} {}".format(
                deal_name[:28], amount, stage[:18], account[:13], sync_time
            ))
        
        # Group by stage
        print("\n📈 Deals by Stage:")
        stage_stats = client.execute("""
            SELECT 
                data_stage,
                COUNT(*) as count,
                SUM(data_amount) as total_amount
            FROM test4.crm_deals
            GROUP BY data_stage
            ORDER BY count DESC
        """)
        
        for stage, count, total in stage_stats:
            print(f"   {stage:20} {count:3} deals    ${total:,}")
        
        print("\n" + "="*60)
        print("✅ Data verification successful!")
        print("="*60)
        
        return True
        
    except Exception as e:
        print(f"❌ Error verifying data: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print_banner("API to ClickHouse Sync - Full Test")
    
    # Step 1: Check ClickHouse
    print("Step 1: Checking ClickHouse connection...")
    if not check_clickhouse_connection():
        print("\n❌ Please start ClickHouse first!")
        return
    
    # Step 2: Prepare database
    print("\nStep 2: Preparing test database...")
    if not ensure_test_database():
        print("\n❌ Could not prepare database!")
        return
    
    # Step 3: Clean up
    print("\nStep 3: Cleaning up old test data...")
    drop_test_table()
    
    # Step 4: Start mock API server
    print("\nStep 4: Starting mock API server...")
    print("   (Starting in background on port 3000)")
    
    # Start server in background
    server_process = subprocess.Popen(
        [sys.executable, 'mock_sse_server.py'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        creationflags=subprocess.CREATE_NEW_CONSOLE if os.name == 'nt' else 0
    )
    
    # Wait for server to start
    time.sleep(3)
    print("✅ Mock API server started")
    
    # Step 5: Start sync
    print("\nStep 5: Starting API sync (will run for 15 seconds)...")
    print("   Syncing from: http://localhost:3000/api/crm/stream")
    print("   Target: ClickHouse test4.crm_deals")
    print()
    
    from api_sync import sync_api_to_clickhouse
    import threading
    
    sync_running = [True]
    
    def run_sync():
        try:
            sync_api_to_clickhouse(
                api_url="http://localhost:3000/api/crm/stream",
                target_database="test4",
                target_table="crm_deals",
                is_sse=True,
                auto_create_table=True
            )
        except Exception as e:
            if sync_running[0]:
                print(f"❌ Sync error: {e}")
    
    sync_thread = threading.Thread(target=run_sync, daemon=True)
    sync_thread.start()
    
    # Let it sync for 15 seconds
    for i in range(15, 0, -1):
        print(f"   ⏳ Syncing... {i} seconds remaining", end='\r')
        time.sleep(1)
    
    sync_running[0] = False
    print("\n\n✅ Sync period completed")
    
    # Step 6: Verify data
    print("\nStep 6: Verifying synced data...")
    time.sleep(2)  # Give it a moment to finish
    
    success = verify_data_in_clickhouse()
    
    # Cleanup
    print("\nCleaning up...")
    server_process.terminate()
    print("✅ Mock server stopped")
    
    # Final result
    if success:
        print_banner("🎉 TEST PASSED - Data synced successfully!")
        print("The API sync feature is working perfectly!")
        print("\nYou can now:")
        print("  1. Use the web UI to add API sources")
        print("  2. Point to your real API endpoints")
        print("  3. Data will auto-sync to ClickHouse")
    else:
        print_banner("❌ TEST FAILED")
        print("Check the errors above for details.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n🛑 Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
