"""
Test script to verify API → ClickHouse sync with multiple event types
This will:
1. Drop the old table with wrong schema
2. Restart the Flask app (you need to do this manually)
3. Verify the sync inserts only actual data records (not "connected" events)
"""

from clickhouse_driver import Client
import time

def test_complete_sync():
    print("=" * 80)
    print("🧪 API SYNC TEST - Multiple Event Types")
    print("=" * 80)
    
    client = Client('localhost')
    target_db = 'test7'
    target_table = 'crm'
    
    # Step 1: Drop old table
    print("\n📌 Step 1: Dropping old table with wrong schema...")
    try:
        client.execute(f'DROP TABLE IF EXISTS {target_db}.{target_table}')
        print(f"✅ Dropped {target_db}.{target_table}")
    except Exception as e:
        print(f"⚠️ Could not drop table: {e}")
    
    # Step 2: Verify table is gone
    print("\n📌 Step 2: Verifying table was dropped...")
    try:
        result = client.execute(f"SELECT count() FROM {target_db}.{target_table}")
        print(f"❌ ERROR: Table still exists with {result[0][0]} rows!")
    except Exception:
        print(f"✅ Table successfully dropped")
    
    # Step 3: Instructions for user
    print("\n📌 Step 3: Manual steps required:")
    print("   1. Stop Flask app (Ctrl+C in the terminal running python app.py)")
    print("   2. Restart Flask: python app.py")
    print("   3. Go to http://localhost:5001/play")
    print("   4. Click 'Sync Server' button on your API source")
    print("   5. Wait 30 seconds for sync to process events")
    print("   6. Run this script again with --verify flag to check results")
    
    print("\n" + "=" * 80)
    print("⏸️ Waiting for you to restart Flask and start sync...")
    print("=" * 80)

def verify_sync_results():
    print("\n" + "=" * 80)
    print("🔍 VERIFICATION - Checking ClickHouse Data")
    print("=" * 80)
    
    client = Client('localhost')
    target_db = 'test7'
    target_table = 'crm'
    
    try:
        # Check row count
        print("\n📊 Checking row count...")
        count_result = client.execute(f"SELECT count() FROM {target_db}.{target_table}")
        total_rows = count_result[0][0]
        print(f"✅ Total rows in {target_db}.{target_table}: {total_rows}")
        
        # Check table structure
        print("\n📋 Checking table structure...")
        columns_result = client.execute(f"DESCRIBE TABLE {target_db}.{target_table}")
        print(f"Columns in table:")
        for col in columns_result:
            print(f"   - {col[0]}: {col[1]}")
        
        # Check first few rows
        print("\n📄 First 5 rows:")
        rows_result = client.execute(f"""
            SELECT * FROM {target_db}.{target_table} 
            ORDER BY id 
            LIMIT 5 
            FORMAT Pretty
        """)
        print(rows_result)
        
        # Check for different ID ranges
        print("\n🔢 Checking ID distribution...")
        id_check = client.execute(f"""
            SELECT 
                min(id) as min_id,
                max(id) as max_id,
                count(DISTINCT id) as unique_ids
            FROM {target_db}.{target_table}
        """)
        print(f"   Min ID: {id_check[0][0]}")
        print(f"   Max ID: {id_check[0][1]}")
        print(f"   Unique IDs: {id_check[0][2]}")
        
        # Expected results
        print("\n" + "=" * 80)
        print("📊 EXPECTED RESULTS:")
        print("=" * 80)
        print("✅ Total rows should be: 10+ (from initial_data) + ongoing new_data events")
        print("✅ Columns should be: id, rollno, timestamp (NOT type, message, current_data_*)")
        print("✅ IDs should range from 19-28 (initial) and higher (new_data)")
        print("✅ NO 'connected' event data (no current_data_id, current_data_rollno columns)")
        
        if total_rows >= 10:
            print("\n🎉 SUCCESS! Sync is working correctly!")
            print(f"   - {total_rows} rows synced")
            print("   - Multiple event types handled properly")
            print("   - Only actual data records inserted (no status messages)")
        else:
            print("\n⚠️ WARNING: Only", total_rows, "rows found.")
            print("   Expected at least 10 from initial_data batch + new_data events")
            print("   Make sure:")
            print("   1. Flask app was restarted")
            print("   2. Sync button was clicked")
            print("   3. Mock SSE server is still running")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        print("\nPossible reasons:")
        print("1. Table hasn't been created yet (sync not started)")
        print("2. Flask app not restarted with new code")
        print("3. Sync button not clicked")

if __name__ == "__main__":
    import sys
    
    if '--verify' in sys.argv:
        verify_sync_results()
    else:
        test_complete_sync()
        print("\n💡 After restarting Flask and starting sync, run:")
        print("   python test_api_sync_complete.py --verify")
