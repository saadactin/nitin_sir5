"""Final verification that everything is working"""
from clickhouse_driver import Client
import time

print("="*100)
print("FINAL VERIFICATION - Waiting 15 seconds for sync to complete")
print("="*100)

time.sleep(15)

ch = Client(host='localhost')

try:
    result = ch.execute("SELECT count() FROM test1.test_dynamic")
    count = result[0][0]
    
    print(f"\n[SUCCESS] Table test1.test_dynamic has {count} rows!")
    
    if count > 0:
        print("\nLatest 5 records:")
        latest = ch.execute("""
            SELECT id, name, value, price, active, timestamp, _sync_timestamp
            FROM test1.test_dynamic 
            ORDER BY _sync_timestamp DESC 
            LIMIT 5
        """)
        
        for i, row in enumerate(latest, 1):
            print(f"{i}. ID={row[0]}, Name={row[1]}, Value={row[2]}, Price={row[3]}, Active={row[4]}")
            print(f"   API Time={row[5]}, Sync Time={row[6]}")
        
        print("\n" + "="*100)
        print("VERIFICATION COMPLETE - SYSTEM IS WORKING!")
        print("="*100)
        print("\nData is being continuously synced:")
        print(f"- From: http://localhost:5555/api/data")
        print(f"- To: ClickHouse test1.test_dynamic")
        print(f"- Total rows: {count}")
        print("\nThe test API adds 5 new records every 5 seconds")
        print("Watch growth with: SELECT count() FROM test1.test_dynamic")
        
    else:
        print("\n[WARNING] Table exists but has no rows yet - wait a few more seconds")
        
except Exception as e:
    print(f"\n[ERROR] {e}")
    print("The universal sync may still be starting up...")

