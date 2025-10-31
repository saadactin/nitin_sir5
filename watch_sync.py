"""Watch the sync in real-time"""
from clickhouse_driver import Client
import time

ch = Client(host='localhost')

print("Watching test1.crm table for changes...")
print("Checking every 3 seconds for 30 seconds\n")

prev_count = 0
for i in range(10):
    try:
        result = ch.execute("SELECT count() FROM test1.crm")
        count = result[0][0]
        change = count - prev_count
        
        status = "[GROWING +{}]".format(change) if change > 0 else "[NO CHANGE]"
        print(f"Check {i+1}/10: {count} rows {status}")
        
        if change > 0 and i > 0:
            # Show latest records
            latest = ch.execute("SELECT id, _sync_timestamp FROM test1.crm ORDER BY _sync_timestamp DESC LIMIT 2")
            print(f"  Latest IDs: {[r[0] for r in latest]}")
        
        prev_count = count
    except Exception as e:
        print(f"Error: {e}")
    
    if i < 9:
        time.sleep(3)

print("\nDone! Final row count: {}".format(prev_count))
print("If rows are increasing, sync is working!")
print("Query in ClickHouse: SELECT * FROM test1.crm LIMIT 100")
