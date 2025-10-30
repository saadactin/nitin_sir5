"""
Watch the ClickHouse table to see new records being added in real-time
"""
from clickhouse_driver import Client
import time
from datetime import datetime

client = Client('localhost')
client.execute('USE test4')

print("=" * 70)
print("🔍 WATCHING FOR NEW RECORDS IN test4.crmmm")
print("=" * 70)
print("\nPress Ctrl+C to stop\n")

last_count = 0

try:
    while True:
        try:
            # Get current count
            result = client.execute('SELECT COUNT(*) FROM crmmm')
            current_count = result[0][0]
            
            if current_count != last_count:
                new_records = current_count - last_count
                
                # Get the latest records
                latest = client.execute(f'SELECT * FROM crmmm ORDER BY _sync_timestamp DESC LIMIT {new_records}')
                
                print(f"\n{'='*70}")
                print(f"🆕 {new_records} NEW RECORD(S) DETECTED! | Total: {current_count}")
                print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"{'='*70}")
                
                for i, record in enumerate(latest, 1):
                    print(f"\nRecord #{current_count - new_records + i}:")
                    print(f"  {record[:3]}...")  # Show first 3 fields
                
                last_count = current_count
            else:
                print(f"⏳ {datetime.now().strftime('%H:%M:%S')} | Count: {current_count} | Waiting for new data...", end='\r')
            
            time.sleep(2)  # Check every 2 seconds
            
        except Exception as e:
            print(f"\n❌ Error: {e}")
            time.sleep(2)
            
except KeyboardInterrupt:
    print("\n\n✋ Stopped watching")
    print(f"Final count: {last_count} records")
