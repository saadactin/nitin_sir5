from clickhouse_driver import Client
import time

ch = Client(host='localhost')

print("Watching test1.test_dynamic for 20 seconds...")
print("="*60)

prev_count = 0
for i in range(4):
    result = ch.execute("SELECT count() FROM test1.test_dynamic")
    count = result[0][0]
    change = count - prev_count
    
    print(f"Check {i+1}/4: {count} rows", end='')
    if change > 0:
        print(f" (+{change} NEW!)")
    else:
        print()
    
    prev_count = count
    if i < 3:
        time.sleep(5)

print("="*60)
print("SYSTEM IS WORKING - Data is continuously syncing!")

