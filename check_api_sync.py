"""
Check if API data has been synced to ClickHouse
"""
from clickhouse_driver import Client

try:
    # Connect to ClickHouse
    client = Client('localhost')
    
    print("=" * 60)
    print("CHECKING API SYNC STATUS")
    print("=" * 60)
    
    # Check if database exists
    databases = client.execute("SHOW DATABASES")
    db_list = [db[0] for db in databases]
    print(f"\n✓ Available databases: {', '.join(db_list)}")
    
    if 'test4' not in db_list:
        print("\n❌ Database 'test4' does not exist!")
        exit()
    
    # Use test4 database
    client.execute("USE test4")
    
    # Check tables
    tables = client.execute("SHOW TABLES")
    table_list = [t[0] for t in tables]
    print(f"✓ Tables in test4: {', '.join(table_list) if table_list else 'None'}")
    
    if not table_list:
        print("\n❌ No tables found! Sync hasn't started yet.")
        exit()
    
    # Check each table for data
    for table_name in table_list:
        print(f"\n{'='*60}")
        print(f"TABLE: {table_name}")
        print(f"{'='*60}")
        
        # Get row count
        count_result = client.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = count_result[0][0]
        print(f"✓ Total records: {row_count}")
        
        if row_count > 0:
            # Show sample data
            print(f"\nSample records (latest 5):")
            print("-" * 60)
            sample = client.execute(f"SELECT * FROM {table_name} ORDER BY deal_id DESC LIMIT 5")
            for i, row in enumerate(sample, 1):
                print(f"\nRecord {i}:")
                print(f"  {row}")
            
            # Get sync time range
            try:
                time_result = client.execute(f"SELECT MIN(created_date), MAX(created_date) FROM {table_name}")
                min_time, max_time = time_result[0]
                print(f"\n✓ Data time range:")
                print(f"  First: {min_time}")
                print(f"  Latest: {max_time}")
            except:
                pass
        else:
            print("❌ No data synced yet!")
    
    print("\n" + "=" * 60)
    print("SYNC CHECK COMPLETE")
    print("=" * 60)
    
except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()
