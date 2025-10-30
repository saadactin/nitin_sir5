"""
Fix all sync issues - Clean slate approach
"""
from clickhouse_driver import Client

try:
    client = Client('localhost')
    
    print("=" * 70)
    print("FIXING API SYNC ISSUES")
    print("=" * 70)
    
    # Drop all old tables with wrong schema
    tables_to_drop = [
        ('test5', 'crm'),
        ('test4', 'crmmm'),
        ('test4', 'crm_stream')
    ]
    
    for db, table in tables_to_drop:
        try:
            client.execute(f'DROP TABLE IF EXISTS {db}.{table}')
            print(f"✓ Dropped {db}.{table}")
        except Exception as e:
            print(f"  (Table {db}.{table} didn't exist: {e})")
    
    print("\n✅ All old tables cleaned up!")
    print("\n📝 Next steps:")
    print("1. Start mock server: python mock_sse_server.py")
    print("2. Start Flask: python app.py")
    print("3. Click 'Sync Server' button")
    print("4. Tables will be auto-created with correct schema!")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
