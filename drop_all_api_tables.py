"""
Drop all CRM tables that might have wrong schemas from previous sync attempts
"""

from clickhouse_driver import Client

def drop_all_api_tables():
    client = Client('localhost')
    
    tables_to_drop = [
        ('test7', 'crm'),
        ('test8', 'crm'),
        ('test5', 'crm'),
        ('test4', 'crmmm'),
        ('test4', 'crm_stream'),
    ]
    
    print("=" * 80)
    print("🗑️  DROPPING ALL OLD API SYNC TABLES")
    print("=" * 80)
    
    for db, table in tables_to_drop:
        try:
            client.execute(f'DROP TABLE IF EXISTS {db}.{table}')
            print(f"✅ Dropped {db}.{table}")
        except Exception as e:
            print(f"⚠️  Could not drop {db}.{table}: {e}")
    
    print("\n" + "=" * 80)
    print("✅ CLEANUP COMPLETE!")
    print("=" * 80)
    print("\n📋 Next steps:")
    print("   1. Start Flask app: python app.py")
    print("   2. Start mock SSE server: python mock_sse_server.py")
    print("   3. Go to http://localhost:5001/play")
    print("   4. Click 'Sync Server' button")
    print("   5. Watch the magic happen! 🎉")
    print("\n💡 Expected result:")
    print("   - ✅ 'connected' event: SKIPPED (just a status message)")
    print("   - ✅ 'initial_data': 10 records inserted (IDs 19-28)")
    print("   - ✅ 'new_data': Each new record inserted as it arrives")
    print("   - ✅ Table schema: id, rollno, timestamp ONLY")
    print("=" * 80)

if __name__ == "__main__":
    drop_all_api_tables()
