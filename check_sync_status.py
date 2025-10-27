"""
Check what status values are in sync_history table
"""
import psycopg2

try:
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="test1",
        user="migration_user",
        password="StrongPassword123"
    )
    cursor = conn.cursor()
    
    print("=" * 80)
    print("CHECKING SYNC_HISTORY STATUS VALUES")
    print("=" * 80)
    
    # Get all distinct status values
    cursor.execute("""
        SELECT DISTINCT status, COUNT(*) 
        FROM metrics_sync_tables.sync_history 
        GROUP BY status
        ORDER BY COUNT(*) DESC
    """)
    
    print("\n📊 Status Distribution:")
    print("-" * 80)
    statuses = cursor.fetchall()
    for status, count in statuses:
        print(f"   Status: '{status}' | Count: {count}")
    
    # Get sample records
    print("\n" + "=" * 80)
    print("SAMPLE SYNC RECORDS (Last 5)")
    print("=" * 80)
    
    cursor.execute("""
        SELECT 
            server_name,
            status,
            sync_time,
            end_time,
            tables_synced,
            error_msg
        FROM metrics_sync_tables.sync_history 
        ORDER BY sync_time DESC
        LIMIT 5
    """)
    
    records = cursor.fetchall()
    for i, record in enumerate(records, 1):
        server, status, sync_time, end_time, tables, error = record
        print(f"\n{i}. Server: {server}")
        print(f"   Status: '{status}'")
        print(f"   Sync Time: {sync_time}")
        print(f"   End Time: {end_time}")
        print(f"   Tables Synced: {tables}")
        print(f"   Error: {error}")
    
    # Check today's syncs specifically
    print("\n" + "=" * 80)
    print("TODAY'S SYNCS")
    print("=" * 80)
    
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed,
            COUNT(CASE WHEN status = 'success' THEN 1 END) as success,
            COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed,
            COUNT(CASE WHEN status = 'error' THEN 1 END) as error_status,
            COUNT(CASE WHEN status = 'running' THEN 1 END) as running
        FROM metrics_sync_tables.sync_history 
        WHERE DATE(sync_time) = CURRENT_DATE
    """)
    
    result = cursor.fetchone()
    print(f"\nTotal: {result[0]}")
    print(f"Status='completed': {result[1]}")
    print(f"Status='success': {result[2]}")
    print(f"Status='failed': {result[3]}")
    print(f"Status='error': {result[4]}")
    print(f"Status='running': {result[5]}")
    
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 80)
    print("✅ Check complete!")
    print("=" * 80)

except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
