"""
Check the actual columns in sync_history table
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
    print("SYNC_HISTORY TABLE STRUCTURE")
    print("=" * 80)
    
    # Get column names
    cursor.execute("""
        SELECT column_name, data_type, character_maximum_length
        FROM information_schema.columns
        WHERE table_schema = 'metrics_sync_tables'
        AND table_name = 'sync_history'
        ORDER BY ordinal_position
    """)
    
    columns = cursor.fetchall()
    print("\nColumns:")
    for col_name, data_type, max_length in columns:
        type_info = f"{data_type}"
        if max_length:
            type_info += f"({max_length})"
        print(f"   - {col_name:20s} {type_info}")
    
    print("\n" + "=" * 80)
    print("SAMPLE DATA (Last 3 records)")
    print("=" * 80)
    
    # Get all columns dynamically
    col_names = [col[0] for col in columns]
    col_list = ", ".join(col_names)
    
    cursor.execute(f"""
        SELECT {col_list}
        FROM metrics_sync_tables.sync_history 
        ORDER BY sync_time DESC
        LIMIT 3
    """)
    
    records = cursor.fetchall()
    for i, record in enumerate(records, 1):
        print(f"\nRecord {i}:")
        for col_name, value in zip(col_names, record):
            print(f"   {col_name}: {value}")
    
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 80)
    print("✅ Check complete!")
    print("=" * 80)

except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
