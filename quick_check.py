"""Quick check of data_sources directly from database"""
import psycopg2

try:
    # Load config same way app.py does
    import sys
    sys.path.insert(0, '.')
    from db_utils import load_pg_config
    
    pg_conf = load_pg_config()
    print(f"Connecting to database: {pg_conf.get('database')} on {pg_conf.get('host')}:{pg_conf.get('port')}")
    
    conn = psycopg2.connect(
        dbname=pg_conf.get('database', 'metrics_sync_tables'),
        user=pg_conf.get('username'),
        password=pg_conf.get('password'),
        host=pg_conf.get('host'),
        port=int(pg_conf.get('port', 5432))
    )
    
    cur = conn.cursor()
    
    # Check if table exists
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'data_sources'
        )
    """)
    table_exists = cur.fetchone()[0]
    
    if not table_exists:
        print("❌ Table 'data_sources' does NOT exist")
        print("\nCreating table...")
        cur.execute("""
            CREATE TABLE data_sources (
                id SERIAL PRIMARY KEY,
                source_name VARCHAR(255) UNIQUE NOT NULL,
                source_type VARCHAR(50) NOT NULL,
                server_address TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                target_type VARCHAR(50) NOT NULL,
                target_database VARCHAR(255) NOT NULL,
                connection_details JSONB,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        print("✓ Table created")
    else:
        print("✓ Table 'data_sources' exists")
    
    # Count rows
    cur.execute("SELECT COUNT(*) FROM data_sources")
    count = cur.fetchone()[0]
    print(f"\nTotal rows in data_sources: {count}")
    
    # Show all rows
    if count > 0:
        cur.execute("SELECT id, source_name, source_type, server_address, target_type, target_database, created_at FROM data_sources ORDER BY created_at DESC")
        rows = cur.fetchall()
        print("\nExisting data_sources:")
        print("-" * 80)
        for r in rows:
            print(f"ID: {r[0]} | Name: {r[1]} | Type: {r[2]}")
            print(f"  Server: {r[3]} | Target: {r[4]}/{r[5]}")
            print(f"  Created: {r[6]}")
            print("-" * 80)
    else:
        print("\n⚠ No data_sources found in database")
        print("\nInserting a test row...")
        cur.execute("""
            INSERT INTO data_sources 
            (source_name, source_type, server_address, username, password, target_type, target_database)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, ('test_quick_check', 'sql_server', '127.0.0.1', 'sa', 'test123', 'postgresql', 'postgres'))
        conn.commit()
        print("✓ Test row inserted")
        
        # Verify
        cur.execute("SELECT COUNT(*) FROM data_sources")
        new_count = cur.fetchone()[0]
        print(f"New count: {new_count}")
    
    cur.close()
    conn.close()
    print("\n✓ Database check complete")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
