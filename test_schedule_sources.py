"""
Test script to check data_sources table and create dummy HANA sources
"""
import psycopg2
import json
from db_utils import load_pg_config, encrypt_password

def check_data_sources():
    """Check what sources exist in the database"""
    try:
        pg_conf = load_pg_config()
        conn = psycopg2.connect(
            dbname=pg_conf.get('database', 'metrics_sync_tables'),
            user=pg_conf.get('username'),
            password=pg_conf.get('password'),
            host=pg_conf.get('host'),
            port=int(pg_conf.get('port', 5432))
        )
        cur = conn.cursor()
        
        # Check table structure
        print("=" * 70)
        print("Checking data_sources table structure...")
        print("=" * 70)
        cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'data_sources'
            ORDER BY ordinal_position
        """)
        columns = cur.fetchall()
        print("\nTable columns:")
        for col_name, col_type in columns:
            print(f"  - {col_name}: {col_type}")
        
        # Check all sources
        print("\n" + "=" * 70)
        print("All data_sources records:")
        print("=" * 70)
        cur.execute("""
            SELECT id, source_name, source_type, server_address, username, 
                   target_type, target_database, is_active, created_at
            FROM data_sources 
            ORDER BY created_at DESC
        """)
        rows = cur.fetchall()
        if rows:
            print(f"\nFound {len(rows)} source(s):\n")
            for r in rows:
                print(f"ID: {r[0]}")
                print(f"  Name: {r[1]}")
                print(f"  Type: {r[2]}")
                print(f"  Server: {r[3]}")
                print(f"  Username: {r[4]}")
                print(f"  Target Type: {r[5]}")
                print(f"  Target DB: {r[6]}")
                print(f"  Active: {r[7]}")
                print(f"  Created: {r[8]}")
                print()
        else:
            print("\nNo sources found in database!")
        
        # Check SQL Server sources
        print("=" * 70)
        print("SQL Server sources (active only):")
        print("=" * 70)
        cur.execute("""
            SELECT id, source_name, source_type, server_address 
            FROM data_sources 
            WHERE is_active = true AND source_type = 'sql_server'
            ORDER BY created_at DESC
        """)
        sql_rows = cur.fetchall()
        if sql_rows:
            print(f"\nFound {len(sql_rows)} SQL Server source(s):")
            for r in sql_rows:
                print(f"  - {r[1]} (ID: {r[0]}, Type: {r[2]}, Address: {r[3]})")
        else:
            print("\nNo SQL Server sources found!")
        
        # Check HANA sources
        print("\n" + "=" * 70)
        print("HANA sources (active only):")
        print("=" * 70)
        cur.execute("""
            SELECT id, source_name, source_type, server_address 
            FROM data_sources 
            WHERE is_active = true AND source_type = 'sap_hana'
            ORDER BY created_at DESC
        """)
        hana_rows = cur.fetchall()
        if hana_rows:
            print(f"\nFound {len(hana_rows)} HANA source(s):")
            for r in hana_rows:
                print(f"  - {r[1]} (ID: {r[0]}, Type: {r[2]}, Address: {r[3]})")
        else:
            print("\nNo HANA sources found!")
        
        # Check what source_type values exist
        print("\n" + "=" * 70)
        print("Unique source_type values in database:")
        print("=" * 70)
        cur.execute("""
            SELECT DISTINCT source_type, COUNT(*) as count
            FROM data_sources
            GROUP BY source_type
        """)
        type_rows = cur.fetchall()
        if type_rows:
            for r in type_rows:
                print(f"  - '{r[0]}': {r[1]} record(s)")
        else:
            print("  No records found")
        
        cur.close()
        conn.close()
        return rows, sql_rows, hana_rows
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        return None, None, None

def create_dummy_hana_sources():
    """Create 2 dummy HANA sources for testing"""
    try:
        pg_conf = load_pg_config()
        conn = psycopg2.connect(
            dbname=pg_conf.get('database', 'metrics_sync_tables'),
            user=pg_conf.get('username'),
            password=pg_conf.get('password'),
            host=pg_conf.get('host'),
            port=int(pg_conf.get('port', 5432))
        )
        cur = conn.cursor()
        
        # Check if hana1 and hana2 already exist
        cur.execute("SELECT id FROM data_sources WHERE source_name IN ('hana1', 'hana2')")
        existing = cur.fetchall()
        if existing:
            print(f"\nDummy HANA sources already exist. Found IDs: {[r[0] for r in existing]}")
            print("Skipping creation...")
            cur.close()
            conn.close()
            return
        
        # Create hana1
        print("\nCreating dummy HANA source: hana1")
        hana1_conn_details = json.dumps({
            'host': '192.168.1.100',
            'port': 30015
        })
        hana1_password = encrypt_password('HanaTest123')
        
        cur.execute("""
            INSERT INTO data_sources 
            (source_name, source_type, server_address, username, password, 
             target_type, target_database, connection_details, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            'hana1',
            'sap_hana',
            '192.168.1.100:30015',
            'SYSTEM',
            hana1_password,
            'clickhouse',
            'hana_test_db',
            hana1_conn_details,
            True
        ))
        hana1_id = cur.fetchone()[0]
        print(f"  Created hana1 with ID: {hana1_id}")
        
        # Create hana2
        print("\nCreating dummy HANA source: hana2")
        hana2_conn_details = json.dumps({
            'host': '192.168.1.101',
            'port': 30015
        })
        hana2_password = encrypt_password('HanaTest456')
        
        cur.execute("""
            INSERT INTO data_sources 
            (source_name, source_type, server_address, username, password, 
             target_type, target_database, connection_details, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            'hana2',
            'sap_hana',
            '192.168.1.101:30015',
            'SYSTEM',
            hana2_password,
            'clickhouse',
            'hana_test_db2',
            hana2_conn_details,
            True
        ))
        hana2_id = cur.fetchone()[0]
        print(f"  Created hana2 with ID: {hana2_id}")
        
        conn.commit()
        cur.close()
        conn.close()
        
        print("\n✓ Successfully created 2 dummy HANA sources!")
        print(f"  - hana1 (ID: {hana1_id})")
        print(f"  - hana2 (ID: {hana2_id})")
        
    except Exception as e:
        print(f"\nERROR creating dummy HANA sources: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("DATA SOURCES DIAGNOSTIC TOOL")
    print("=" * 70)
    
    # Check existing sources
    all_sources, sql_sources, hana_sources = check_data_sources()
    
    # Create dummy HANA sources if needed
    print("\n" + "=" * 70)
    print("Creating dummy HANA sources...")
    print("=" * 70)
    create_dummy_hana_sources()
    
    # Re-check after creation
    print("\n" + "=" * 70)
    print("Re-checking after dummy creation...")
    print("=" * 70)
    check_data_sources()

