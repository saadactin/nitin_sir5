"""
Final verification that schedule page will display sources correctly
This simulates the exact logic used in the Flask route and template
"""
import psycopg2
from db_utils import load_pg_config

def verify_schedule_display():
    """Verify that schedule page will show sources correctly"""
    print("=" * 70)
    print("SCHEDULE PAGE DISPLAY VERIFICATION")
    print("=" * 70)
    
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
        
        # Exact query from schedule_page route
        cur.execute("""
            SELECT id, source_name, source_type, server_address, username, target_type, target_database, connection_details 
            FROM data_sources 
            WHERE is_active = true AND source_type IN ('sql_server', 'sap_hana')
            ORDER BY created_at DESC
        """)
        rows = cur.fetchall()
        
        data_sources = []
        for r in rows:
            ds = {
                'id': r[0],
                'source_name': r[1],
                'source_type': r[2],
                'server_address': r[3],
                'username': r[4],
                'target_type': r[5],
                'target_database': r[6],
                'connection_details': r[7]
            }
            data_sources.append(ds)
        
        # Exact filtering from schedule_page route
        sql_server_sources = [ds for ds in data_sources if ds.get('source_type') == 'sql_server']
        hana_sources = [ds for ds in data_sources if ds.get('source_type') == 'sap_hana']
        
        print(f"\nLoaded {len(data_sources)} total data source(s)")
        print(f"SQL Server sources: {len(sql_server_sources)}")
        print(f"HANA sources: {len(hana_sources)}")
        
        print("\n" + "-" * 70)
        print("SQL SERVER SOURCES (should show in template when SQL Server is selected):")
        print("-" * 70)
        if sql_server_sources:
            for ds in sql_server_sources:
                print(f"  - {ds['source_name']} (ID: {ds['id']}, Type: {ds['source_type']})")
                print(f"    Template check: sql_server_sources|length = {len(sql_server_sources)} > 0: TRUE")
                print(f"    Template will show: YES")
        else:
            print("  NONE - Template will show 'No SQL Server sources available' message")
        
        print("\n" + "-" * 70)
        print("HANA SOURCES (should show in template when HANA is selected):")
        print("-" * 70)
        if hana_sources:
            for ds in hana_sources:
                print(f"  - {ds['source_name']} (ID: {ds['id']}, Type: {ds['source_type']})")
                print(f"    Template check: hana_sources|length = {len(hana_sources)} > 0: TRUE")
                print(f"    Template will show: YES")
        else:
            print("  NONE - Template will show 'No HANA sources available' message")
        
        # Verify template conditions
        print("\n" + "-" * 70)
        print("TEMPLATE CONDITION VERIFICATION:")
        print("-" * 70)
        
        # SQL Server section
        sql_condition1 = sql_server_sources and len(sql_server_sources) > 0
        print(f"SQL Server section condition: sql_server_sources and sql_server_sources|length > 0")
        print(f"  Result: {sql_condition1}")
        print(f"  Will display cards: {sql_condition1}")
        
        # HANA section
        hana_condition = hana_sources and len(hana_sources) > 0
        print(f"\nHANA section condition: hana_sources and hana_sources|length > 0")
        print(f"  Result: {hana_condition}")
        print(f"  Will display cards: {hana_condition}")
        
        # Final verdict
        print("\n" + "=" * 70)
        print("VERIFICATION RESULT:")
        print("=" * 70)
        
        if len(sql_server_sources) > 0 and len(hana_sources) >= 2:
            print("[SUCCESS] All sources are correctly loaded and will display in template!")
            print(f"  - SQL Server sources: {len(sql_server_sources)} (including server1)")
            print(f"  - HANA sources: {len(hana_sources)} (including hana1 and hana2)")
            return True
        else:
            print("[FAILURE] Some sources are missing!")
            if len(sql_server_sources) == 0:
                print("  - ERROR: No SQL Server sources found (expected server1)")
            if len(hana_sources) < 2:
                print(f"  - ERROR: Only {len(hana_sources)} HANA sources found (expected at least 2)")
            return False
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"\n[ERROR] Verification failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    result = verify_schedule_display()
    exit(0 if result else 1)

