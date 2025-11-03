"""
Test HANA scheduling functionality
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scheduler_utils import schedule_source_interval_sync, schedule_source_daily_sync
import psycopg2
from db_utils import load_pg_config

def test_hana_scheduling():
    """Test that HANA sources can be scheduled"""
    print("=" * 70)
    print("TESTING HANA SCHEDULING")
    print("=" * 70)
    
    try:
        # Get HANA sources from database
        pg_conf = load_pg_config()
        conn = psycopg2.connect(
            dbname=pg_conf.get('database', 'metrics_sync_tables'),
            user=pg_conf.get('username'),
            password=pg_conf.get('password'),
            host=pg_conf.get('host'),
            port=int(pg_conf.get('port', 5432))
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT id, source_name, source_type 
            FROM data_sources 
            WHERE source_type = 'sap_hana' AND is_active = true
            LIMIT 1
        """)
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        if not row:
            print("\n[SKIP] No HANA sources found in database")
            print("Create a HANA source first to test scheduling")
            return True
        
        hana_id = row[0]
        hana_name = row[1]
        
        print(f"\nFound HANA source: {hana_name} (ID: {hana_id})")
        
        # Test interval scheduling
        print("\n" + "-" * 70)
        print("Testing interval scheduling...")
        print("-" * 70)
        try:
            schedule_source_interval_sync(hana_id, 5)
            print(f"[PASS] Successfully scheduled interval sync for {hana_name}")
        except Exception as e:
            print(f"[FAIL] Failed to schedule interval sync: {e}")
            import traceback
            traceback.print_exc()
            return False
        
        # Test daily scheduling
        print("\n" + "-" * 70)
        print("Testing daily scheduling...")
        print("-" * 70)
        try:
            schedule_source_daily_sync(hana_id, 14, 30)
            print(f"[PASS] Successfully scheduled daily sync for {hana_name}")
        except Exception as e:
            print(f"[FAIL] Failed to schedule daily sync: {e}")
            import traceback
            traceback.print_exc()
            return False
        
        print("\n" + "=" * 70)
        print("[SUCCESS] HANA scheduling tests passed!")
        print("=" * 70)
        print("\nNote: Schedule is created regardless of HANA connection status.")
        print("If HANA is offline, the scheduled job will attempt connection and fail gracefully with logging.")
        
        return True
        
    except Exception as e:
        print(f"\n[ERROR] Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    result = test_hana_scheduling()
    sys.exit(0 if result else 1)

