"""
Test both SQL Server and HANA scheduling to ensure neither fails
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scheduler_utils import schedule_source_interval_sync, schedule_source_daily_sync
import psycopg2
from db_utils import load_pg_config

def test_both_source_scheduling():
    """Test that both SQL Server and HANA sources can be scheduled"""
    print("=" * 70)
    print("TESTING BOTH SQL SERVER AND HANA SCHEDULING")
    print("=" * 70)
    
    try:
        # Get sources from database
        pg_conf = load_pg_config()
        conn = psycopg2.connect(
            dbname=pg_conf.get('database', 'metrics_sync_tables'),
            user=pg_conf.get('username'),
            password=pg_conf.get('password'),
            host=pg_conf.get('host'),
            port=int(pg_conf.get('port', 5432))
        )
        cur = conn.cursor()
        
        # Get SQL Server source
        cur.execute("""
            SELECT id, source_name, source_type 
            FROM data_sources 
            WHERE source_type = 'sql_server' AND is_active = true
            LIMIT 1
        """)
        sql_row = cur.fetchone()
        
        # Get HANA source
        cur.execute("""
            SELECT id, source_name, source_type 
            FROM data_sources 
            WHERE source_type = 'sap_hana' AND is_active = true
            LIMIT 1
        """)
        hana_row = cur.fetchone()
        
        cur.close()
        conn.close()
        
        results = []
        
        # Test SQL Server scheduling
        if sql_row:
            sql_id = sql_row[0]
            sql_name = sql_row[1]
            print(f"\n[TEST] SQL Server source: {sql_name} (ID: {sql_id})")
            
            print("  Testing interval scheduling...")
            try:
                schedule_source_interval_sync(sql_id, 15)
                print("  [PASS] SQL Server interval scheduling works")
                results.append(("SQL Server Interval", True))
            except Exception as e:
                print(f"  [FAIL] SQL Server interval scheduling failed: {e}")
                results.append(("SQL Server Interval", False))
                import traceback
                traceback.print_exc()
            
            print("  Testing daily scheduling...")
            try:
                schedule_source_daily_sync(sql_id, 15, 0)
                print("  [PASS] SQL Server daily scheduling works")
                results.append(("SQL Server Daily", True))
            except Exception as e:
                print(f"  [FAIL] SQL Server daily scheduling failed: {e}")
                results.append(("SQL Server Daily", False))
                import traceback
                traceback.print_exc()
        else:
            print("\n[SKIP] No SQL Server sources found")
        
        # Test HANA scheduling
        if hana_row:
            hana_id = hana_row[0]
            hana_name = hana_row[1]
            print(f"\n[TEST] HANA source: {hana_name} (ID: {hana_id})")
            
            print("  Testing interval scheduling...")
            try:
                schedule_source_interval_sync(hana_id, 20)
                print("  [PASS] HANA interval scheduling works")
                results.append(("HANA Interval", True))
            except Exception as e:
                print(f"  [FAIL] HANA interval scheduling failed: {e}")
                results.append(("HANA Interval", False))
                import traceback
                traceback.print_exc()
            
            print("  Testing daily scheduling...")
            try:
                schedule_source_daily_sync(hana_id, 16, 0)
                print("  [PASS] HANA daily scheduling works")
                results.append(("HANA Daily", True))
            except Exception as e:
                print(f"  [FAIL] HANA daily scheduling failed: {e}")
                results.append(("HANA Daily", False))
                import traceback
                traceback.print_exc()
        else:
            print("\n[SKIP] No HANA sources found")
        
        # Summary
        print("\n" + "=" * 70)
        print("TEST SUMMARY")
        print("=" * 70)
        
        passed = sum(1 for _, result in results if result)
        total = len(results)
        
        for test_name, result in results:
            status = "PASSED" if result else "FAILED"
            print(f"  {test_name}: {status}")
        
        print(f"\nTotal: {passed}/{total} tests passed")
        
        if passed == total and total > 0:
            print("\n[SUCCESS] All scheduling tests passed!")
            print("\nNote: Schedules are created regardless of source connection status.")
            print("If sources are offline, scheduled jobs will attempt connection and fail gracefully with logging.")
            return True
        elif total == 0:
            print("\n[SKIP] No sources available for testing")
            return True
        else:
            print("\n[FAILURE] Some tests failed!")
            return False
        
    except Exception as e:
        print(f"\n[ERROR] Test suite failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    result = test_both_source_scheduling()
    sys.exit(0 if result else 1)

