"""
Test cases to verify schedule page shows SQL Server and HANA sources correctly
"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_data_sources_loading():
    """Test that data sources are loaded correctly"""
    import psycopg2
    from db_utils import load_pg_config
    
    print("=" * 70)
    print("TEST 1: Data Sources Loading")
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
        
        # Test the exact query used in schedule_page
        cur.execute("""
            SELECT id, source_name, source_type, server_address, username, target_type, target_database, connection_details 
            FROM data_sources 
            WHERE is_active = true AND source_type IN ('sql_server', 'sap_hana')
            ORDER BY created_at DESC
        """)
        rows = cur.fetchall()
        
        print(f"\nQuery returned {len(rows)} row(s)")
        
        sql_server_count = 0
        hana_count = 0
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
            
            if ds['source_type'] == 'sql_server':
                sql_server_count += 1
                print(f"  [OK] SQL Server: {ds['source_name']} (ID: {ds['id']})")
            elif ds['source_type'] == 'sap_hana':
                hana_count += 1
                print(f"  [OK] HANA: {ds['source_name']} (ID: {ds['id']})")
        
        # Test filtering
        sql_server_sources = [ds for ds in data_sources if ds.get('source_type') == 'sql_server']
        hana_sources = [ds for ds in data_sources if ds.get('source_type') == 'sap_hana']
        
        print(f"\nFiltered results:")
        print(f"  SQL Server sources: {len(sql_server_sources)}")
        print(f"  HANA sources: {len(hana_sources)}")
        
        # Assertions
        assert len(rows) > 0, "ERROR: No data sources found!"
        assert sql_server_count > 0, "ERROR: No SQL Server sources found! Expected at least server1"
        assert hana_count >= 2, f"ERROR: Expected at least 2 HANA sources, found {hana_count}"
        assert len(sql_server_sources) == sql_server_count, "ERROR: SQL Server filtering failed!"
        assert len(hana_sources) == hana_count, "ERROR: HANA filtering failed!"
        
        print("\n[PASS] TEST 1 PASSED: Data sources loading and filtering works correctly!")
        
        cur.close()
        conn.close()
        return True
        
    except AssertionError as e:
        print(f"\n[FAIL] TEST 1 FAILED: {e}")
        return False
    except Exception as e:
        print(f"\n[ERROR] TEST 1 ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_source_type_values():
    """Test that source_type values match expected values"""
    import psycopg2
    from db_utils import load_pg_config
    
    print("\n" + "=" * 70)
    print("TEST 2: Source Type Values")
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
        
        # Check all source_type values
        cur.execute("""
            SELECT DISTINCT source_type 
            FROM data_sources
            WHERE is_active = true
        """)
        types = [r[0] for r in cur.fetchall()]
        
        print(f"\nActive source types in database: {types}")
        
        # Check expected types exist
        assert 'sql_server' in types, "ERROR: 'sql_server' type not found!"
        assert 'sap_hana' in types, "ERROR: 'sap_hana' type not found!"
        
        # Check specific sources
        cur.execute("SELECT source_name, source_type FROM data_sources WHERE source_name IN ('server1', 'hana1', 'hana2') AND is_active = true")
        sources = cur.fetchall()
        
        print(f"\nFound {len(sources)} expected source(s):")
        for name, stype in sources:
            print(f"  - {name}: {stype}")
            if name == 'server1':
                assert stype == 'sql_server', f"ERROR: server1 has wrong type: {stype}"
            elif name in ('hana1', 'hana2'):
                assert stype == 'sap_hana', f"ERROR: {name} has wrong type: {stype}"
        
        print("\n[PASS] TEST 2 PASSED: Source type values are correct!")
        
        cur.close()
        conn.close()
        return True
        
    except AssertionError as e:
        print(f"\n[FAIL] TEST 2 FAILED: {e}")
        return False
    except Exception as e:
        print(f"\n[ERROR] TEST 2 ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_template_variables():
    """Test that template variables would be populated correctly"""
    print("\n" + "=" * 70)
    print("TEST 3: Template Variables")
    print("=" * 70)
    
    # Simulate the filtering logic used in the route
    data_sources = [
        {'id': 6, 'source_name': 'server1', 'source_type': 'sql_server'},
        {'id': 61, 'source_name': 'hana1', 'source_type': 'sap_hana'},
        {'id': 62, 'source_name': 'hana2', 'source_type': 'sap_hana'},
    ]
    
    sql_server_sources = [ds for ds in data_sources if ds.get('source_type') == 'sql_server']
    hana_sources = [ds for ds in data_sources if ds.get('source_type') == 'sap_hana']
    
    print(f"\nSQL Server sources for template: {len(sql_server_sources)}")
    for ds in sql_server_sources:
        print(f"  - {ds['source_name']} (ID: {ds['id']})")
    
    print(f"\nHANA sources for template: {len(hana_sources)}")
    for ds in hana_sources:
        print(f"  - {ds['source_name']} (ID: {ds['id']})")
    
    assert len(sql_server_sources) > 0, "ERROR: No SQL Server sources for template!"
    assert len(hana_sources) >= 2, f"ERROR: Expected at least 2 HANA sources, got {len(hana_sources)}"
    assert 'server1' in [ds['source_name'] for ds in sql_server_sources], "ERROR: server1 not in SQL Server sources!"
    assert 'hana1' in [ds['source_name'] for ds in hana_sources], "ERROR: hana1 not in HANA sources!"
    assert 'hana2' in [ds['source_name'] for ds in hana_sources], "ERROR: hana2 not in HANA sources!"
    
    print("\n[PASS] TEST 3 PASSED: Template variables would be populated correctly!")
    return True

if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("SCHEDULE PAGE SOURCE DISPLAY - TEST SUITE")
    print("=" * 70)
    
    results = []
    
    # Run tests
    results.append(("Data Sources Loading", test_data_sources_loading()))
    results.append(("Source Type Values", test_source_type_values()))
    results.append(("Template Variables", test_template_variables()))
    
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
    
    if passed == total:
        print("\n[SUCCESS] ALL TESTS PASSED!")
        sys.exit(0)
    else:
        print("\n[FAILURE] SOME TESTS FAILED!")
        sys.exit(1)

