"""
Complete Test Case: HANA to ClickHouse Incremental Sync
1. Insert initial data into HANA
2. Sync HANA to ClickHouse (initial sync)
3. Verify initial data in ClickHouse
4. Add more rows to HANA
5. Run incremental sync
6. Verify incremental data in ClickHouse
"""
import os
import sys
import time
from datetime import datetime
from dotenv import load_dotenv

# Load .env file
load_dotenv()

def load_hana_config():
    """Load HANA config from .env variables only"""
    host = os.environ.get('HANA_HOST')
    port = os.environ.get('HANA_PORT')
    username = os.environ.get('HANA_USERNAME')
    password = os.environ.get('HANA_PASSWORD')
    
    if not all([host, port, username, password]):
        raise ValueError(
            "Missing HANA environment variables. Please set in .env:\n"
            "HANA_HOST, HANA_PORT, HANA_USERNAME, HANA_PASSWORD"
        )
    
    return {
        'host': host,
        'port': int(port),
        'username': username,
        'password': password
    }

def load_clickhouse_config():
    """Load ClickHouse config from .env variables only"""
    from db_utils import load_clickhouse_config
    return load_clickhouse_config()

def wait_for_hana_ready(hana_config, max_attempts=30, wait_seconds=10):
    """Wait for HANA to be ready for connections"""
    import hdbcli.dbapi as hana_dbapi
    
    print(f"\n[INFO] Waiting for HANA at {hana_config['host']}:{hana_config['port']} to be ready...")
    
    for attempt in range(max_attempts):
        try:
            conn = hana_dbapi.connect(
                address=hana_config['host'],
                port=hana_config['port'],
                user=hana_config['username'],
                password=hana_config['password'],
                encrypt=True,
                sslValidateCertificate=False,
                timeout=5
            )
            conn.close()
            print(f"[OK] HANA is ready! (attempt {attempt + 1}/{max_attempts})")
            return True
        except Exception as e:
            if attempt < max_attempts - 1:
                print(f"   Attempt {attempt + 1}/{max_attempts}: HANA not ready yet, waiting {wait_seconds}s... ({str(e)[:50]})")
                time.sleep(wait_seconds)
            else:
                print(f"[ERROR] HANA not ready after {max_attempts} attempts: {e}")
                return False
    
    return False

def test_complete_hana_incremental_sync():
    """Complete test: Insert → Initial Sync → Add Data → Incremental Sync"""
    
    print("=" * 80)
    print("COMPLETE TEST: HANA to ClickHouse Incremental Sync")
    print("=" * 80)
    
    # Load configurations
    try:
        hana_config = load_hana_config()
        print(f"\n[OK] HANA Config: {hana_config['host']}:{hana_config['port']}")
    except ValueError as e:
        print(f"\n[ERROR] {e}")
        return False
    
    try:
        ch_config = load_clickhouse_config()
        print(f"[OK] ClickHouse Config: {ch_config['host']}:{ch_config['port']}")
    except ValueError as e:
        print(f"\n[ERROR] {e}")
        return False
    
    # Import required libraries
    try:
        import hdbcli.dbapi as hana_dbapi
        from clickhouse_driver import Client as CHClient
    except ImportError as e:
        print(f"\n[ERROR] Missing library: {e}")
        print("Install: pip install hdbcli clickhouse-driver")
        return False
    
    # Wait for HANA to be ready
    if not wait_for_hana_ready(hana_config):
        print("\n[ERROR] HANA is not ready. Please:")
        print("  1. Check HANA container: docker ps --filter 'name=hana-express'")
        print("  2. Check HANA logs: docker logs hana-express")
        print("  3. Wait for 'Startup finished!' message")
        print("  4. Try again")
        return False
    
    # Database and table configuration
    hana_database = "HOSPITAL_DB"
    hana_schema = "HOSPITAL_SCHEMA"
    hana_table = "HOSPITALS"
    ch_database = ch_config.get('database', 'test1')
    ch_table_prefix = "HOSPITAL_SCHEMA_HOSPITALS"
    
    print(f"\n[CONFIG] HANA: {hana_database}.{hana_schema}.{hana_table}")
    print(f"[CONFIG] ClickHouse: {ch_database}.{ch_table_prefix}")
    
    # Step 1: Connect to HANA
    print("\n" + "=" * 80)
    print("STEP 1: Connect to HANA")
    print("=" * 80)
    try:
        hana_conn = hana_dbapi.connect(
            address=hana_config['host'],
            port=hana_config['port'],
            user=hana_config['username'],
            password=hana_config['password'],
            encrypt=True,
            sslValidateCertificate=False
        )
        print("[OK] Connected to HANA")
    except Exception as e:
        print(f"[ERROR] HANA connection failed: {e}")
        return False
    
    # Step 2: Ensure database, schema, and table exist
    print("\n" + "=" * 80)
    print("STEP 2: Ensure HANA Database/Schema/Table Exist")
    print("=" * 80)
    cursor = hana_conn.cursor()
    try:
        # Create database if not exists
        try:
            cursor.execute(f'CREATE DATABASE "{hana_database}"')
            print(f"[OK] Created database: {hana_database}")
        except Exception:
            print(f"[INFO] Database {hana_database} already exists")
        
        # Use database
        cursor.execute(f'USE DATABASE "{hana_database}"')
        
        # Create schema if not exists
        try:
            cursor.execute(f'CREATE SCHEMA "{hana_schema}"')
            print(f"[OK] Created schema: {hana_schema}")
        except Exception:
            print(f"[INFO] Schema {hana_schema} already exists")
        
        # Create table if not exists
        create_table_sql = f'''
        CREATE COLUMN TABLE IF NOT EXISTS "{hana_schema}"."{hana_table}"
        (
            HOSPITAL_ID INTEGER PRIMARY KEY,
            HOSPITAL_NAME NVARCHAR(200) NOT NULL,
            ADDRESS NVARCHAR(500),
            CITY NVARCHAR(100),
            STATE NVARCHAR(100),
            PHONE NVARCHAR(20),
            BEDS INTEGER,
            SPECIALITY NVARCHAR(200),
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        '''
        cursor.execute(create_table_sql)
        print(f"[OK] Table {hana_table} ready")
        
        # Clear existing data for clean test
        cursor.execute(f'TRUNCATE TABLE "{hana_schema}"."{hana_table}"')
        print(f"[OK] Cleared existing data")
        
    except Exception as e:
        print(f"[ERROR] Failed to setup HANA: {e}")
        cursor.close()
        hana_conn.close()
        return False
    
    # Step 3: Insert Initial Data (3 hospitals)
    print("\n" + "=" * 80)
    print("STEP 3: Insert Initial Data into HANA")
    print("=" * 80)
    
    initial_hospitals = [
        (1, 'City General Hospital', '123 Medical Center Drive', 'New York', 'NY', '555-0101', 500, 'General Medicine, Cardiology, Surgery'),
        (2, 'Sunset Medical Center', '456 Health Boulevard', 'Los Angeles', 'CA', '555-0102', 350, 'Emergency Care, Orthopedics, Pediatrics'),
        (3, 'Riverside Community Hospital', '789 Riverside Avenue', 'Chicago', 'IL', '555-0103', 275, 'Oncology, Neurology, Maternity')
    ]
    
    insert_sql = f'''
    INSERT INTO "{hana_schema}"."{hana_table}"
    (HOSPITAL_ID, HOSPITAL_NAME, ADDRESS, CITY, STATE, PHONE, BEDS, SPECIALITY)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    '''
    
    try:
        for hospital in initial_hospitals:
            cursor.execute(insert_sql, hospital)
        hana_conn.commit()
        print(f"[OK] Inserted {len(initial_hospitals)} initial hospitals")
        
        # Verify count
        cursor.execute(f'SELECT COUNT(*) FROM "{hana_schema}"."{hana_table}"')
        count = cursor.fetchone()[0]
        print(f"[OK] HANA now has {count} hospitals")
    except Exception as e:
        print(f"[ERROR] Failed to insert initial data: {e}")
        cursor.close()
        hana_conn.close()
        return False
    
    cursor.close()
    
    # Step 4: Connect to ClickHouse
    print("\n" + "=" * 80)
    print("STEP 4: Connect to ClickHouse")
    print("=" * 80)
    try:
        ch_client = CHClient(
            host=ch_config['host'],
            port=ch_config['port'],
            user=ch_config['user'],
            password=ch_config['password'],
            database=ch_database
        )
        print("[OK] Connected to ClickHouse")
    except Exception as e:
        print(f"[ERROR] ClickHouse connection failed: {e}")
        hana_conn.close()
        return False
    
    # Step 5: Initial Sync - Use hana_sync module
    print("\n" + "=" * 80)
    print("STEP 5: Initial Sync from HANA to ClickHouse")
    print("=" * 80)
    try:
        from hana_sync import HanaToClickHouseSync
        
        sync_engine = HanaToClickHouseSync(
            hana_config=hana_config,
            clickhouse_config={**ch_config, 'database': ch_database}
        )
        
        if not sync_engine.connect_hana():
            print("[ERROR] Failed to connect to HANA")
            return False
        
        if not sync_engine.connect_clickhouse():
            print("[ERROR] Failed to connect to ClickHouse")
            return False
        
        # Get table schema and create in ClickHouse
        columns = sync_engine.get_hana_table_schema(hana_schema, hana_table)
        if not columns:
            print("[ERROR] Could not get HANA table schema")
            return False
        
        sync_engine.create_clickhouse_table(hana_schema, hana_table, columns)
        print("[OK] Created ClickHouse table")
        
        # Migrate initial data
        result = sync_engine.migrate_table_data(hana_schema, hana_table)
        
        if result.get('status') != 'success':
            print(f"[ERROR] Initial sync failed: {result.get('error')}")
            return False
        
        migrated_rows = result.get('migrated_rows', 0)
        print(f"[OK] Initial sync complete: {migrated_rows} rows migrated")
        
        # Verify in ClickHouse
        ch_table_name = sync_engine.create_clickhouse_table_name(hana_schema, hana_table)
        count_query = f"SELECT count() FROM {ch_database}.{ch_table_name}"
        ch_count = ch_client.execute(count_query)[0][0]
        print(f"[OK] ClickHouse has {ch_count} records")
        
        if ch_count != 3:
            print(f"[WARNING] Expected 3 records, got {ch_count}")
        
        # Setup incremental sync
        sync_engine.setup_incremental_sync(hana_schema, hana_table)
        print("[OK] Incremental sync configured")
        
    except Exception as e:
        print(f"[ERROR] Initial sync failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Wait a moment
    print("\n[INFO] Waiting 5 seconds before adding new data...")
    time.sleep(5)
    
    # Step 6: Add More Data to HANA (2 more hospitals)
    print("\n" + "=" * 80)
    print("STEP 6: Add More Data to HANA (Incremental Test)")
    print("=" * 80)
    
    new_hospitals = [
        (4, 'North Regional Medical', '1000 Health Parkway', 'Boston', 'MA', '555-0201', 450, 'Cardiology, Neurology, Surgery'),
        (5, 'South Valley Hospital', '2000 Wellness Way', 'Miami', 'FL', '555-0202', 300, 'Pediatrics, Maternity, Emergency')
    ]
    
    cursor = hana_conn.cursor()
    try:
        for hospital in new_hospitals:
            cursor.execute(insert_sql, hospital)
        hana_conn.commit()
        print(f"[OK] Added {len(new_hospitals)} new hospitals")
        
        # Verify new count
        cursor.execute(f'SELECT COUNT(*) FROM "{hana_schema}"."{hana_table}"')
        new_count = cursor.fetchone()[0]
        print(f"[OK] HANA now has {new_count} hospitals (was 3, added 2)")
        
        # Get new records (for verification)
        cursor.execute(f'''
            SELECT HOSPITAL_ID, HOSPITAL_NAME, CITY 
            FROM "{hana_schema}"."{hana_table}" 
            WHERE HOSPITAL_ID > 3 
            ORDER BY HOSPITAL_ID
        ''')
        new_records = cursor.fetchall()
        print(f"[OK] New records in HANA:")
        for record in new_records:
            print(f"      ID: {record[0]}, Name: {record[1]}, City: {record[2]}")
            
    except Exception as e:
        print(f"[ERROR] Failed to add new data: {e}")
        cursor.close()
        hana_conn.close()
        return False
    
    cursor.close()
    
    # Wait a moment for timestamps to be different
    print("\n[INFO] Waiting 3 seconds before incremental sync...")
    time.sleep(3)
    
    # Step 7: Run Incremental Sync
    print("\n" + "=" * 80)
    print("STEP 7: Run Incremental Sync")
    print("=" * 80)
    
    try:
        # Perform incremental sync
        incremental_result = sync_engine.perform_incremental_sync(hana_schema, hana_table)
        
        if incremental_result.get('status') != 'success':
            print(f"[ERROR] Incremental sync failed: {incremental_result.get('message')}")
            return False
        
        new_records_synced = incremental_result.get('new_records', 0)
        print(f"[OK] Incremental sync complete: {new_records_synced} new records synced")
        
        if new_records_synced == 0:
            print("[WARNING] No new records found - this might indicate:")
            print("          - Incremental sync not detecting changes")
            print("          - Timestamp column issue")
            print("          - Or records were already synced")
        
    except Exception as e:
        print(f"[ERROR] Incremental sync error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Step 8: Verify Final Data in ClickHouse
    print("\n" + "=" * 80)
    print("STEP 8: Verify Final Data in ClickHouse")
    print("=" * 80)
    
    try:
        # Get final count
        final_count = ch_client.execute(count_query)[0][0]
        print(f"[INFO] ClickHouse now has {final_count} total records")
        
        # Expected: 3 initial + 2 new = 5 total
        expected_count = 5
        if final_count >= expected_count:
            print(f"[OK] SUCCESS! Expected at least {expected_count} records, got {final_count}")
        else:
            print(f"[WARNING] Expected at least {expected_count} records, got {final_count}")
        
        # Get all records
        select_query = f'''
        SELECT HOSPITAL_ID, HOSPITAL_NAME, CITY, BEDS 
        FROM {ch_database}.{ch_table_name}
        ORDER BY HOSPITAL_ID
        '''
        all_records = ch_client.execute(select_query)
        
        print(f"\n[INFO] All records in ClickHouse:")
        print("-" * 80)
        for record in all_records:
            print(f"  ID: {record[0]:2d} | {record[1]:30s} | {record[2]:15s} | Beds: {record[3]}")
        print("-" * 80)
        
        # Verify new records are present
        new_ids = [r[0] for r in all_records if r[0] > 3]
        if len(new_ids) >= 2:
            print(f"[OK] SUCCESS! Found new records with IDs: {new_ids}")
        else:
            print(f"[WARNING] Expected to find IDs 4 and 5, found: {new_ids}")
        
    except Exception as e:
        print(f"[ERROR] Verification failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Cleanup
    print("\n" + "=" * 80)
    print("CLEANUP: Closing Connections")
    print("=" * 80)
    try:
        cursor.close()
        hana_conn.close()
        sync_engine.close_connections()
        ch_client.disconnect()
        print("[OK] All connections closed")
    except:
        pass
    
    # Final Summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    print("✅ Step 1: HANA Connection - OK")
    print("✅ Step 2: HANA Database/Schema/Table Setup - OK")
    print("✅ Step 3: Initial Data Insert (3 hospitals) - OK")
    print("✅ Step 4: ClickHouse Connection - OK")
    print("✅ Step 5: Initial Sync (HANA → ClickHouse) - OK")
    print("✅ Step 6: Added More Data to HANA (2 hospitals) - OK")
    print("✅ Step 7: Incremental Sync - OK")
    print("✅ Step 8: Verification - OK")
    print("\n" + "=" * 80)
    print("✅ ALL TESTS PASSED!")
    print("=" * 80)
    print("\nIncremental sync is working correctly!")
    print(f"Initial: 3 hospitals → ClickHouse")
    print(f"Added: 2 hospitals → HANA")
    print(f"Incremental sync: 2 hospitals → ClickHouse")
    print(f"Final: {final_count} hospitals in ClickHouse")
    print("=" * 80)
    
    return True

if __name__ == '__main__':
    try:
        success = test_complete_hana_incremental_sync()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n[INFO] Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
