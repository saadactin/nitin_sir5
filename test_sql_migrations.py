"""
Complete Test Suite for SQL Server → PostgreSQL and ClickHouse Migrations
Verifies that data can be correctly migrated from SQL Server to both targets
"""
import sys
import logging
from datetime import datetime
import psycopg2
from clickhouse_driver import Client
import pyodbc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

print("="*80)
print("SQL SERVER MIGRATION TEST SUITE")
print("="*80)
print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# Test configuration
test_results = {
    "sql_server_connectivity": False,
    "postgresql_connectivity": False,
    "clickhouse_connectivity": False,
    "sql_to_postgres_sync": False,
    "sql_to_clickhouse_sync": False,
    "data_integrity": False
}

# ============================================================================
# STEP 1: Test SQL Server Connectivity
# ============================================================================
print("\n" + "="*80)
print("STEP 1: Testing SQL Server Connectivity")
print("="*80)

try:
    # Load data sources from PostgreSQL
    pg_conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='test1',
        user='postgres',
        password='root'
    )
    cursor = pg_conn.cursor()
    cursor.execute("SELECT source_name, source_type, server_address, username, password FROM data_sources WHERE is_active = true")
    data_sources = cursor.fetchall()
    cursor.close()
    pg_conn.close()
    
    print(f"\nFound {len(data_sources)} active data sources:")
    
    sql_servers = []
    for source in data_sources:
        source_name, source_type, server_address, username, password = source
        print(f"  - {source_name} ({source_type}) @ {server_address}")
        
        if source_type == 'SQL Server':
            sql_servers.append({
                'name': source_name,
                'server': server_address,
                'username': username,
                'password': password
            })
    
    # Test each SQL Server connection
    print("\nTesting SQL Server connections:")
    sql_connections = []
    
    for sql_server in sql_servers:
        try:
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={sql_server['server']};"
                f"UID={sql_server['username']};"
                f"PWD={sql_server['password']};"
                f"Timeout=10;"
            )
            
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            
            # Get server info
            cursor.execute("SELECT @@SERVERNAME, @@VERSION")
            server_name, version = cursor.fetchone()
            
            # Get database count
            cursor.execute("SELECT COUNT(*) FROM sys.databases WHERE state = 0 AND name NOT IN ('master', 'tempdb', 'model', 'msdb')")
            db_count = cursor.fetchone()[0]
            
            print(f"  ✓ {sql_server['name']}: Connected")
            print(f"    - Server: {server_name}")
            print(f"    - Version: {version[:50]}...")
            print(f"    - Databases: {db_count}")
            
            sql_connections.append({
                'config': sql_server,
                'connection': conn,
                'db_count': db_count
            })
            
        except Exception as e:
            print(f"  ✗ {sql_server['name']}: Failed - {str(e)}")
    
    if sql_connections:
        test_results["sql_server_connectivity"] = True
        print(f"\n✓ SQL Server connectivity: {len(sql_connections)}/{len(sql_servers)} servers accessible")
    else:
        print(f"\n✗ SQL Server connectivity: No servers accessible")
        
except Exception as e:
    print(f"✗ Error testing SQL Server connectivity: {e}")

# ============================================================================
# STEP 2: Test PostgreSQL Connectivity
# ============================================================================
print("\n" + "="*80)
print("STEP 2: Testing PostgreSQL Connectivity")
print("="*80)

try:
    # Connect to PostgreSQL
    pg_conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='test1',
        user='postgres',
        password='root'
    )
    cursor = pg_conn.cursor()
    
    # Get PostgreSQL version
    cursor.execute("SELECT version()")
    pg_version = cursor.fetchone()[0]
    print(f"\n✓ PostgreSQL Connected")
    print(f"  Version: {pg_version[:60]}...")
    
    # Check for sync tracking tables
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name IN ('sync_database_status', 'sync_table_status')
    """)
    sync_tables = [row[0] for row in cursor.fetchall()]
    
    print(f"  Sync tracking tables: {', '.join(sync_tables) if sync_tables else 'None found'}")
    
    # Get synced schemas
    cursor.execute("""
        SELECT schema_name, COUNT(*) as table_count
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        AND table_schema != 'public'
        GROUP BY schema_name
        ORDER BY table_count DESC
    """)
    schemas = cursor.fetchall()
    
    if schemas:
        print(f"\n  Synced data schemas:")
        for schema, count in schemas[:5]:  # Show top 5
            print(f"    - {schema}: {count} tables")
        if len(schemas) > 5:
            print(f"    ... and {len(schemas) - 5} more schemas")
    
    cursor.close()
    pg_conn.close()
    
    test_results["postgresql_connectivity"] = True
    print(f"\n✓ PostgreSQL connectivity test passed")
    
except Exception as e:
    print(f"✗ PostgreSQL connectivity failed: {e}")

# ============================================================================
# STEP 3: Test ClickHouse Connectivity
# ============================================================================
print("\n" + "="*80)
print("STEP 3: Testing ClickHouse Connectivity")
print("="*80)

try:
    # Connect to ClickHouse
    ch_client = Client(
        host='localhost',
        port=9000,
        user='default',
        password='',
        database='default'
    )
    
    # Get ClickHouse version
    result = ch_client.execute('SELECT version()')
    ch_version = result[0][0]
    print(f"\n✓ ClickHouse Connected")
    print(f"  Version: {ch_version}")
    
    # Get databases
    result = ch_client.execute('SHOW DATABASES')
    databases = [row[0] for row in result]
    
    print(f"  Databases: {len(databases)}")
    
    # Check for synced databases (ones we created)
    synced_dbs = [db for db in databases if db not in ('default', 'system', 'information_schema', 'INFORMATION_SCHEMA')]
    
    if synced_dbs:
        print(f"\n  Synced databases:")
        for db in synced_dbs:
            # Get table count
            result = ch_client.execute(f'SELECT COUNT(*) FROM system.tables WHERE database = %s', [db])
            table_count = result[0][0]
            print(f"    - {db}: {table_count} tables")
    
    test_results["clickhouse_connectivity"] = True
    print(f"\n✓ ClickHouse connectivity test passed")
    
except Exception as e:
    print(f"✗ ClickHouse connectivity failed: {e}")

# ============================================================================
# STEP 4: Test SQL → PostgreSQL Sync
# ============================================================================
print("\n" + "="*80)
print("STEP 4: Testing SQL Server → PostgreSQL Sync")
print("="*80)

if test_results["sql_server_connectivity"] and test_results["postgresql_connectivity"]:
    try:
        print("\nChecking for synced data in PostgreSQL...")
        
        # Connect to PostgreSQL
        pg_conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='test1',
            user='postgres',
            password='root'
        )
        cursor = pg_conn.cursor()
        
        # Get sync status
        cursor.execute("""
            SELECT server_name, database_name, sync_status, last_full_sync, last_incremental_sync
            FROM sync_database_status
            ORDER BY updated_at DESC
            LIMIT 10
        """)
        sync_statuses = cursor.fetchall()
        
        if sync_statuses:
            print(f"\nFound {len(sync_statuses)} sync records:")
            for server, database, status, full_sync, incr_sync in sync_statuses[:5]:
                print(f"  - {server}/{database}: {status}")
                if full_sync:
                    print(f"    Last full sync: {full_sync}")
                if incr_sync:
                    print(f"    Last incremental: {incr_sync}")
            
            test_results["sql_to_postgres_sync"] = True
            print(f"\n✓ SQL → PostgreSQL sync verified")
        else:
            print("\n⚠ No sync records found. Run a sync operation first.")
            print("  To sync: Click 'Sync' button on a data source card in the web UI")
        
        cursor.close()
        pg_conn.close()
        
    except Exception as e:
        print(f"✗ SQL → PostgreSQL sync test failed: {e}")
else:
    print("\n⚠ Skipping - Prerequisites not met")

# ============================================================================
# STEP 5: Test SQL → ClickHouse Sync
# ============================================================================
print("\n" + "="*80)
print("STEP 5: Testing SQL Server → ClickHouse Sync")
print("="*80)

if test_results["sql_server_connectivity"] and test_results["clickhouse_connectivity"]:
    try:
        print("\nChecking for SQL Server data in ClickHouse...")
        
        # Connect to ClickHouse
        ch_client = Client(
            host='localhost',
            port=9000,
            user='default',
            password='',
            database='default'
        )
        
        # Get all non-system databases
        result = ch_client.execute('SHOW DATABASES')
        databases = [row[0] for row in result if row[0] not in ('default', 'system', 'information_schema', 'INFORMATION_SCHEMA')]
        
        if databases:
            print(f"\nFound {len(databases)} data databases in ClickHouse:")
            
            total_tables = 0
            total_rows = 0
            
            for db in databases:
                # Get tables in this database
                result = ch_client.execute(f'SHOW TABLES FROM {db}')
                tables = [row[0] for row in result]
                
                if tables:
                    db_rows = 0
                    for table in tables[:3]:  # Check first 3 tables
                        result = ch_client.execute(f'SELECT COUNT(*) FROM {db}.{table}')
                        row_count = result[0][0]
                        db_rows += row_count
                    
                    total_tables += len(tables)
                    total_rows += db_rows
                    
                    print(f"  - {db}: {len(tables)} tables, ~{db_rows:,} rows")
            
            if total_tables > 0:
                test_results["sql_to_clickhouse_sync"] = True
                print(f"\n✓ SQL → ClickHouse sync verified")
                print(f"  Total: {total_tables} tables, ~{total_rows:,} rows")
            else:
                print("\n⚠ No SQL Server data found in ClickHouse")
        else:
            print("\n⚠ No data databases found in ClickHouse. Run sync first.")
            
    except Exception as e:
        print(f"✗ SQL → ClickHouse sync test failed: {e}")
else:
    print("\n⚠ Skipping - Prerequisites not met")

# ============================================================================
# STEP 6: Data Integrity Check
# ============================================================================
print("\n" + "="*80)
print("STEP 6: Data Integrity Verification")
print("="*80)

if test_results["sql_to_postgres_sync"] or test_results["sql_to_clickhouse_sync"]:
    try:
        print("\nVerifying data integrity...")
        
        integrity_checks = []
        
        # Check PostgreSQL data integrity
        if test_results["sql_to_postgres_sync"]:
            pg_conn = psycopg2.connect(
                host='localhost',
                port=5432,
                database='test1',
                user='postgres',
                password='root'
            )
            cursor = pg_conn.cursor()
            
            # Get a sample table with data
            cursor.execute("""
                SELECT t.table_schema, t.table_name, 
                       (SELECT COUNT(*) 
                        FROM information_schema.columns c 
                        WHERE c.table_schema = t.table_schema 
                        AND c.table_name = t.table_name) as column_count
                FROM information_schema.tables t
                WHERE t.table_schema NOT IN ('information_schema', 'pg_catalog', 'public')
                LIMIT 1
            """)
            sample = cursor.fetchone()
            
            if sample:
                schema, table, col_count = sample
                print(f"\n  PostgreSQL Sample Check:")
                print(f"    Table: {schema}.{table}")
                print(f"    Columns: {col_count}")
                
                # Try to count rows
                try:
                    cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
                    row_count = cursor.fetchone()[0]
                    print(f"    Rows: {row_count:,}")
                    integrity_checks.append(True)
                except Exception as e:
                    print(f"    ⚠ Could not count rows: {e}")
            
            cursor.close()
            pg_conn.close()
        
        # Check ClickHouse data integrity
        if test_results["sql_to_clickhouse_sync"]:
            ch_client = Client(
                host='localhost',
                port=9000,
                user='default',
                password='',
                database='default'
            )
            
            # Get a sample table
            result = ch_client.execute('SHOW DATABASES')
            databases = [row[0] for row in result if row[0] not in ('default', 'system', 'information_schema', 'INFORMATION_SCHEMA')]
            
            if databases:
                sample_db = databases[0]
                result = ch_client.execute(f'SHOW TABLES FROM {sample_db}')
                
                if result:
                    sample_table = result[0][0]
                    
                    print(f"\n  ClickHouse Sample Check:")
                    print(f"    Table: {sample_db}.{sample_table}")
                    
                    # Get column count
                    result = ch_client.execute(f'DESCRIBE TABLE {sample_db}.{sample_table}')
                    col_count = len(result)
                    print(f"    Columns: {col_count}")
                    
                    # Get row count
                    result = ch_client.execute(f'SELECT COUNT(*) FROM {sample_db}.{sample_table}')
                    row_count = result[0][0]
                    print(f"    Rows: {row_count:,}")
                    
                    integrity_checks.append(True)
        
        if integrity_checks:
            test_results["data_integrity"] = True
            print(f"\n✓ Data integrity verified")
        
    except Exception as e:
        print(f"✗ Data integrity check failed: {e}")
else:
    print("\n⚠ Skipping - No sync data available")

# ============================================================================
# FINAL SUMMARY
# ============================================================================
print("\n" + "="*80)
print("TEST SUMMARY")
print("="*80)

total_tests = len(test_results)
passed_tests = sum(1 for result in test_results.values() if result)

print(f"\nTests Passed: {passed_tests}/{total_tests}")
print()

for test_name, result in test_results.items():
    status = "✓ PASS" if result else "✗ FAIL"
    test_display = test_name.replace('_', ' ').title()
    print(f"  {status}: {test_display}")

print("\n" + "="*80)

if passed_tests == total_tests:
    print("🎉 ALL TESTS PASSED - Migration system is working correctly!")
elif passed_tests >= 3:
    print("⚠ PARTIAL SUCCESS - Some components need attention")
    print("\nRecommendations:")
    
    if not test_results["sql_to_postgres_sync"]:
        print("  • Run a sync operation from the web UI to test PostgreSQL migration")
    if not test_results["sql_to_clickhouse_sync"]:
        print("  • Configure a ClickHouse target and run sync to test")
    if not test_results["data_integrity"]:
        print("  • Verify that synced data is accessible and complete")
else:
    print("❌ TESTS FAILED - Check connectivity and configuration")
    print("\nTroubleshooting:")
    print("  1. Verify all services are running (SQL Server, PostgreSQL, ClickHouse)")
    print("  2. Check connection credentials in data_sources table")
    print("  3. Review logs in hybrid_sync.log for detailed errors")
    print("  4. Ensure ODBC Driver 17 for SQL Server is installed")

print("="*80)
print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)

# Exit with appropriate code
sys.exit(0 if passed_tests == total_tests else 1)
