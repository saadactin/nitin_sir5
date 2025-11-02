"""
HANA to ClickHouse Migration Test Script
==========================================

This script tests the complete migration flow from SAP HANA to ClickHouse.
It includes:
- Connection testing for both HANA and ClickHouse
- Schema discovery from HANA
- Data type mapping validation
- Complete table migration with data validation
- Migration verification

PREREQUISITES:
-------------
1. Install required packages:
   pip install hdbcli clickhouse-driver pandas

2. Set ClickHouse environment variables in .env or system:
   CLICKHOUSE_HOST=localhost (or your ClickHouse server IP)
   CLICKHOUSE_PORT=9000
   CLICKHOUSE_USER=default
   CLICKHOUSE_PASSWORD=your_password

3. HANA credentials (to be provided):
   - HANA server host/IP
   - HANA port (typically 30015)
   - Username
   - Password
   - Database name (optional)

USAGE:
------
1. Update the HANA_CONFIG section below with your credentials
2. Update the CLICKHOUSE_CONFIG section with your ClickHouse details
3. Run: python test_hana_to_clickhouse_migration.py
"""

import os
import sys
import logging
from datetime import datetime
from typing import Dict, List, Optional

# Try to load environment variables from .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Import the sync module
try:
    from hana_sync import HanaToClickHouseSync
except ImportError:
    print("ERROR: Could not import hana_sync module.")
    print("Make sure you're running this from the project root directory.")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('hana_clickhouse_test.log')
    ]
)
logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION SECTION - READ FROM .ENV FILE ONLY
# ============================================================================

def load_hana_config_from_env():
    """
    Load HANA configuration from environment variables (.env file)
    Returns None if required variables are missing
    """
    host = os.getenv('HANA_HOST')
    port_str = os.getenv('HANA_PORT', '30015')  # Default port if not set
    username = os.getenv('HANA_USERNAME')
    password = os.getenv('HANA_PASSWORD')
    
    if not host or not username or not password:
        return None
    
    try:
        port = int(port_str)
    except ValueError:
        logger.warning(f"Invalid HANA_PORT '{port_str}', using default 30015")
        port = 30015
    
    return {
        'host': host,
        'port': port,
        'username': username,
        'password': password
    }

# HANA Database Configuration - Loaded from .env file
HANA_CONFIG = load_hana_config_from_env()

# ClickHouse Configuration (from environment variables)
CLICKHOUSE_CONFIG = {
    'host': os.getenv('CLICKHOUSE_HOST', 'localhost'),
    'port': int(os.getenv('CLICKHOUSE_PORT', '9000')),
    'user': os.getenv('CLICKHOUSE_USER', 'default'),
    'password': os.getenv('CLICKHOUSE_PASSWORD', ''),
    'database': os.getenv('CLICKHOUSE_DATABASE', 'hana_migrated')  # Target database name
}

# Migration Settings
MIGRATION_SETTINGS = {
    'batch_size': 10000,           # Number of rows to process per batch
    'test_schemas': None,          # None = test all schemas, or specify list: ['SCHEMA1', 'SCHEMA2']
    'test_tables': None,           # None = test all tables, or specify list: ['TABLE1', 'TABLE2']
    'skip_empty_tables': True,     # Skip tables with no data
    'validate_migration': True     # Verify row counts match after migration
}


# ============================================================================
# TEST FUNCTIONS
# ============================================================================

def test_hana_connection(hana_config: Dict) -> bool:
    """
    Test connection to SAP HANA database
    
    Returns:
        bool: True if connection successful, False otherwise
    """
    logger.info("=" * 70)
    logger.info("TEST 1: Testing HANA Connection")
    logger.info("=" * 70)
    
    try:
        import hdbcli.dbapi as hana_dbapi
        
        logger.info(f"Attempting to connect to HANA at {hana_config['host']}:{hana_config['port']}")
        logger.info(f"Username: {hana_config['username']}")
        
        conn = hana_dbapi.connect(
            address=hana_config['host'],
            port=hana_config['port'],
            user=hana_config['username'],
            password=hana_config['password'],
            encrypt=True,
            sslValidateCertificate=False
        )
        
        # Test query
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER, CURRENT_TIMESTAMP FROM DUMMY")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        logger.info(f"✅ HANA Connection: SUCCESS")
        logger.info(f"   Connected as: {result[0]}")
        logger.info(f"   Server timestamp: {result[1]}")
        return True
        
    except ImportError:
        logger.error("❌ HANA Connection: FAILED")
        logger.error("   hdbcli library not installed. Install with: pip install hdbcli")
        return False
    except Exception as e:
        logger.error(f"❌ HANA Connection: FAILED")
        logger.error(f"   Error: {str(e)}")
        logger.error("   Possible issues:")
        logger.error("   - Incorrect host/IP address")
        logger.error("   - Wrong port number")
        logger.error("   - Invalid username/password")
        logger.error("   - Firewall blocking connection")
        logger.error("   - HANA server not running")
        return False


def test_clickhouse_connection(clickhouse_config: Dict) -> bool:
    """
    Test connection to ClickHouse database
    
    Returns:
        bool: True if connection successful, False otherwise
    """
    logger.info("=" * 70)
    logger.info("TEST 2: Testing ClickHouse Connection")
    logger.info("=" * 70)
    
    try:
        from clickhouse_driver import Client
        
        logger.info(f"Attempting to connect to ClickHouse at {clickhouse_config['host']}:{clickhouse_config['port']}")
        logger.info(f"Username: {clickhouse_config['user']}")
        logger.info(f"Database: {clickhouse_config['database']}")
        
        client = Client(
            host=clickhouse_config['host'],
            port=clickhouse_config['port'],
            user=clickhouse_config['user'],
            password=clickhouse_config['password'],
            database=clickhouse_config['database']
        )
        
        # Test query
        result = client.execute("SELECT version(), currentDatabase()")
        version, current_db = result[0]
        
        # Check if database exists, create if not
        databases = client.execute("SHOW DATABASES")
        db_names = [db[0] for db in databases]
        
        if clickhouse_config['database'] not in db_names:
            logger.info(f"   Creating database '{clickhouse_config['database']}'...")
            client.execute(f"CREATE DATABASE IF NOT EXISTS {clickhouse_config['database']}")
        
        client.disconnect()
        
        logger.info(f"✅ ClickHouse Connection: SUCCESS")
        logger.info(f"   Version: {version}")
        logger.info(f"   Current database: {current_db}")
        return True
        
    except ImportError:
        logger.error("❌ ClickHouse Connection: FAILED")
        logger.error("   clickhouse-driver library not installed. Install with: pip install clickhouse-driver")
        return False
    except Exception as e:
        logger.error(f"❌ ClickHouse Connection: FAILED")
        logger.error(f"   Error: {str(e)}")
        logger.error("   Possible issues:")
        logger.error("   - Incorrect host/IP address")
        logger.error("   - Wrong port number (use 9000 for native, 8123 for HTTP)")
        logger.error("   - Invalid username/password")
        logger.error("   - Firewall blocking connection")
        logger.error("   - ClickHouse server not running")
        return False


def discover_hana_schemas(sync_engine: HanaToClickHouseSync) -> List[str]:
    """
    Discover all schemas in HANA database
    
    Returns:
        List[str]: List of schema names
    """
    logger.info("=" * 70)
    logger.info("TEST 3: Discovering HANA Schemas")
    logger.info("=" * 70)
    
    try:
        schemas = sync_engine.get_hana_schemas()
        
        if schemas:
            logger.info(f"✅ Found {len(schemas)} schema(s):")
            for i, schema in enumerate(schemas, 1):
                logger.info(f"   {i}. {schema}")
        else:
            logger.warning("⚠️  No schemas found in HANA database")
        
        return schemas
        
    except Exception as e:
        logger.error(f"❌ Schema Discovery: FAILED")
        logger.error(f"   Error: {str(e)}")
        return []


def discover_hana_tables(sync_engine: HanaToClickHouseSync, schema: str) -> List[Dict]:
    """
    Discover all tables in a HANA schema
    
    Args:
        schema: Schema name
        
    Returns:
        List[Dict]: List of table dictionaries with name and type
    """
    try:
        tables = sync_engine.get_hana_tables(schema)
        
        if tables:
            logger.info(f"   Found {len(tables)} table(s) in schema '{schema}':")
            for table in tables[:10]:  # Show first 10
                logger.info(f"      - {table['name']} ({table['type']})")
            if len(tables) > 10:
                logger.info(f"      ... and {len(tables) - 10} more tables")
        
        return tables
        
    except Exception as e:
        logger.error(f"   Error discovering tables in schema '{schema}': {str(e)}")
        return []


def test_schema_mapping(sync_engine: HanaToClickHouseSync, schema: str, table: str) -> bool:
    """
    Test HANA to ClickHouse schema mapping for a table
    
    Returns:
        bool: True if schema mapping successful, False otherwise
    """
    try:
        logger.info(f"\n   Testing schema mapping for {schema}.{table}...")
        
        # Get HANA table schema
        columns = sync_engine.get_hana_table_schema(schema, table)
        
        if not columns:
            logger.warning(f"      ⚠️  No columns found for {schema}.{table}")
            return False
        
        logger.info(f"      HANA columns ({len(columns)}):")
        for col in columns[:5]:  # Show first 5
            ch_type = sync_engine.map_hana_to_clickhouse_type(col['type'], col['length'], col['scale'])
            logger.info(f"         - {col['name']}: {col['type']} -> {ch_type}")
        
        if len(columns) > 5:
            logger.info(f"         ... and {len(columns) - 5} more columns")
        
        # Create ClickHouse table
        ch_table_name = sync_engine.create_clickhouse_table_name(schema, table)
        logger.info(f"      ClickHouse table name: {ch_table_name}")
        
        if sync_engine.create_clickhouse_table(schema, table, columns):
            logger.info(f"      ✅ Schema mapping successful")
            return True
        else:
            logger.error(f"      ❌ Failed to create ClickHouse table")
            return False
            
    except Exception as e:
        logger.error(f"      ❌ Schema mapping failed: {str(e)}")
        return False


def migrate_table(sync_engine: HanaToClickHouseSync, schema: str, table: str, 
                  batch_size: int = 10000, validate: bool = True) -> Dict:
    """
    Migrate a single table from HANA to ClickHouse
    
    Returns:
        Dict: Migration result with status, row counts, etc.
    """
    try:
        logger.info(f"\n   Migrating table {schema}.{table}...")
        
        # Perform migration
        result = sync_engine.migrate_table_data(schema, table, batch_size=batch_size)
        
        if result.get('status') == 'success':
            logger.info(f"      ✅ Migration successful")
            logger.info(f"         Source: {result.get('source_table')}")
            logger.info(f"         Target: {result.get('target_table')}")
            logger.info(f"         Rows migrated: {result.get('migrated_rows')} / {result.get('total_rows')}")
            logger.info(f"         Time taken: {result.get('elapsed_time', 0):.2f} seconds")
            
            # Validate migration if requested
            if validate and result.get('migrated_rows', 0) > 0:
                if validate_row_count(sync_engine, schema, table, result.get('migrated_rows', 0)):
                    logger.info(f"         ✅ Row count validation: PASSED")
                else:
                    logger.warning(f"         ⚠️  Row count validation: FAILED")
        else:
            logger.error(f"      ❌ Migration failed")
            logger.error(f"         Error: {result.get('error', 'Unknown error')}")
        
        return result
        
    except Exception as e:
        logger.error(f"      ❌ Migration exception: {str(e)}")
        return {'status': 'failed', 'error': str(e)}


def validate_row_count(sync_engine: HanaToClickHouseSync, schema: str, table: str, 
                       expected_rows: int) -> bool:
    """
    Validate that row counts match between HANA and ClickHouse
    
    Returns:
        bool: True if row counts match, False otherwise
    """
    try:
        # Get HANA row count
        cursor = sync_engine.hana_conn.cursor()
        cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
        hana_count = cursor.fetchone()[0]
        cursor.close()
        
        # Get ClickHouse row count
        ch_table_name = sync_engine.create_clickhouse_table_name(schema, table)
        database_name = sync_engine.clickhouse_config.get('database', 'hana_migrated')
        
        result = sync_engine.ch_client.execute(
            f"SELECT COUNT(*) FROM {database_name}.{ch_table_name}"
        )
        ch_count = result[0][0] if result else 0
        
        if hana_count == ch_count == expected_rows:
            return True
        else:
            logger.warning(f"         Row count mismatch: HANA={hana_count}, ClickHouse={ch_count}, Expected={expected_rows}")
            return False
            
    except Exception as e:
        logger.warning(f"         Could not validate row count: {str(e)}")
        return False


def run_full_migration_test(hana_config: Dict, clickhouse_config: Dict, 
                           settings: Dict) -> bool:
    """
    Run complete migration test from HANA to ClickHouse
    
    Returns:
        bool: True if all tests passed, False otherwise
    """
    logger.info("=" * 70)
    logger.info("MIGRATION TEST: Starting Full Migration Test")
    logger.info("=" * 70)
    logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("")
    
    # Initialize sync engine
    sync_engine = HanaToClickHouseSync(hana_config, clickhouse_config)
    
    # Connect to databases
    logger.info("Connecting to databases...")
    if not sync_engine.connect_hana():
        logger.error("Failed to connect to HANA. Aborting migration test.")
        return False
    
    if not sync_engine.connect_clickhouse():
        logger.error("Failed to connect to ClickHouse. Aborting migration test.")
        sync_engine.close_connections()
        return False
    
    logger.info("✅ Connected to both HANA and ClickHouse\n")
    
    try:
        # Discover schemas
        all_schemas = discover_hana_schemas(sync_engine)
        
        if not all_schemas:
            logger.error("No schemas found. Cannot proceed with migration.")
            return False
        
        # Filter schemas if specified
        test_schemas = settings.get('test_schemas')
        if test_schemas:
            schemas = [s for s in all_schemas if s in test_schemas]
            logger.info(f"\nFiltered to {len(schemas)} specified schema(s)")
        else:
            schemas = all_schemas
        
        # Migrate tables
        total_tables = 0
        successful_tables = 0
        failed_tables = 0
        
        logger.info("=" * 70)
        logger.info("TEST 4: Migrating Tables")
        logger.info("=" * 70)
        
        for schema in schemas:
            logger.info(f"\nProcessing schema: {schema}")
            
            # Discover tables
            tables = discover_hana_tables(sync_engine, schema)
            
            if not tables:
                logger.info(f"   No tables found in schema '{schema}', skipping...")
                continue
            
            # Filter tables if specified
            test_tables = settings.get('test_tables')
            if test_tables:
                tables = [t for t in tables if t['name'] in test_tables]
            
            # Process each table
            for table_info in tables:
                table = table_info['name']
                total_tables += 1
                
                # Get table schema and create ClickHouse table
                columns = sync_engine.get_hana_table_schema(schema, table)
                if not columns:
                    logger.warning(f"   ⚠️  Skipping {schema}.{table} (no columns found)")
                    continue
                
                # Test schema mapping
                if not test_schema_mapping(sync_engine, schema, table):
                    failed_tables += 1
                    continue
                
                # Check if table is empty (if skip_empty_tables is True)
                cursor = sync_engine.hana_conn.cursor()
                cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
                row_count = cursor.fetchone()[0]
                cursor.close()
                
                if settings.get('skip_empty_tables') and row_count == 0:
                    logger.info(f"   ⏭️  Skipping empty table {schema}.{table}")
                    continue
                
                # Migrate table data
                result = migrate_table(
                    sync_engine, 
                    schema, 
                    table,
                    batch_size=settings.get('batch_size', 10000),
                    validate=settings.get('validate_migration', True)
                )
                
                if result.get('status') == 'success':
                    successful_tables += 1
                else:
                    failed_tables += 1
        
        # Print summary
        logger.info("")
        logger.info("=" * 70)
        logger.info("MIGRATION TEST SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Total tables processed: {total_tables}")
        logger.info(f"Successful migrations: {successful_tables}")
        logger.info(f"Failed migrations: {failed_tables}")
        logger.info(f"Success rate: {(successful_tables/total_tables*100) if total_tables > 0 else 0:.1f}%")
        logger.info(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return failed_tables == 0
        
    except Exception as e:
        logger.exception(f"Error during migration test: {str(e)}")
        return False
        
    finally:
        # Close connections
        sync_engine.close_connections()
        logger.info("\n✅ Database connections closed")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function"""
    print("\n" + "=" * 70)
    print("HANA TO CLICKHOUSE MIGRATION TEST SCRIPT")
    print("=" * 70)
    print("\nThis script will test the complete migration flow from SAP HANA to ClickHouse.")
    print("Make sure you have:")
    print("  1. Set HANA credentials in .env file (HANA_HOST, HANA_PORT, HANA_USERNAME, HANA_PASSWORD)")
    print("  2. Set ClickHouse environment variables in .env")
    print("  3. Installed required packages: hdbcli, clickhouse-driver\n")
    
    # Validate HANA configuration
    if HANA_CONFIG is None:
        print("❌ ERROR: HANA configuration not found in .env file!")
        print("\n" + "=" * 70)
        print("MISSING CONFIGURATION")
        print("=" * 70)
        print("\nPlease ADD these variables to your existing .env file:")
        print("\n# SAP HANA Configuration")
        print("HANA_HOST=192.168.16.62")
        print("HANA_PORT=30015")
        print("HANA_USERNAME=your_hana_username")
        print("HANA_PASSWORD=your_hana_password")
        print("\n⚠️  IMPORTANT: Add these to your existing .env file,")
        print("   do NOT replace any existing variables!")
        print("\nSee ADD_HANA_TO_ENV.md for detailed instructions.")
        print("=" * 70)
        return
    
    # Run tests
    print("\nStarting tests...\n")
    
    # Test 1: HANA Connection
    hana_ok = test_hana_connection(HANA_CONFIG)
    print()
    
    # Test 2: ClickHouse Connection
    clickhouse_ok = test_clickhouse_connection(CLICKHOUSE_CONFIG)
    print()
    
    if not hana_ok or not clickhouse_ok:
        print("❌ Connection tests failed. Please fix connection issues before proceeding.")
        print("   Check the error messages above for troubleshooting steps.")
        return
    
    # Test 3: Full Migration
    print("\n" + "=" * 70)
    print("READY TO START MIGRATION")
    print("=" * 70)
    print("\nAll connection tests passed!")
    print("The migration will:")
    print(f"  - Connect to HANA at {HANA_CONFIG['host']}:{HANA_CONFIG['port']}")
    print(f"  - Connect to ClickHouse at {CLICKHOUSE_CONFIG['host']}:{CLICKHOUSE_CONFIG['port']}")
    print(f"  - Migrate data to database: {CLICKHOUSE_CONFIG['database']}")
    print(f"  - Process {MIGRATION_SETTINGS['batch_size']} rows per batch\n")
    
    response = input("Do you want to proceed with the full migration test? (yes/no): ")
    if response.lower() != 'yes':
        print("Migration test cancelled.")
        return
    
    # Run full migration
    success = run_full_migration_test(HANA_CONFIG, CLICKHOUSE_CONFIG, MIGRATION_SETTINGS)
    
    print("\n" + "=" * 70)
    if success:
        print("✅ ALL TESTS PASSED - Migration completed successfully!")
    else:
        print("⚠️  SOME TESTS FAILED - Check the logs above for details")
    print("=" * 70)
    print(f"\nDetailed log saved to: hana_clickhouse_test.log")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Migration test interrupted by user.")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Unexpected error: {str(e)}")
        print(f"\n❌ Unexpected error occurred. Check hana_clickhouse_test.log for details.")
        sys.exit(1)

