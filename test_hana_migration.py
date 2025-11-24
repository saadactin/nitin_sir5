"""
Test Script for HANA to ClickHouse Direct Migration
Tests connection and migrates data directly from HANA database to ClickHouse
"""

import os
import sys
from typing import Dict, List, Optional
import logging
from datetime import datetime

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('hana_migration_test.log')
    ]
)
logger = logging.getLogger(__name__)

# Try to import required libraries
try:
    import hdbcli.dbapi as hana_dbapi
    HANA_AVAILABLE = True
except ImportError:
    HANA_AVAILABLE = False
    logger.error("hdbcli library not installed. Install with: pip install hdbcli")

try:
    from clickhouse_connect import get_client
    CLICKHOUSE_CONNECT_AVAILABLE = True
except ImportError:
    CLICKHOUSE_CONNECT_AVAILABLE = False
    logger.error("clickhouse-connect library not installed. Install with: pip install clickhouse-connect")


def get_hana_config() -> Dict:
    """Get HANA configuration from environment variables or user input"""
    config = {}
    
    # Try to load from .env first
    host = os.environ.get('HANA_HOST')
    port = os.environ.get('HANA_PORT', '30015')
    username = os.environ.get('HANA_USERNAME')
    password = os.environ.get('HANA_PASSWORD')
    database = os.environ.get('HANA_DATABASE', '')
    
    # If not in .env, prompt user
    if not host:
        host = input("Enter HANA Host (IP or hostname): ").strip()
    if not username:
        username = input("Enter HANA Username: ").strip()
    if not password:
        import getpass
        password = getpass.getpass("Enter HANA Password: ").strip()
    
    if not port:
        port = input("Enter HANA Port (default 30015): ").strip() or '30015'
    
    config = {
        'host': host,
        'port': int(port),
        'username': username,
        'password': password,
        'database': database
    }
    
    return config


def get_clickhouse_config() -> Dict:
    """Get ClickHouse configuration from environment variables or user input"""
    # Try to load from .env first
    host = os.environ.get('CLICKHOUSE_HOST')
    port = os.environ.get('CLICKHOUSE_PORT', '9000')
    username = os.environ.get('CLICKHOUSE_USER', 'default')
    password = os.environ.get('CLICKHOUSE_PASSWORD', '')
    database = os.environ.get('CLICKHOUSE_DATABASE', 'JARVIS_DB')
    
    # If not in .env, prompt user
    if not host:
        host = input("Enter ClickHouse Host (default: localhost): ").strip() or 'localhost'
    if not username:
        username = input("Enter ClickHouse Username (default: default): ").strip() or 'default'
    if password is None:
        import getpass
        password = getpass.getpass("Enter ClickHouse Password (press Enter if none): ").strip()
    if not database:
        database = input("Enter ClickHouse Database (default: JARVIS_DB): ").strip() or 'JARVIS_DB'
    
    config = {
        'host': host,
        'port': int(port) if port else 9000,
        'username': username,
        'password': password or '',
        'database': database
    }
    
    return config


def test_hana_connection(hana_config: Dict) -> Optional[object]:
    """Test HANA connection and return connection object if successful"""
    if not HANA_AVAILABLE:
        logger.error("hdbcli library not available")
        return None
    
    try:
        logger.info(f"Connecting to HANA: {hana_config['host']}:{hana_config['port']}")
        conn = hana_dbapi.connect(
            address=hana_config['host'],
            port=hana_config['port'],
            user=hana_config['username'],
            password=hana_config['password'],
            encrypt=True,
            sslValidateCertificate=False
        )
        logger.info("✅ HANA connection successful!")
        return conn
    except Exception as e:
        logger.error(f"❌ HANA connection failed: {e}")
        return None


def test_clickhouse_connection(ch_config: Dict) -> Optional[object]:
    """Test ClickHouse connection and return client object if successful"""
    if not CLICKHOUSE_CONNECT_AVAILABLE:
        logger.error("clickhouse-connect library not available")
        return None
    
    try:
        logger.info(f"Connecting to ClickHouse: {ch_config['host']}:{ch_config['port']}")
        client = get_client(
            host=ch_config['host'],
            port=ch_config['port'],
            username=ch_config['username'],
            password=ch_config['password'],
            database=ch_config['database']
        )
        logger.info(f"✅ ClickHouse connection successful! (database: {ch_config['database']})")
        return client
    except Exception as e:
        logger.error(f"❌ ClickHouse connection failed: {e}")
        return None


def get_hana_schemas(hana_conn) -> List[str]:
    """Get all user-accessible schemas from HANA"""
    try:
        cursor = hana_conn.cursor()
        cursor.execute("""
            SELECT SCHEMA_NAME 
            FROM SYS.SCHEMAS 
            WHERE SCHEMA_NAME NOT IN ('SYS', '_SYS_BI', '_SYS_BIC', '_SYS_EPM', '_SYS_REPO', '_SYS_STATISTICS', 'SYSTEM')
            ORDER BY SCHEMA_NAME
        """)
        schemas = [row[0] for row in cursor.fetchall()]
        cursor.close()
        logger.info(f"Found {len(schemas)} schemas in HANA: {', '.join(schemas[:10])}{'...' if len(schemas) > 10 else ''}")
        return schemas
    except Exception as e:
        logger.error(f"Failed to get HANA schemas: {e}")
        return []


def get_hana_tables(hana_conn, schema: str) -> List[Dict]:
    """Get all tables in a schema"""
    try:
        cursor = hana_conn.cursor()
        query = """
        SELECT 
            TABLE_NAME,
            TABLE_TYPE
        FROM SYS.TABLES
        WHERE SCHEMA_NAME = ?
        ORDER BY TABLE_NAME
        """
        cursor.execute(query, (schema,))
        tables = [{'name': row[0], 'type': row[1]} for row in cursor.fetchall()]
        cursor.close()
        return tables
    except Exception as e:
        logger.error(f"Failed to get tables for schema {schema}: {e}")
        return []


def get_hana_table_schema(hana_conn, schema: str, table: str) -> List[Dict]:
    """Get detailed column information for a table"""
    try:
        cursor = hana_conn.cursor()
        query = """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE_NAME,
            LENGTH,
            SCALE,
            IS_NULLABLE
        FROM SYS.TABLE_COLUMNS
        WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?
        ORDER BY POSITION
        """
        cursor.execute(query, (schema, table))
        columns = []
        for row in cursor.fetchall():
            columns.append({
                'name': row[0],
                'type': row[1],
                'length': row[2],
                'scale': row[3],
                'nullable': row[4] == 'TRUE'
            })
        cursor.close()
        return columns
    except Exception as e:
        logger.error(f"Failed to get schema for {schema}.{table}: {e}")
        return []


def map_hana_to_clickhouse_type(hana_type: str, length: Optional[int] = None, 
                               scale: Optional[int] = None, nullable: bool = True) -> str:
    """Map HANA data types to ClickHouse types"""
    type_mapping = {
        'TINYINT': 'Int8',
        'SMALLINT': 'Int16',
        'INTEGER': 'Int32',
        'INT': 'Int32',
        'BIGINT': 'Int64',
        'REAL': 'Float32',
        'DOUBLE': 'Float64',
        'SMALLDECIMAL': 'Decimal32(2)',
        'VARCHAR': 'String',
        'NVARCHAR': 'String',
        'CHAR': 'String',
        'NCHAR': 'String',
        'DATE': 'Date',
        'TIME': 'String',
        'TIMESTAMP': 'DateTime',
        'SECONDDATE': 'DateTime',
        'BOOLEAN': 'UInt8',
        'BLOB': 'String',
        'CLOB': 'String',
        'NCLOB': 'String',
        'TEXT': 'String'
    }
    
    # Handle DECIMAL/NUMERIC with precision
    if hana_type in ['DECIMAL', 'NUMERIC']:
        if scale:
            return f'Decimal64({scale})'
        return 'Decimal64(2)'
    
    # Default to String for unknown types
    ch_type = type_mapping.get(hana_type.upper(), 'String')
    
    # Wrap in Nullable if needed
    if nullable:
        return f'Nullable({ch_type})'
    return ch_type


def create_clickhouse_table(ch_client, database: str, schema: str, table: str, columns: List[Dict]) -> str:
    """Create table in ClickHouse"""
    import re
    
    # Create table name: schema_tablename
    clean_schema = re.sub(r'[^a-zA-Z0-9_]', '_', schema.upper())
    clean_table = re.sub(r'[^a-zA-Z0-9_]', '_', table.upper())
    ch_table_name = f"{clean_schema}_{clean_table}"
    
    try:
        # Create database if not exists
        ch_client.command(f"CREATE DATABASE IF NOT EXISTS {database}")
        
        # Build column definitions
        column_defs = []
        for col in columns:
            ch_type = map_hana_to_clickhouse_type(
                col['type'], 
                col.get('length'), 
                col.get('scale'),
                col.get('nullable', True)
            )
            column_defs.append(f"`{col['name']}` {ch_type}")
        
        # Create table
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {database}.{ch_table_name} (
            {', '.join(column_defs)}
        ) ENGINE = MergeTree()
        ORDER BY tuple()
        """
        
        ch_client.command(create_sql)
        logger.info(f"✅ Created table: {database}.{ch_table_name}")
        return ch_table_name
    except Exception as e:
        logger.error(f"❌ Failed to create table {ch_table_name}: {e}")
        raise


def migrate_table_data(hana_conn, ch_client, database: str, schema: str, table: str, ch_table_name: str) -> int:
    """Migrate data from HANA table to ClickHouse"""
    import pandas as pd
    
    try:
        # Query data from HANA
        query = f'SELECT * FROM "{schema}"."{table}"'
        logger.info(f"Fetching data from HANA: {schema}.{table}")
        
        df = pd.read_sql(query, hana_conn)
        
        if df.empty:
            logger.info(f"ℹ️ Table {schema}.{table} is empty")
            return 0
        
        # Clean column names
        df.columns = [col.replace(' ', '_').replace('-', '_') for col in df.columns]
        
        # Insert into ClickHouse
        logger.info(f"Inserting {len(df)} rows into ClickHouse: {database}.{ch_table_name}")
        ch_client.insert_df(f"{database}.{ch_table_name}", df, column_names=df.columns.tolist())
        
        logger.info(f"✅ Migrated {len(df)} rows from {schema}.{table} to {database}.{ch_table_name}")
        return len(df)
    except Exception as e:
        logger.error(f"❌ Failed to migrate data from {schema}.{table}: {e}")
        return 0


def migrate_hana_to_clickhouse(hana_config: Dict, ch_config: Dict, 
                               schemas_filter: Optional[List[str]] = None,
                               tables_filter: Optional[List[str]] = None,
                               max_tables: Optional[int] = None) -> Dict:
    """Main migration function"""
    results = {
        'success': False,
        'tables_processed': 0,
        'tables_successful': 0,
        'tables_failed': 0,
        'total_rows_migrated': 0,
        'errors': []
    }
    
    # Test connections
    hana_conn = test_hana_connection(hana_config)
    if not hana_conn:
        results['errors'].append("HANA connection failed")
        return results
    
    ch_client = test_clickhouse_connection(ch_config)
    if not ch_client:
        results['errors'].append("ClickHouse connection failed")
        hana_conn.close()
        return results
    
    try:
        # Get schemas
        all_schemas = get_hana_schemas(hana_conn)
        if schemas_filter:
            schemas = [s for s in all_schemas if s in schemas_filter]
        else:
            schemas = all_schemas
        
        if not schemas:
            logger.warning("No schemas found to migrate")
            results['errors'].append("No schemas found")
            return results
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Starting migration of {len(schemas)} schema(s)")
        logger.info(f"{'='*60}\n")
        
        # Process each schema
        for schema in schemas:
            logger.info(f"\n📦 Processing schema: {schema}")
            tables = get_hana_tables(hana_conn, schema)
            
            if not tables:
                logger.info(f"  No tables found in schema {schema}")
                continue
            
            # Filter tables if needed
            if tables_filter:
                tables = [t for t in tables if t['name'] in tables_filter]
            
            logger.info(f"  Found {len(tables)} table(s)")
            
            # Process each table
            for table_info in tables:
                if max_tables and results['tables_processed'] >= max_tables:
                    logger.info(f"\n⚠️ Reached max_tables limit ({max_tables})")
                    break
                
                table = table_info['name']
                table_type = table_info['type']
                
                # Skip views for now
                if table_type != 'TABLE':
                    logger.info(f"  ⏭️ Skipping {schema}.{table} (type: {table_type})")
                    continue
                
                results['tables_processed'] += 1
                logger.info(f"\n  📋 Processing table: {schema}.{table} ({results['tables_processed']})")
                
                try:
                    # Get table schema
                    columns = get_hana_table_schema(hana_conn, schema, table)
                    if not columns:
                        logger.warning(f"  ⚠️ No columns found for {schema}.{table}")
                        results['tables_failed'] += 1
                        continue
                    
                    # Create ClickHouse table
                    ch_table_name = create_clickhouse_table(ch_client, ch_config['database'], schema, table, columns)
                    
                    # Migrate data
                    rows_migrated = migrate_table_data(hana_conn, ch_client, ch_config['database'], 
                                                     schema, table, ch_table_name)
                    
                    results['total_rows_migrated'] += rows_migrated
                    results['tables_successful'] += 1
                    
                except Exception as e:
                    logger.error(f"  ❌ Error processing {schema}.{table}: {e}")
                    results['tables_failed'] += 1
                    results['errors'].append(f"{schema}.{table}: {str(e)}")
        
        results['success'] = True
        
    except Exception as e:
        logger.error(f"Migration error: {e}")
        results['errors'].append(str(e))
    finally:
        # Close connections
        if hana_conn:
            hana_conn.close()
        if ch_client:
            ch_client.close()
    
    return results


def main():
    """Main test function"""
    print("\n" + "="*60)
    print("HANA to ClickHouse Migration Test")
    print("="*60 + "\n")
    
    # Check dependencies
    if not HANA_AVAILABLE:
        print("❌ hdbcli library not installed. Install with: pip install hdbcli")
        return
    if not CLICKHOUSE_CONNECT_AVAILABLE:
        print("❌ clickhouse-connect library not installed. Install with: pip install clickhouse-connect")
        return
    
    # Get configurations
    print("📝 Configuration:")
    print("-" * 60)
    hana_config = get_hana_config()
    print(f"  HANA: {hana_config['host']}:{hana_config['port']} (user: {hana_config['username']})")
    
    ch_config = get_clickhouse_config()
    print(f"  ClickHouse: {ch_config['host']}:{ch_config['port']} (database: {ch_config['database']})")
    print("-" * 60 + "\n")
    
    # Ask for migration options
    print("Migration Options:")
    print("1. Migrate all tables from all schemas")
    print("2. Migrate specific schemas (comma-separated)")
    print("3. Test connection only (no migration)")
    
    choice = input("\nEnter choice (1-3, default: 3): ").strip() or "3"
    
    schemas_filter = None
    if choice == "2":
        schemas_input = input("Enter schema names (comma-separated): ").strip()
        if schemas_input:
            schemas_filter = [s.strip() for s in schemas_input.split(',')]
    
    max_tables = None
    if choice in ["1", "2"]:
        max_input = input("Max tables to migrate (press Enter for all): ").strip()
        if max_input:
            try:
                max_tables = int(max_input)
            except ValueError:
                pass
    
    # Test connections
    if choice == "3":
        print("\n🔍 Testing connections only...\n")
        hana_conn = test_hana_connection(hana_config)
        ch_client = test_clickhouse_connection(ch_config)
        
        if hana_conn and ch_client:
            print("\n✅ Both connections successful! Ready for migration.")
            # Show available schemas
            if hana_conn:
                schemas = get_hana_schemas(hana_conn)
                if schemas:
                    print(f"\nAvailable schemas: {', '.join(schemas)}")
                hana_conn.close()
            if ch_client:
                ch_client.close()
        else:
            print("\n❌ Connection test failed. Please check your credentials.")
    else:
        # Run migration
        print("\n🚀 Starting migration...\n")
        results = migrate_hana_to_clickhouse(
            hana_config, 
            ch_config,
            schemas_filter=schemas_filter,
            max_tables=max_tables
        )
        
        # Print summary
        print("\n" + "="*60)
        print("Migration Summary")
        print("="*60)
        print(f"Status: {'✅ Success' if results['success'] else '❌ Failed'}")
        print(f"Tables processed: {results['tables_processed']}")
        print(f"Tables successful: {results['tables_successful']}")
        print(f"Tables failed: {results['tables_failed']}")
        print(f"Total rows migrated: {results['total_rows_migrated']:,}")
        
        if results['errors']:
            print(f"\nErrors ({len(results['errors'])}):")
            for error in results['errors'][:10]:  # Show first 10 errors
                print(f"  - {error}")
            if len(results['errors']) > 10:
                print(f"  ... and {len(results['errors']) - 10} more errors")
        print("="*60 + "\n")


if __name__ == "__main__":
    main()

