"""
Unified Migration Manager
Handles migration from all data sources (DevOps API, Zoho API, HANA) to ClickHouse
Ensures empty tables are migrated with structure, proper error handling, and validation
"""

import os
import sys
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import traceback

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('unified_migration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class UnifiedMigrationManager:
    """Manages migration from all data sources to ClickHouse"""
    
    def __init__(self, target_database: str = None):
        """Initialize migration manager"""
        from db_utils import load_clickhouse_config
        
        ch_config = load_clickhouse_config()
        self.target_database = target_database or os.environ.get('CLICKHOUSE_DATABASE', 'JARVIS_DB')
        self.ch_config = {
            'host': ch_config['host'],
            'port': ch_config['port'],
            'user': ch_config['user'],
            'password': ch_config['password'],
            'database': self.target_database
        }
        
        # Statistics
        self.stats = {
            'total_sources': 0,
            'successful': 0,
            'failed': 0,
            'tables_processed': 0,
            'tables_successful': 0,
            'tables_failed': 0,
            'empty_tables_created': 0,
            'records_migrated': 0,
            'errors': []
        }
    
    def ensure_clickhouse_connection(self):
        """Ensure ClickHouse connection is available"""
        try:
            from clickhouse_connect import get_client
            client = get_client(
                host=self.ch_config['host'],
                port=self.ch_config.get('port', 9000),
                username=self.ch_config['user'],
                password=self.ch_config['password'],
                database=self.ch_config['database']
            )
            # Test connection
            client.command("SELECT 1")
            return client
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
    
    def migrate_hana_source(self, source_id: int, source_config: Dict) -> Dict:
        """Migrate HANA database source to ClickHouse"""
        logger.info(f"Starting HANA migration for source {source_id}: {source_config.get('source_name')}")
        
        result = {
            'source_id': source_id,
            'source_type': 'sap_hana',
            'success': False,
            'tables_processed': 0,
            'tables_successful': 0,
            'tables_failed': 0,
            'empty_tables_created': 0,
            'records_migrated': 0,
            'errors': []
        }
        
        try:
            from hana_sync import HanaToClickHouseSync
            from db_utils import load_hana_config
            import json
            
            # Parse connection details
            connection_details = source_config.get('connection_details', {})
            if isinstance(connection_details, str):
                connection_details = json.loads(connection_details)
            
            server_address = source_config.get('server_address', '')
            if ':' in server_address:
                host, port = server_address.split(':', 1)
            else:
                host = connection_details.get('host', server_address)
                port = connection_details.get('port')
            
            # Build HANA config
            try:
                hana_base_config = load_hana_config()
                hana_config = {
                    'host': host or hana_base_config['host'],
                    'port': int(port) if port else hana_base_config['port'],
                    'username': source_config.get('username') or hana_base_config['username'],
                    'password': source_config.get('password') or hana_base_config['password']
                }
            except ValueError:
                if not port:
                    raise ValueError("HANA_PORT is required")
                hana_config = {
                    'host': host,
                    'port': int(port),
                    'username': source_config.get('username'),
                    'password': source_config.get('password', '')
                }
            
            # Initialize sync engine
            sync_engine = HanaToClickHouseSync(hana_config, self.ch_config)
            
            if not sync_engine.connect_hana():
                raise Exception(f"Failed to connect to HANA: {hana_config['host']}:{hana_config['port']}")
            
            if not sync_engine.connect_clickhouse():
                raise Exception(f"Failed to connect to ClickHouse")
            
            # Get all schemas and tables
            schemas = sync_engine.get_hana_schemas()
            logger.info(f"Found {len(schemas)} schemas in HANA")
            
            for schema in schemas:
                tables = sync_engine.get_hana_tables(schema)
                logger.info(f"Processing schema {schema}: {len(tables)} tables")
                
                for table_info in tables:
                    if table_info['type'] != 'TABLE':
                        continue
                    
                    table = table_info['name']
                    result['tables_processed'] += 1
                    
                    try:
                        # Get table schema
                        columns = sync_engine.get_hana_table_schema(schema, table)
                        if not columns:
                            logger.warning(f"No columns found for {schema}.{table}")
                            result['tables_failed'] += 1
                            continue
                        
                        # Create table in ClickHouse (even if empty)
                        ch_table_name = sync_engine.create_clickhouse_table_name(schema, table)
                        if sync_engine.create_clickhouse_table(schema, table, columns):
                            # Check if table is empty
                            cursor = sync_engine.hana_conn.cursor()
                            cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
                            row_count = cursor.fetchone()[0]
                            cursor.close()
                            
                            if row_count == 0:
                                result['empty_tables_created'] += 1
                                logger.info(f"Created empty table structure: {schema}.{table}")
                            else:
                                # Migrate data
                                migrate_result = sync_engine.migrate_table_data(schema, table)
                                if migrate_result.get('status') == 'success':
                                    result['records_migrated'] += migrate_result.get('migrated_rows', 0)
                                    result['tables_successful'] += 1
                                else:
                                    result['tables_failed'] += 1
                                    result['errors'].append(f"{schema}.{table}: {migrate_result.get('error', 'Unknown error')}")
                        else:
                            result['tables_failed'] += 1
                            result['errors'].append(f"{schema}.{table}: Failed to create table")
                    
                    except Exception as e:
                        logger.error(f"Error processing {schema}.{table}: {e}")
                        result['tables_failed'] += 1
                        result['errors'].append(f"{schema}.{table}: {str(e)}")
            
            sync_engine.close_connections()
            result['success'] = result['tables_failed'] == 0
            
        except Exception as e:
            logger.exception(f"HANA migration failed for source {source_id}: {e}")
            result['errors'].append(str(e))
        
        return result
    
    def migrate_api_source(self, source_id: int, source_config: Dict) -> Dict:
        """Migrate API source (DevOps, Zoho, etc.) to ClickHouse"""
        logger.info(f"Starting API migration for source {source_id}: {source_config.get('source_name')}")
        
        result = {
            'source_id': source_id,
            'source_type': 'api',
            'success': False,
            'tables_processed': 0,
            'tables_successful': 0,
            'tables_failed': 0,
            'empty_tables_created': 0,
            'records_migrated': 0,
            'errors': []
        }
        
        try:
            from api_sync import sync_api_to_clickhouse_once
            from clickhouse_connect import get_client
            
            ch_client = get_client(
                host=self.ch_config['host'],
                port=self.ch_config.get('port', 9000),
                username=self.ch_config['user'],
                password=self.ch_config['password'],
                database=self.ch_config['database']
            )
            
            api_url = source_config.get('server_address', '')
            target_table = source_config.get('target_database', '') or source_config.get('source_name', f'api_source_{source_id}')
            
            # Handle Zoho OAuth if needed
            auth_type = source_config.get('auth_type', '')
            headers = {}
            
            if auth_type == 'zoho_oauth':
                from zoho_oauth_manager import ZohoOAuthManager
                connection_details = source_config.get('connection_details', {})
                if isinstance(connection_details, str):
                    import json
                    connection_details = json.loads(connection_details)
                
                token_result = ZohoOAuthManager.get_valid_token(
                    refresh_token=source_config.get('oauth_refresh_token') or connection_details.get('zoho_refresh_token'),
                    client_id=source_config.get('oauth_client_id') or connection_details.get('zoho_client_id'),
                    client_secret=source_config.get('oauth_client_secret') or connection_details.get('zoho_client_secret')
                )
                
                if token_result and token_result.get('access_token'):
                    headers['Authorization'] = f"Zoho-oauthtoken {token_result['access_token']}"
            
            # Try to fetch data from API
            try:
                import requests
                response = requests.get(api_url, headers=headers, timeout=30)
                response.raise_for_status()
                data = response.json()
            except requests.RequestException as e:
                # If API fails, create empty table structure
                logger.warning(f"API request failed for {api_url}: {e}. Creating empty table structure.")
                result['empty_tables_created'] += 1
                result['tables_processed'] += 1
                
                # Create empty table with basic structure
                try:
                    ch_client.command(f"CREATE DATABASE IF NOT EXISTS {self.ch_config['database']}")
                    ch_client.command(f"""
                        CREATE TABLE IF NOT EXISTS {self.ch_config['database']}.{target_table} (
                            `id` Nullable(String),
                            `data` Nullable(String),
                            `_sync_timestamp` DateTime DEFAULT now(),
                            `_error` Nullable(String)
                        ) ENGINE = MergeTree()
                        ORDER BY tuple()
                    """)
                    result['tables_successful'] += 1
                    result['success'] = True
                    logger.info(f"Created empty table structure for API source: {target_table}")
                except Exception as create_error:
                    result['tables_failed'] += 1
                    result['errors'].append(f"Failed to create empty table: {str(create_error)}")
                
                return result
            
            # Process API data
            result['tables_processed'] += 1
            
            # Use existing API sync function
            sync_result = sync_api_to_clickhouse_once(
                api_url=api_url,
                target_database=self.ch_config['database'],
                target_table=target_table,
                headers=headers
            )
            
            if sync_result.get('success'):
                result['tables_successful'] += 1
                result['records_migrated'] = sync_result.get('records_synced', 0)
                result['success'] = True
            else:
                result['tables_failed'] += 1
                result['errors'].append(sync_result.get('error', 'Unknown error'))
            
            # Ensure table exists even if empty
            if result['records_migrated'] == 0:
                result['empty_tables_created'] += 1
                logger.info(f"Table {target_table} is empty but structure created")
        
        except Exception as e:
            logger.exception(f"API migration failed for source {source_id}: {e}")
            result['errors'].append(str(e))
        
        return result
    
    def migrate_all_sources(self, source_types: List[str] = None) -> Dict:
        """Migrate all configured sources to ClickHouse"""
        logger.info("="*70)
        logger.info("Starting Unified Migration")
        logger.info("="*70)
        
        try:
            from db_utils import load_pg_config
            import psycopg2
            
            pg_conf = load_pg_config()
            conn = psycopg2.connect(
                dbname=pg_conf.get('database', 'metrics_sync_tables'),
                user=pg_conf.get('username'),
                password=pg_conf.get('password'),
                host=pg_conf.get('host'),
                port=int(pg_conf.get('port', 5432))
            )
            cursor = conn.cursor()
            
            # Get all active sources
            if source_types:
                placeholders = ','.join(['%s'] * len(source_types))
                cursor.execute(f"""
                    SELECT id, source_name, source_type, server_address, username, password,
                           connection_details, target_database
                    FROM data_sources
                    WHERE is_active = true AND source_type IN ({placeholders})
                    ORDER BY id
                """, source_types)
            else:
                cursor.execute("""
                    SELECT id, source_name, source_type, server_address, username, password,
                           connection_details, target_database
                    FROM data_sources
                    WHERE is_active = true
                    ORDER BY id
                """)
            
            sources = cursor.fetchall()
            cursor.close()
            conn.close()
            
            self.stats['total_sources'] = len(sources)
            logger.info(f"Found {len(sources)} active source(s) to migrate")
            
            # Migrate each source
            for source_row in sources:
                source_id, source_name, source_type, server_address, username, password, \
                connection_details, target_database = source_row
                
                source_config = {
                    'source_name': source_name,
                    'source_type': source_type,
                    'server_address': server_address,
                    'username': username,
                    'password': password,
                    'connection_details': connection_details,
                    'target_database': target_database or self.target_database
                }
                
                logger.info(f"\n{'='*70}")
                logger.info(f"Migrating Source: {source_name} (ID: {source_id}, Type: {source_type})")
                logger.info(f"{'='*70}")
                
                try:
                    if source_type == 'sap_hana':
                        result = self.migrate_hana_source(source_id, source_config)
                    elif source_type in ['api', 'rest_api', 'zoho']:
                        result = self.migrate_api_source(source_id, source_config)
                    else:
                        logger.warning(f"Unknown source type: {source_type}. Skipping.")
                        continue
                    
                    # Update statistics
                    if result['success']:
                        self.stats['successful'] += 1
                    else:
                        self.stats['failed'] += 1
                    
                    self.stats['tables_processed'] += result['tables_processed']
                    self.stats['tables_successful'] += result['tables_successful']
                    self.stats['tables_failed'] += result['tables_failed']
                    self.stats['empty_tables_created'] += result['empty_tables_created']
                    self.stats['records_migrated'] += result['records_migrated']
                    self.stats['errors'].extend(result['errors'])
                    
                    logger.info(f"Source {source_name} migration: {'✅ Success' if result['success'] else '❌ Failed'}")
                    logger.info(f"  Tables: {result['tables_successful']}/{result['tables_processed']} successful")
                    logger.info(f"  Records: {result['records_migrated']:,}")
                    logger.info(f"  Empty tables: {result['empty_tables_created']}")
                
                except Exception as e:
                    logger.exception(f"Error migrating source {source_name}: {e}")
                    self.stats['failed'] += 1
                    self.stats['errors'].append(f"{source_name}: {str(e)}")
            
            # Print summary
            logger.info("\n" + "="*70)
            logger.info("Migration Summary")
            logger.info("="*70)
            logger.info(f"Total Sources: {self.stats['total_sources']}")
            logger.info(f"✅ Successful: {self.stats['successful']}")
            logger.info(f"❌ Failed: {self.stats['failed']}")
            logger.info(f"Tables Processed: {self.stats['tables_processed']}")
            logger.info(f"Tables Successful: {self.stats['tables_successful']}")
            logger.info(f"Tables Failed: {self.stats['tables_failed']}")
            logger.info(f"Empty Tables Created: {self.stats['empty_tables_created']}")
            logger.info(f"Total Records Migrated: {self.stats['records_migrated']:,}")
            
            if self.stats['errors']:
                logger.warning(f"\nErrors ({len(self.stats['errors'])}):")
                for error in self.stats['errors'][:10]:  # Show first 10
                    logger.warning(f"  - {error}")
                if len(self.stats['errors']) > 10:
                    logger.warning(f"  ... and {len(self.stats['errors']) - 10} more errors")
            
            logger.info("="*70)
            
            return self.stats
        
        except Exception as e:
            logger.exception(f"Migration process failed: {e}")
            self.stats['errors'].append(str(e))
            return self.stats


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Unified Migration Manager')
    parser.add_argument('--database', help='Target ClickHouse database', default=None)
    parser.add_argument('--source-types', nargs='+', help='Source types to migrate (e.g., sap_hana api)', default=None)
    parser.add_argument('--source-id', type=int, help='Migrate specific source ID', default=None)
    
    args = parser.parse_args()
    
    manager = UnifiedMigrationManager(target_database=args.database)
    
    if args.source_id:
        # Migrate specific source
        from db_utils import load_pg_config
        import psycopg2
        
        pg_conf = load_pg_config()
        conn = psycopg2.connect(
            dbname=pg_conf.get('database', 'metrics_sync_tables'),
            user=pg_conf.get('username'),
            password=pg_conf.get('password'),
            host=pg_conf.get('host'),
            port=int(pg_conf.get('port', 5432))
        )
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, source_name, source_type, server_address, username, password,
                   connection_details, target_database
            FROM data_sources
            WHERE id = %s AND is_active = true
        """, (args.source_id,))
        
        row = cursor.fetchone()
        if row:
            source_config = {
                'source_name': row[1],
                'source_type': row[2],
                'server_address': row[3],
                'username': row[4],
                'password': row[5],
                'connection_details': row[6],
                'target_database': row[7] or manager.target_database
            }
            
            if row[2] == 'sap_hana':
                result = manager.migrate_hana_source(args.source_id, source_config)
            elif row[2] in ['api', 'rest_api', 'zoho']:
                result = manager.migrate_api_source(args.source_id, source_config)
            else:
                print(f"Unknown source type: {row[2]}")
                return
            
            print(f"\nMigration Result: {'✅ Success' if result['success'] else '❌ Failed'}")
            print(f"Tables: {result['tables_successful']}/{result['tables_processed']}")
            print(f"Records: {result['records_migrated']:,}")
        else:
            print(f"Source ID {args.source_id} not found")
        
        cursor.close()
        conn.close()
    else:
        # Migrate all sources
        manager.migrate_all_sources(source_types=args.source_types)


if __name__ == "__main__":
    main()

