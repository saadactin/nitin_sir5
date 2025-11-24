"""
Migration Validator
Validates and verifies that all data sources have been properly migrated to ClickHouse
"""

import os
import sys
import logging
from typing import Dict, List, Optional
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MigrationValidator:
    """Validates migration completeness and correctness"""
    
    def __init__(self, target_database: str = None):
        """Initialize validator"""
        from db_utils import load_clickhouse_config
        
        ch_config = load_clickhouse_config()
        self.target_database = target_database or os.environ.get('CLICKHOUSE_DATABASE', 'JARVIS_DB')
        self.ch_config = ch_config
    
    def get_clickhouse_client(self):
        """Get ClickHouse client"""
        try:
            from clickhouse_connect import get_client
            return get_client(
                host=self.ch_config['host'],
                port=self.ch_config.get('port', 9000),
                username=self.ch_config['user'],
                password=self.ch_config['password'],
                database=self.target_database
            )
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
    
    def validate_hana_migration(self, source_id: int) -> Dict:
        """Validate HANA source migration"""
        logger.info(f"Validating HANA source {source_id}")
        
        result = {
            'source_id': source_id,
            'source_type': 'sap_hana',
            'valid': False,
            'tables_checked': 0,
            'tables_found': 0,
            'tables_missing': [],
            'empty_tables': [],
            'data_mismatches': [],
            'errors': []
        }
        
        try:
            from db_utils import load_pg_config, load_hana_config
            import psycopg2
            import json
            from hana_sync import HanaToClickHouseSync
            
            # Get source config
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
                SELECT source_name, server_address, username, password, connection_details, target_database
                FROM data_sources
                WHERE id = %s AND source_type = 'sap_hana'
            """, (source_id,))
            
            row = cursor.fetchone()
            if not row:
                result['errors'].append("Source not found")
                return result
            
            source_name, server_address, username, password, connection_details, target_db = row
            connection_details = json.loads(connection_details) if isinstance(connection_details, str) else connection_details
            
            # Connect to HANA
            if ':' in server_address:
                host, port = server_address.split(':', 1)
            else:
                host = connection_details.get('host', server_address)
                port = connection_details.get('port')
            
            try:
                hana_base_config = load_hana_config()
                hana_config = {
                    'host': host or hana_base_config['host'],
                    'port': int(port) if port else hana_base_config['port'],
                    'username': username or hana_base_config['username'],
                    'password': password or hana_base_config['password']
                }
            except ValueError:
                hana_config = {
                    'host': host,
                    'port': int(port),
                    'username': username,
                    'password': password or ''
                }
            
            clickhouse_config = {
                'host': self.ch_config['host'],
                'port': self.ch_config['port'],
                'user': self.ch_config['user'],
                'password': self.ch_config['password'],
                'database': target_db or self.target_database
            }
            
            sync_engine = HanaToClickHouseSync(hana_config, clickhouse_config)
            
            if not sync_engine.connect_hana():
                result['errors'].append("Failed to connect to HANA")
                return result
            
            ch_client = self.get_clickhouse_client()
            
            # Get all HANA tables
            schemas = sync_engine.get_hana_schemas()
            for schema in schemas:
                tables = sync_engine.get_hana_tables(schema)
                
                for table_info in tables:
                    if table_info['type'] != 'TABLE':
                        continue
                    
                    table = table_info['name']
                    result['tables_checked'] += 1
                    ch_table_name = sync_engine.create_clickhouse_table_name(schema, table)
                    
                    # Check if table exists in ClickHouse
                    try:
                        ch_client.query_df(f"SELECT COUNT(*) as cnt FROM {clickhouse_config['database']}.{ch_table_name}")
                        result['tables_found'] += 1
                        
                        # Check row count
                        hana_cursor = sync_engine.hana_conn.cursor()
                        hana_cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
                        hana_count = hana_cursor.fetchone()[0]
                        hana_cursor.close()
                        
                        ch_count_df = ch_client.query_df(f"SELECT COUNT(*) as cnt FROM {clickhouse_config['database']}.{ch_table_name}")
                        ch_count = ch_count_df.iloc[0]['cnt'] if not ch_count_df.empty else 0
                        
                        if hana_count == 0 and ch_count == 0:
                            result['empty_tables'].append(f"{schema}.{table}")
                        elif hana_count != ch_count:
                            result['data_mismatches'].append({
                                'table': f"{schema}.{table}",
                                'hana_count': hana_count,
                                'clickhouse_count': ch_count
                            })
                    
                    except Exception as e:
                        result['tables_missing'].append(f"{schema}.{table}")
                        logger.warning(f"Table {schema}.{table} not found in ClickHouse: {e}")
            
            sync_engine.close_connections()
            result['valid'] = len(result['tables_missing']) == 0 and len(result['data_mismatches']) == 0
            
        except Exception as e:
            logger.exception(f"Validation error for HANA source {source_id}: {e}")
            result['errors'].append(str(e))
        
        return result
    
    def validate_api_migration(self, source_id: int) -> Dict:
        """Validate API source migration"""
        logger.info(f"Validating API source {source_id}")
        
        result = {
            'source_id': source_id,
            'source_type': 'api',
            'valid': False,
            'table_exists': False,
            'table_empty': False,
            'records_count': 0,
            'errors': []
        }
        
        try:
            from db_utils import load_pg_config
            import psycopg2
            
            # Get source config
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
                SELECT source_name, target_database
                FROM data_sources
                WHERE id = %s AND source_type IN ('api', 'rest_api', 'zoho')
            """, (source_id,))
            
            row = cursor.fetchone()
            if not row:
                result['errors'].append("Source not found")
                return result
            
            source_name, target_db = row
            target_table = target_db or source_name.replace(' ', '_').lower()
            
            ch_client = self.get_clickhouse_client()
            
            # Check if table exists
            try:
                count_df = ch_client.query_df(f"SELECT COUNT(*) as cnt FROM {self.target_database}.{target_table}")
                result['table_exists'] = True
                result['records_count'] = count_df.iloc[0]['cnt'] if not count_df.empty else 0
                result['table_empty'] = result['records_count'] == 0
                result['valid'] = True
            except Exception as e:
                result['errors'].append(f"Table {target_table} not found: {str(e)}")
            
            cursor.close()
            conn.close()
        
        except Exception as e:
            logger.exception(f"Validation error for API source {source_id}: {e}")
            result['errors'].append(str(e))
        
        return result
    
    def validate_all_sources(self) -> Dict:
        """Validate all sources"""
        logger.info("="*70)
        logger.info("Starting Migration Validation")
        logger.info("="*70)
        
        results = {
            'total_sources': 0,
            'valid_sources': 0,
            'invalid_sources': 0,
            'details': []
        }
        
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
            cursor.execute("""
                SELECT id, source_name, source_type
                FROM data_sources
                WHERE is_active = true
                ORDER BY id
            """)
            
            sources = cursor.fetchall()
            cursor.close()
            conn.close()
            
            results['total_sources'] = len(sources)
            
            for source_id, source_name, source_type in sources:
                logger.info(f"\nValidating: {source_name} (ID: {source_id}, Type: {source_type})")
                
                if source_type == 'sap_hana':
                    validation_result = self.validate_hana_migration(source_id)
                elif source_type in ['api', 'rest_api', 'zoho']:
                    validation_result = self.validate_api_migration(source_id)
                else:
                    logger.warning(f"Unknown source type: {source_type}")
                    continue
                
                results['details'].append(validation_result)
                
                if validation_result['valid']:
                    results['valid_sources'] += 1
                    logger.info(f"✅ {source_name}: Valid")
                else:
                    results['invalid_sources'] += 1
                    logger.warning(f"❌ {source_name}: Invalid")
                    if validation_result.get('errors'):
                        for error in validation_result['errors']:
                            logger.warning(f"  Error: {error}")
            
            # Print summary
            logger.info("\n" + "="*70)
            logger.info("Validation Summary")
            logger.info("="*70)
            logger.info(f"Total Sources: {results['total_sources']}")
            logger.info(f"✅ Valid: {results['valid_sources']}")
            logger.info(f"❌ Invalid: {results['invalid_sources']}")
            logger.info("="*70)
            
            return results
        
        except Exception as e:
            logger.exception(f"Validation process failed: {e}")
            results['errors'] = [str(e)]
            return results


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Migration Validator')
    parser.add_argument('--database', help='Target ClickHouse database', default=None)
    parser.add_argument('--source-id', type=int, help='Validate specific source ID', default=None)
    
    args = parser.parse_args()
    
    validator = MigrationValidator(target_database=args.database)
    
    if args.source_id:
        # Validate specific source
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
        cursor.execute("SELECT source_type FROM data_sources WHERE id = %s", (args.source_id,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if row:
            if row[0] == 'sap_hana':
                result = validator.validate_hana_migration(args.source_id)
            elif row[0] in ['api', 'rest_api', 'zoho']:
                result = validator.validate_api_migration(args.source_id)
            else:
                print(f"Unknown source type: {row[0]}")
                return
            
            print(f"\nValidation Result: {'✅ Valid' if result['valid'] else '❌ Invalid'}")
            if result.get('errors'):
                for error in result['errors']:
                    print(f"  Error: {error}")
        else:
            print(f"Source ID {args.source_id} not found")
    else:
        # Validate all sources
        validator.validate_all_sources()


if __name__ == "__main__":
    main()

