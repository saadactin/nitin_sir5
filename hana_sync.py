"""
SAP HANA to ClickHouse Data Sync Module
Handles connection, schema extraction, data type mapping, and data migration
"""

import hdbcli.dbapi as hana_dbapi
from clickhouse_driver import Client
import pandas as pd
import logging
from typing import Dict, List, Optional, Tuple
import time
from datetime import datetime, time as time_type
import re

logger = logging.getLogger(__name__)


class HanaToClickHouseSync:
    def __init__(self, hana_config: dict, clickhouse_config: dict):
        self.hana_config = hana_config
        self.clickhouse_config = clickhouse_config
        self.hana_conn = None
        self.ch_client = None
    
    def connect_hana(self) -> bool:
        """Establish connection to SAP HANA"""
        try:
            self.hana_conn = hana_dbapi.connect(
                address=self.hana_config['host'],
                port=int(self.hana_config.get('port', 30015)),
                user=self.hana_config['username'],
                password=self.hana_config['password'],
                encrypt=True,
                sslValidateCertificate=False
            )
            logger.info(f"Connected to HANA: {self.hana_config['host']}:{self.hana_config['port']}")
            return True
        except Exception as e:
            logger.error(f"HANA connection failed: {e}")
            return False
    
    def connect_clickhouse(self) -> bool:
        """Establish connection to ClickHouse"""
        try:
            self.ch_client = Client(
                host=self.clickhouse_config.get('host', 'localhost'),
                port=int(self.clickhouse_config.get('port', 9000)),
                user=self.clickhouse_config.get('user', 'default'),
                password=self.clickhouse_config.get('password', ''),
                database=self.clickhouse_config.get('database', 'default')
            )
            logger.info(f"Connected to ClickHouse: {self.clickhouse_config.get('database')}")
            return True
        except Exception as e:
            logger.error(f"ClickHouse connection failed: {e}")
            return False
    
    def close_connections(self):
        """Close all database connections"""
        if self.hana_conn:
            try:
                self.hana_conn.close()
                logger.info("HANA connection closed")
            except:
                pass
        if self.ch_client:
            try:
                self.ch_client.disconnect()
                logger.info("ClickHouse connection closed")
            except:
                pass
    
    def get_hana_schemas(self) -> List[str]:
        """Get all user-accessible schemas from HANA"""
        try:
            cursor = self.hana_conn.cursor()
            cursor.execute("""
                SELECT SCHEMA_NAME 
                FROM SYS.SCHEMAS 
                WHERE SCHEMA_NAME NOT IN ('SYS', '_SYS_BI', '_SYS_BIC', '_SYS_EPM', '_SYS_REPO', '_SYS_STATISTICS')
                ORDER BY SCHEMA_NAME
            """)
            schemas = [row[0] for row in cursor.fetchall()]
            cursor.close()
            logger.info(f"Found {len(schemas)} schemas in HANA")
            return schemas
        except Exception as e:
            logger.error(f"Failed to get HANA schemas: {e}")
            return []
    
    def get_hana_tables(self, schema: str) -> List[Dict]:
        """Get all tables in a schema with metadata"""
        try:
            cursor = self.hana_conn.cursor()
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
            logger.info(f"Found {len(tables)} tables in schema {schema}")
            return tables
        except Exception as e:
            logger.error(f"Failed to get tables for schema {schema}: {e}")
            return []
    
    def get_hana_table_schema(self, schema: str, table: str) -> List[Dict]:
        """Get detailed column information for a table"""
        try:
            cursor = self.hana_conn.cursor()
            query = """
            SELECT 
                COLUMN_NAME,
                DATA_TYPE_NAME,
                LENGTH,
                SCALE,
                IS_NULLABLE,
                DEFAULT_VALUE
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
                    'nullable': row[4] == 'TRUE',
                    'default': row[5]
                })
            cursor.close()
            return columns
        except Exception as e:
            logger.error(f"Failed to get schema for {schema}.{table}: {e}")
            return []
    
    def map_hana_to_clickhouse_type(self, hana_type: str, length: Optional[int] = None, 
                                   scale: Optional[int] = None) -> str:
        """Map HANA data types to ClickHouse data types"""
        type_mapping = {
            'TINYINT': 'Int8',
            'SMALLINT': 'Int16',
            'INTEGER': 'Int32',
            'INT': 'Int32',
            'BIGINT': 'Int64',
            'DECIMAL': f'Decimal64({scale or 2})',
            'NUMERIC': f'Decimal64({scale or 2})',
            'REAL': 'Float32',
            'DOUBLE': 'Float64',
            'SMALLDECIMAL': 'Decimal32(2)',
            'VARCHAR': 'String',
            'NVARCHAR': 'String',
            'CHAR': f'FixedString({min(length or 1, 65535)})',
            'NCHAR': f'FixedString({min(length or 1, 65535)})',
            'DATE': 'Date',
            'TIME': 'String',
            'TIMESTAMP': 'DateTime64',
            'SECONDDATE': 'DateTime',
            'BOOLEAN': 'UInt8',
            'BLOB': 'String',
            'CLOB': 'String',
            'NCLOB': 'String',
            'TEXT': 'String'
        }
        
        # Handle array types
        if hana_type.startswith('ARRAY'):
            return 'Array(String)'
        
        # Handle VARCHAR with length
        if hana_type == 'VARCHAR' and length and length <= 255:
            return f'FixedString({length})'
        
        # Default to String for unknown types
        return type_mapping.get(hana_type, 'String')
    
    def create_clickhouse_table_name(self, schema: str, table: str) -> str:
        """Create ClickHouse table name in format: schema_tablename"""
        # Remove special characters and ensure valid ClickHouse table name
        clean_schema = re.sub(r'[^a-zA-Z0-9_]', '_', schema.replace('"', '').upper())
        clean_table = re.sub(r'[^a-zA-Z0-9_]', '_', table.replace('"', '').upper())
        return f"{clean_schema}_{clean_table}"
    
    def create_clickhouse_table(self, schema: str, table: str, columns: List[Dict]) -> bool:
        """Create table in ClickHouse with mapped schema using combined naming"""
        try:
            ch_table_name = self.create_clickhouse_table_name(schema, table)
            database_name = self.clickhouse_config.get('database', 'hana_migrated')
            
            # Create database if not exists
            self.ch_client.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
            
            # Build column definitions
            column_defs = []
            for col in columns:
                ch_type = self.map_hana_to_clickhouse_type(col['type'], col['length'], col['scale'])
                nullable = "Nullable(" + ch_type + ")" if col['nullable'] else ch_type
                column_defs.append(f"`{col['name']}` {nullable}")
            
            # Add metadata columns
            column_defs.append("`_source_schema` String")
            column_defs.append("`_source_table` String")
            column_defs.append("`_sync_timestamp` DateTime DEFAULT now()")
            
            # Create table
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {database_name}.{ch_table_name}
            (
                {', '.join(column_defs)}
            )
            ENGINE = MergeTree()
            ORDER BY tuple()
            SETTINGS allow_nullable_key = 1
            """
            
            self.ch_client.execute(create_table_sql)
            logger.info(f"Created table: {database_name}.{ch_table_name} (from {schema}.{table})")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create table {database_name}.{ch_table_name}: {e}")
            return False
    
    def migrate_table_data(self, schema: str, table: str, batch_size: int = 10000) -> Dict:
        """Migrate data from HANA to ClickHouse in batches with combined table names"""
        try:
            start_time = time.time()
            ch_table_name = self.create_clickhouse_table_name(schema, table)
            database_name = self.clickhouse_config.get('database', 'hana_migrated')
            
            # Get total row count
            cursor = self.hana_conn.cursor()
            try:
                cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
                total_rows = cursor.fetchone()[0]
            except Exception as e:
                logger.warning(f"Could not get row count for {schema}.{table}: {e}")
                total_rows = 0
            finally:
                cursor.close()
            
            if total_rows == 0:
                logger.info(f"Table {schema}.{table} is empty, skipping migration")
                return {
                    'source_table': f"{schema}.{table}",
                    'target_table': f"{database_name}.{ch_table_name}",
                    'total_rows': 0,
                    'migrated_rows': 0,
                    'elapsed_time': 0,
                    'status': 'success'
                }
            
            logger.info(f"Starting migration of {total_rows} rows from {schema}.{table} to {database_name}.{ch_table_name}")
            
            # Get column names first
            cursor = self.hana_conn.cursor()
            cursor.execute(f'SELECT * FROM "{schema}"."{table}" LIMIT 0')
            column_names = [desc[0] for desc in cursor.description]
            cursor.close()
            
            # Migrate in batches
            migrated_rows = 0
            offset = 0
            
            while offset < total_rows:
                cursor = self.hana_conn.cursor()
                try:
                    query = f'SELECT * FROM "{schema}"."{table}" LIMIT {batch_size} OFFSET {offset}'
                    cursor.execute(query)
                    
                    # Fetch batch data
                    batch_data = cursor.fetchall()
                finally:
                    cursor.close()
                
                if not batch_data:
                    break
                
                # Prepare data for ClickHouse with source metadata
                rows_to_insert = []
                for row in batch_data:
                    converted_row = []
                    # Convert original data
                    for value in row:
                        if value is None:
                            converted_row.append(None)
                        elif isinstance(value, (datetime, time_type)):
                            converted_row.append(str(value))
                        elif isinstance(value, bytes):
                            converted_row.append(value.decode('utf-8', errors='ignore'))
                        else:
                            converted_row.append(value)
                    # Add source metadata
                    converted_row.append(schema)  # _source_schema
                    converted_row.append(table)   # _source_table
                    rows_to_insert.append(converted_row)
                
                # Insert into ClickHouse
                if rows_to_insert:
                    # Add source metadata columns to column list
                    all_columns = column_names + ['_source_schema', '_source_table']
                    columns_str = ', '.join([f'`{col}`' for col in all_columns])
                    
                    insert_sql = f"INSERT INTO {database_name}.{ch_table_name} ({columns_str}) VALUES"
                    self.ch_client.execute(insert_sql, rows_to_insert)
                
                migrated_rows += len(batch_data)
                offset += batch_size
                
                logger.info(f"Progress: {migrated_rows}/{total_rows} rows migrated to {ch_table_name}")
            
            elapsed_time = time.time() - start_time
            logger.info(f"Completed migration: {migrated_rows} rows in {elapsed_time:.2f}s")
            
            return {
                'source_table': f"{schema}.{table}",
                'target_table': f"{database_name}.{ch_table_name}",
                'total_rows': total_rows,
                'migrated_rows': migrated_rows,
                'elapsed_time': elapsed_time,
                'status': 'success'
            }
            
        except Exception as e:
            logger.error(f"Migration failed for {schema}.{table}: {e}")
            return {
                'source_table': f"{schema}.{table}",
                'error': str(e),
                'status': 'failed'
            }
    
    def table_exists_in_clickhouse(self, database: str, table: str) -> bool:
        """Check if table exists in ClickHouse"""
        try:
            result = self.ch_client.execute(
                "SELECT name FROM system.tables WHERE database = %s AND name = %s",
                [database, table]
            )
            return len(result) > 0
        except Exception:
            return False
    
    def setup_incremental_sync(self, schema: str, table: str, timestamp_column: str = None) -> bool:
        """Setup incremental sync with combined table names"""
        try:
            ch_table_name = self.create_clickhouse_table_name(schema, table)
            database_name = self.clickhouse_config.get('database', 'hana_migrated')
            
            # Create sync metadata table
            self.ch_client.execute(f"""
            CREATE TABLE IF NOT EXISTS {database_name}.sync_metadata
            (
                source_schema String,
                source_table String,
                target_table String,
                last_sync_timestamp DateTime,
                last_sync_id UInt64,
                sync_enabled UInt8
            )
            ENGINE = MergeTree()
            ORDER BY (source_schema, source_table)
            """)
            
            # If no timestamp column specified, try to find one
            if not timestamp_column:
                columns = self.get_hana_table_schema(schema, table)
                timestamp_columns = [col['name'] for col in columns 
                                   if col['type'] in ['TIMESTAMP', 'SECONDDATE', 'DATE']]
                timestamp_column = timestamp_columns[0] if timestamp_columns else None
            
            if timestamp_column:
                # Store sync configuration
                self.ch_client.execute(f"""
                INSERT INTO {database_name}.sync_metadata 
                (source_schema, source_table, target_table, last_sync_timestamp, sync_enabled)
                VALUES
                """, [[schema, table, ch_table_name, datetime.now(), 1]])
                
                logger.info(f"Setup incremental sync for {schema}.{table} -> {ch_table_name} using column: {timestamp_column}")
                return True
            else:
                logger.warning(f"No suitable timestamp column found for incremental sync on {schema}.{table}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to setup incremental sync for {schema}.{table}: {e}")
            return False
    
    def perform_incremental_sync(self, schema: str, table: str) -> Dict:
        """Perform incremental sync based on last sync timestamp"""
        try:
            ch_table_name = self.create_clickhouse_table_name(schema, table)
            database_name = self.clickhouse_config.get('database', 'hana_migrated')
            
            # Get last sync timestamp
            result = self.ch_client.execute(f"""
            SELECT last_sync_timestamp FROM {database_name}.sync_metadata 
            WHERE source_schema = %s AND source_table = %s AND sync_enabled = 1
            """, [schema, table])
            
            if not result:
                return {'status': 'error', 'message': 'No sync configuration found'}
            
            last_sync = result[0][0]
            
            # Get new/changed records from HANA
            cursor = self.hana_conn.cursor()
            query = f"""
            SELECT * FROM "{schema}"."{table}" 
            WHERE _sync_timestamp > ? OR _last_modified > ?
            ORDER BY _sync_timestamp
            """
            cursor.execute(query, (last_sync, last_sync))
            
            new_data = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            cursor.close()
            
            # Insert into ClickHouse
            if new_data:
                columns_str = ', '.join([f'`{col}`' for col in column_names])
                self.ch_client.execute(f"INSERT INTO {database_name}.{ch_table_name} ({columns_str}) VALUES", new_data)
                
                # Update sync timestamp
                self.ch_client.execute(f"""
                ALTER TABLE {database_name}.sync_metadata 
                UPDATE last_sync_timestamp = %s
                WHERE source_schema = %s AND source_table = %s
                """, [datetime.now(), schema, table])
            
            return {
                'status': 'success',
                'table': f"{schema}.{table}",
                'target_table': f"{database_name}.{ch_table_name}",
                'new_records': len(new_data),
                'last_sync': last_sync
            }
            
        except Exception as e:
            logger.error(f"Incremental sync failed for {schema}.{table}: {e}")
            return {'status': 'error', 'message': str(e)}

