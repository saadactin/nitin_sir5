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
from db_utils import load_clickhouse_config

logger = logging.getLogger(__name__)


class HanaToClickHouseSync:
    def __init__(self, hana_config: dict, clickhouse_config: dict):
        self.hana_config = hana_config
        self.clickhouse_config = clickhouse_config
        self.hana_conn = None
        self.ch_client = None
    
    def connect_hana(self) -> bool:
        """
        Establish connection to SAP HANA using config from .env.
        
        Uses load_hana_config() if hana_config is not provided, otherwise uses
        the provided hana_config (for backward compatibility with stored sources).
        """
        try:
            # If hana_config doesn't have required fields, load from .env
            if not self.hana_config.get('host') or not self.hana_config.get('username'):
                logger.info("HANA config incomplete, loading from .env...")
                env_config = load_hana_config()
                # Merge with provided config (allows overriding with source-specific values)
                self.hana_config = {**env_config, **self.hana_config}
            
            # Validate required fields are present
            if not self.hana_config.get('host'):
                raise ValueError("HANA host is required")
            if not self.hana_config.get('username'):
                raise ValueError("HANA username is required")
            
            # Get port (required, no default)
            port = int(self.hana_config.get('port'))
            if not port:
                raise ValueError("HANA port is required")
            
            self.hana_conn = hana_dbapi.connect(
                address=self.hana_config['host'],
                port=port,
                user=self.hana_config['username'],
                password=self.hana_config.get('password', ''),
                encrypt=True,
                sslValidateCertificate=False
            )
            logger.info(f"Connected to HANA: {self.hana_config['host']}:{port}")
            return True
        except Exception as e:
            logger.error(f"HANA connection failed: {e}")
            return False
    
    def connect_clickhouse(self) -> bool:
        """
        Establish connection to ClickHouse using environment variables.
        
        Uses load_clickhouse_config() to get connection settings from .env file.
        The 'database' field from clickhouse_config is used for the database name.
        """
        try:
            # Load ClickHouse config from .env (no hardcoded values)
            ch_config = load_clickhouse_config()
            
            # Get database name from clickhouse_config dict (passed by caller)
            database = self.clickhouse_config.get('database', 'default')
            
            self.ch_client = Client(
                host=ch_config['host'],
                port=ch_config['port'],
                user=ch_config['user'],
                password=ch_config['password'],
                database=database
            )
            logger.info(f"Connected to ClickHouse: {ch_config['host']}:{ch_config['port']} (database: {database})")
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
            # ClickHouse doesn't support %s placeholders - use string formatting with escaping
            result = self.ch_client.execute(
                f"SELECT name FROM system.tables WHERE database = '{database.replace("'", "''")}' AND name = '{table.replace("'", "''")}'"
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
        """
        Perform incremental sync based on last sync timestamp.
        
        Tries multiple strategies:
        1. Uses timestamp column if available (e.g., CREATED_AT, UPDATED_AT, TIMESTAMP)
        2. Falls back to ID-based incremental if timestamp not available
        3. Returns error if neither available
        """
        try:
            ch_table_name = self.create_clickhouse_table_name(schema, table)
            database_name = self.clickhouse_config.get('database', 'hana_migrated')
            
            # Get last sync timestamp from metadata
            try:
                # ClickHouse doesn't support %s placeholders - use string formatting with escaping
                result = self.ch_client.execute(f"""
                SELECT last_sync_timestamp, last_sync_id 
                FROM {database_name}.sync_metadata 
                WHERE source_schema = '{schema.replace("'", "''")}' AND source_table = '{table.replace("'", "''")}' AND sync_enabled = 1
                """)
                
                if not result:
                    return {'status': 'error', 'message': 'No sync configuration found. Run initial sync first.'}
                
                last_sync = result[0][0] if result[0][0] else datetime(1970, 1, 1)  # Default to epoch if None
                last_sync_id = result[0][1] if len(result[0]) > 1 and result[0][1] else None
            except Exception as e:
                logger.warning(f"Could not get last sync timestamp: {e}")
                last_sync = datetime(1970, 1, 1)  # Start from beginning
                last_sync_id = None
            
            # Get table columns to find best sync column
            columns = self.get_hana_table_schema(schema, table)
            if not columns:
                return {'status': 'error', 'message': 'Could not get table schema'}
            
            column_names = [col['name'] for col in columns]
            
            # Find timestamp columns for incremental sync
            timestamp_columns = [col['name'] for col in columns 
                               if col['type'] in ['TIMESTAMP', 'SECONDDATE', 'DATE', 'TIME']]
            
            # Also check for common timestamp column names
            common_timestamp_names = ['CREATED_AT', 'UPDATED_AT', 'MODIFIED_AT', 'TIMESTAMP', 
                                    'LAST_MODIFIED', 'CHANGE_TIME', 'UPDATE_TIME', 'CREATED_TIME']
            for col_name in column_names:
                if col_name.upper() in common_timestamp_names:
                    timestamp_columns.insert(0, col_name)
            
            # Find ID column for fallback
            id_columns = [col['name'] for col in columns 
                        if col['type'] in ['BIGINT', 'INTEGER', 'INT'] and 
                        col['name'].upper() in ['ID', 'ROWID', 'ROW_ID', 'RECORD_ID']]
            
            new_data = []
            column_names_for_insert = []
            
            # Strategy 1: Use timestamp column if available
            if timestamp_columns:
                timestamp_col = timestamp_columns[0]
                logger.info(f"Using timestamp column '{timestamp_col}' for incremental sync of {schema}.{table}")
                
                try:
                    cursor = self.hana_conn.cursor()
                    # Convert last_sync to appropriate format for HANA
                    query = f"""
                    SELECT * FROM "{schema}"."{table}" 
                    WHERE "{timestamp_col}" > ?
                    ORDER BY "{timestamp_col}" ASC
                    """
                    cursor.execute(query, (last_sync,))
                    new_data = cursor.fetchall()
                    column_names_for_insert = [desc[0] for desc in cursor.description]
                    cursor.close()
                except Exception as e:
                    logger.warning(f"Timestamp-based sync failed: {e}, trying ID-based")
                    new_data = []
            
            # Strategy 2: Use ID column if timestamp failed and ID available
            if not new_data and id_columns and last_sync_id is not None:
                id_col = id_columns[0]
                logger.info(f"Using ID column '{id_col}' for incremental sync of {schema}.{table}")
                
                try:
                    cursor = self.hana_conn.cursor()
                    query = f"""
                    SELECT * FROM "{schema}"."{table}" 
                    WHERE "{id_col}" > ?
                    ORDER BY "{id_col}" ASC
                    """
                    cursor.execute(query, (last_sync_id,))
                    new_data = cursor.fetchall()
                    column_names_for_insert = [desc[0] for desc in cursor.description]
                    cursor.close()
                except Exception as e:
                    logger.warning(f"ID-based sync failed: {e}")
                    new_data = []
            
            # Strategy 3: If no suitable column, return error
            if not new_data and not timestamp_columns and not id_columns:
                return {
                    'status': 'error', 
                    'message': f'No timestamp or ID column found for incremental sync. Table needs CREATED_AT, UPDATED_AT, or ID column.',
                    'new_records': 0
                }
            
            # If no new data, still return success
            if not new_data:
                # Update last sync timestamp anyway (nothing changed)
                try:
                    # Use DELETE + INSERT for compatibility
                    self.ch_client.execute(f"""
                    ALTER TABLE {database_name}.sync_metadata 
                    DELETE WHERE source_schema = '{schema.replace("'", "''")}' AND source_table = '{table.replace("'", "''")}'
                    """)
                    self.ch_client.execute(f"""
                    INSERT INTO {database_name}.sync_metadata 
                    (source_schema, source_table, target_table, last_sync_timestamp, sync_enabled)
                    VALUES
                    """, [[schema, table, ch_table_name, datetime.now(), 1]])
                except:
                    pass  # Ignore update errors if metadata doesn't support ALTER UPDATE
                
                return {
                    'status': 'success',
                    'table': f"{schema}.{table}",
                    'target_table': f"{database_name}.{ch_table_name}",
                    'new_records': 0,
                    'last_sync': last_sync
                }
            
            # Ensure ClickHouse table exists
            if not self.table_exists_in_clickhouse(database_name, ch_table_name):
                logger.info(f"Table {ch_table_name} doesn't exist, creating it...")
                self.create_clickhouse_table(schema, table, columns)
            
            # Prepare data for ClickHouse with source metadata
            rows_to_insert = []
            for row in new_data:
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
                all_columns = column_names_for_insert + ['_source_schema', '_source_table']
                columns_str = ', '.join([f'`{col}`' for col in all_columns])
                
                insert_sql = f"INSERT INTO {database_name}.{ch_table_name} ({columns_str}) VALUES"
                self.ch_client.execute(insert_sql, rows_to_insert)
                
                # Update sync timestamp in metadata
                try:
                    # Get latest timestamp/ID from synced data for next sync
                    latest_timestamp = last_sync
                    latest_id = last_sync_id
                    
                    if timestamp_columns:
                        # Find the max timestamp value in the new data
                        timestamp_idx = column_names_for_insert.index(timestamp_columns[0])
                        latest_timestamp = max(row[timestamp_idx] for row in new_data if row[timestamp_idx] is not None)
                    
                    if id_columns:
                        # Find the max ID value in the new data
                        id_idx = column_names_for_insert.index(id_columns[0])
                        latest_id = max(row[id_idx] for row in new_data if row[id_idx] is not None)
                    
                    # Update metadata (ClickHouse ALTER UPDATE syntax)
                    # Note: ClickHouse ALTER UPDATE might not be supported in all versions
                    # Use DELETE + INSERT instead for compatibility
                    try:
                        # Delete old record
                        self.ch_client.execute(f"""
                        ALTER TABLE {database_name}.sync_metadata 
                        DELETE WHERE source_schema = '{schema.replace("'", "''")}' AND source_table = '{table.replace("'", "''")}'
                        """)
                        # Insert updated record
                        self.ch_client.execute(f"""
                        INSERT INTO {database_name}.sync_metadata 
                        (source_schema, source_table, target_table, last_sync_timestamp, last_sync_id, sync_enabled)
                        VALUES
                        """, [[schema, table, ch_table_name, latest_timestamp, latest_id or 0, 1]])
                    except Exception as update_error:
                        # If DELETE not supported, try ALTER UPDATE (might not work in all ClickHouse versions)
                        logger.warning(f"Could not update with DELETE, trying ALTER UPDATE: {update_error}")
                        # Format timestamp as string for SQL
                        timestamp_str = latest_timestamp.strftime("'%Y-%m-%d %H:%M:%S'") if isinstance(latest_timestamp, datetime) else f"'{latest_timestamp}'"
                        self.ch_client.execute(f"""
                        ALTER TABLE {database_name}.sync_metadata 
                        UPDATE last_sync_timestamp = {timestamp_str}, last_sync_id = {latest_id or 0}
                        WHERE source_schema = '{schema.replace("'", "''")}' AND source_table = '{table.replace("'", "''")}'
                        """)
                except Exception as e:
                    logger.warning(f"Could not update sync metadata: {e}. Using current time.")
                    # Fallback: use current time
                    try:
                        # Use DELETE + INSERT for compatibility
                        self.ch_client.execute(f"""
                        ALTER TABLE {database_name}.sync_metadata 
                        DELETE WHERE source_schema = '{schema.replace("'", "''")}' AND source_table = '{table.replace("'", "''")}'
                        """)
                        self.ch_client.execute(f"""
                        INSERT INTO {database_name}.sync_metadata 
                        (source_schema, source_table, target_table, last_sync_timestamp, sync_enabled)
                        VALUES
                        """, [[schema, table, ch_table_name, datetime.now(), 1]])
                    except:
                        pass  # Ignore if ALTER UPDATE not supported
            
            return {
                'status': 'success',
                'table': f"{schema}.{table}",
                'target_table': f"{database_name}.{ch_table_name}",
                'new_records': len(new_data),
                'last_sync': last_sync,
                'records_synced': len(new_data)
            }
            
        except Exception as e:
            logger.error(f"Incremental sync failed for {schema}.{table}: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return {'status': 'error', 'message': str(e), 'records_synced': 0}
    
    def sync_incremental(self, database: str = None) -> List[Dict]:
        """
        Perform incremental sync for all tables configured for incremental sync.
        
        This method is called by the scheduler to sync all tables that have
        incremental sync enabled. It:
        1. Gets all tables from sync_metadata table that have sync_enabled = 1
        2. For each table, performs incremental sync
        3. Returns results for all tables
        
        Args:
            database: ClickHouse database name (optional, uses from clickhouse_config if not provided)
            
        Returns:
            List of sync results for each table
        """
        try:
            database_name = database or self.clickhouse_config.get('database', 'hana_migrated')
            
            # Ensure sync_metadata table exists
            try:
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
            except Exception as e:
                logger.warning(f"Could not create sync_metadata table: {e}")
            
            # Get all tables configured for incremental sync
            try:
                metadata_records = self.ch_client.execute(f"""
                SELECT source_schema, source_table, target_table, last_sync_timestamp
                FROM {database_name}.sync_metadata
                WHERE sync_enabled = 1
                ORDER BY source_schema, source_table
                """)
            except Exception as e:
                logger.warning(f"Could not read sync_metadata table: {e}")
                metadata_records = []
            
            if not metadata_records:
                logger.info("No tables configured for incremental sync. Setting up initial sync for all tables.")
                # If no metadata, do initial setup - get all schemas and tables
                schemas = self.get_hana_schemas()
                metadata_records = []
                
                # Limit to reasonable number of schemas for performance
                for schema in schemas[:20]:  # First 20 schemas
                    tables = self.get_hana_tables(schema)
                    for table_info in tables:
                        table_name = table_info['name']
                        # Setup incremental sync for this table
                        if self.setup_incremental_sync(schema, table_name):
                            metadata_records.append((schema, table_name, None, datetime.now()))
            
            results = []
            total_records_synced = 0
            
            for record in metadata_records:
                schema = record[0]
                table = record[1]
                target_table = record[2] if len(record) > 2 else None
                last_sync = record[3] if len(record) > 3 else None
                
                try:
                    logger.info(f"Performing incremental sync for {schema}.{table}")
                    result = self.perform_incremental_sync(schema, table)
                    
                    if result.get('status') == 'success':
                        new_records = result.get('new_records', 0)
                        total_records_synced += new_records
                        logger.info(f"✅ Synced {new_records} new records from {schema}.{table}")
                    elif result.get('status') == 'error':
                        # If incremental sync fails (e.g., no timestamp column), try full sync
                        logger.warning(f"Incremental sync failed for {schema}.{table}: {result.get('message')}")
                        logger.info(f"Attempting full sync for {schema}.{table}...")
                        
                        # Get table schema and ensure table exists
                        columns = self.get_hana_table_schema(schema, table)
                        if columns:
                            self.create_clickhouse_table(schema, table, columns)
                            # Do full sync instead
                            full_result = self.migrate_table_data(schema, table)
                            if full_result.get('status') == 'success':
                                total_records_synced += full_result.get('migrated_rows', 0)
                                # Update metadata for next incremental sync
                                self.setup_incremental_sync(schema, table)
                                result = {
                                    'status': 'success',
                                    'table': f"{schema}.{table}",
                                    'new_records': full_result.get('migrated_rows', 0),
                                    'sync_type': 'full_fallback'
                                }
                            else:
                                result['sync_type'] = 'failed'
                        else:
                            result['sync_type'] = 'skipped_no_schema'
                    else:
                        result['sync_type'] = 'unknown'
                    
                    results.append(result)
                    
                except Exception as e:
                    logger.error(f"Error syncing {schema}.{table}: {e}")
                    results.append({
                        'status': 'error',
                        'table': f"{schema}.{table}",
                        'error': str(e),
                        'records_synced': 0
                    })
            
            logger.info(f"Incremental sync completed: {total_records_synced} total records synced across {len(results)} tables")
            
            return results
            
        except Exception as e:
            logger.error(f"Incremental sync process failed: {e}")
            return [{'status': 'error', 'message': str(e), 'records_synced': 0}]

