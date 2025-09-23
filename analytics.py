"""
Analytics module for SQL Server → Postgres hybrid sync.
Provides table/row-level analytics functionality.
"""

import pandas as pd
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from hybrid_sync import get_sqlalchemy_engine, get_pg_engine, get_sql_connection, get_primary_key_info, get_timestamp_column, get_unique_identifier_column
from manage_server import load_config

logger = logging.getLogger(__name__)


def compare_table_rows(server_name, db_name, table_name):
    """
    Compare source vs destination rows for a given table.
    Only compares data that should have been synced based on last sync time.
    
    Args:
        server_name (str): Name of the SQL Server
        db_name (str): Database name
        table_name (str): Table name (format: schema.table)
    
    Returns:
        dict: Contains rows_source, rows_destination, missing_rows, extra_rows
    """
    try:
        config = load_config()
        server_conf = config["sqlservers"].get(server_name)
        if not server_conf:
            raise ValueError(f"Server {server_name} not found in configuration")
        
        # Parse schema and table from table_name
        if '.' in table_name:
            schema, table = table_name.split('.', 1)
        else:
            schema = 'dbo'
            table = table_name
        
        # Get last sync status to determine what should be synced
        sync_status = get_table_sync_status(server_name, db_name, table_name)
        last_sync_time = sync_status.get('last_sync_time')
        last_pk_value = sync_status.get('last_pk_value')
        
        # Get source data - filter by last sync time if available
        sql_engine = get_sqlalchemy_engine(server_conf, db_name)
        
        # Build source query based on sync status
        if last_sync_time:
            # Try to use timestamp column first
            timestamp_column = get_timestamp_column(server_name, db_name, table_name)
            if timestamp_column:
                source_query = f"SELECT * FROM [{schema}].[{table}] WHERE [{timestamp_column}] <= '{last_sync_time}'"
                logger.info(f"Filtering source data by timestamp column: {timestamp_column} <= {last_sync_time}")
            elif last_pk_value:
                # Use primary key if timestamp not available
                pk_columns = get_primary_key_info(server_name, db_name, table_name)
                if pk_columns and len(pk_columns) == 1:  # Single column PK
                    pk_column = pk_columns[0]
                    source_query = f"SELECT * FROM [{schema}].[{table}] WHERE [{pk_column}] <= {last_pk_value}"
                    logger.info(f"Filtering source data by PK column: {pk_column} <= {last_pk_value}")
                else:
                    # Fallback: compare all data but warn
                    source_query = f"SELECT * FROM [{schema}].[{table}]"
                    logger.warning(f"No timestamp or single-column PK found for filtering: {table_name}. Comparing all data.")
            else:
                source_query = f"SELECT * FROM [{schema}].[{table}]"
                logger.info(f"No PK value available, comparing all source data for: {table_name}")
        else:
            # Never synced - compare all data
            source_query = f"SELECT * FROM [{schema}].[{table}]"
            logger.info(f"Table never synced, comparing all source data for: {table_name}")
        
        source_df = pd.read_sql(source_query, sql_engine)
        
        # Get total source count for context (including unsynced data)
        total_source_query = f"SELECT COUNT(*) as total_count FROM [{schema}].[{table}]"
        total_source_df = pd.read_sql(total_source_query, sql_engine)
        total_source_count = total_source_df.iloc[0]['total_count'] if not total_source_df.empty else 0
        
        # Get destination data (this should contain ONLY synced data)
        pg_engine = get_pg_engine()
        server_clean = ''.join(c for c in server_conf['server'] if c.isalnum() or c in '_-')
        schema_name = f"{server_clean}_{db_name}".replace('-', '_').replace(' ', '_')
        pg_table_name = f"{schema}_{table}"
        
        try:
            dest_query = f'SELECT * FROM "{schema_name}"."{pg_table_name}"'
            dest_df = pd.read_sql(dest_query, pg_engine)
        except Exception as e:
            # Table doesn't exist in destination
            logger.info(f"Destination table not found: {schema_name}.{pg_table_name}")
            dest_df = pd.DataFrame()
        
        # Calculate row hashes for comparison
        if not source_df.empty:
            source_df['row_hash'] = source_df.apply(lambda x: hash(tuple(x.fillna(''))), axis=1)
        else:
            source_df['row_hash'] = pd.Series(dtype='int64')
        
        if not dest_df.empty:
            dest_df['row_hash'] = dest_df.apply(lambda x: hash(tuple(x.fillna(''))), axis=1)
        else:
            dest_df['row_hash'] = pd.Series(dtype='int64')
        
        # Find missing and extra rows
        source_hashes = set(source_df['row_hash'].values) if not source_df.empty else set()
        dest_hashes = set(dest_df['row_hash'].values) if not dest_df.empty else set()
        
        missing_hashes = source_hashes - dest_hashes
        extra_hashes = dest_hashes - source_hashes
        
        missing_rows = source_df[source_df['row_hash'].isin(missing_hashes)].drop('row_hash', axis=1) if not source_df.empty else pd.DataFrame()
        extra_rows = dest_df[dest_df['row_hash'].isin(extra_hashes)].drop('row_hash', axis=1) if not dest_df.empty else pd.DataFrame()
        
        sql_engine.dispose()
        
        # Calculate new data since last sync
        new_data_since_sync = total_source_count - len(source_df)
        
        return {
            'rows_source': len(source_df),  # Only synced portion
            'rows_destination': len(dest_df),
            'missing_rows': missing_rows,
            'extra_rows': extra_rows,
            'source_data': source_df.drop('row_hash', axis=1) if not source_df.empty else pd.DataFrame(),
            'destination_data': dest_df.drop('row_hash', axis=1) if not dest_df.empty else pd.DataFrame(),
            'total_source_rows': total_source_count,  # Total including unsynced
            'last_sync_time': last_sync_time,
            'new_data_since_sync': new_data_since_sync,  # Unsynced data count
            'sync_status': sync_status['sync_status'],
            'data_match': len(missing_rows) == 0 and len(extra_rows) == 0
        }
        
    except Exception as e:
        logger.error(f"Error comparing table rows for {server_name}.{db_name}.{table_name}: {e}")
        raise


def delta_tracking(server_name, db_name, table_name):
    """
    Track rows added/updated since last sync using row_hash comparison.
    Only considers data that should have been synced.
    
    Args:
        server_name (str): Name of the SQL Server
        db_name (str): Database name
        table_name (str): Table name (format: schema.table)
    
    Returns:
        dict: Contains delta information
    """
    try:
        config = load_config()
        server_conf = config["sqlservers"].get(server_name)
        if not server_conf:
            raise ValueError(f"Server {server_name} not found in configuration")
        
        # Parse schema and table from table_name
        if '.' in table_name:
            schema, table = table_name.split('.', 1)
        else:
            schema = 'dbo'
            table = table_name
        
        # Get last sync status
        sync_status = get_table_sync_status(server_name, db_name, table_name)
        last_sync_time = sync_status.get('last_sync_time')
        
        # Get source data - filtered by last sync time
        sql_engine = get_sqlalchemy_engine(server_conf, db_name)
        
        if last_sync_time:
            timestamp_column = get_timestamp_column(server_name, db_name, table_name)
            if timestamp_column:
                source_query = f"SELECT * FROM [{schema}].[{table}] WHERE [{timestamp_column}] <= '{last_sync_time}'"
            else:
                source_query = f"SELECT * FROM [{schema}].[{table}]"
        else:
            source_query = f"SELECT * FROM [{schema}].[{table}]"
        
        source_df = pd.read_sql(source_query, sql_engine)
        
        # Get destination data
        pg_engine = get_pg_engine()
        server_clean = ''.join(c for c in server_conf['server'] if c.isalnum() or c in '_-')
        schema_name = f"{server_clean}_{db_name}".replace('-', '_').replace(' ', '_')
        pg_table_name = f"{schema}_{table}"
        
        try:
            dest_query = f'SELECT * FROM "{schema_name}"."{pg_table_name}"'
            dest_df = pd.read_sql(dest_query, pg_engine)
        except Exception:
            # Table doesn't exist in destination
            dest_df = pd.DataFrame()
        
        # Calculate row hashes
        if not source_df.empty:
            source_df['row_hash'] = source_df.apply(lambda x: hash(tuple(x.fillna(''))), axis=1)
        else:
            source_df['row_hash'] = pd.Series(dtype='int64')
        
        if not dest_df.empty:
            dest_df['row_hash'] = dest_df.apply(lambda x: hash(tuple(x.fillna(''))), axis=1)
        else:
            dest_df['row_hash'] = pd.Series(dtype='int64')
        
        # Find delta rows (new/updated) - only in the synced portion
        source_hashes = set(source_df['row_hash'].values) if not source_df.empty else set()
        dest_hashes = set(dest_df['row_hash'].values) if not dest_df.empty else set()
        
        delta_hashes = source_hashes - dest_hashes
        delta_rows = source_df[source_df['row_hash'].isin(delta_hashes)].drop('row_hash', axis=1) if not source_df.empty else pd.DataFrame()
        
        sql_engine.dispose()
        
        return {
            'delta_count': len(delta_rows),
            'delta_rows': delta_rows,
            'total_source_rows': len(source_df),  # Synced portion only
            'total_destination_rows': len(dest_df),
            'last_sync_time': last_sync_time,
            'last_check': datetime.now().isoformat(),
            'sync_status': sync_status['sync_status']
        }
        
    except Exception as e:
        logger.error(f"Error tracking delta for {server_name}.{db_name}.{table_name}: {e}")
        raise


def top_changed_tables(server_name, db_name):
    """
    Get ranked list of tables by number of rows added/updated in last sync.
    Only considers data that should have been synced.
    
    Args:
        server_name (str): Name of the SQL Server
        db_name (str): Database name
    
    Returns:
        list: Ranked list of tables with delta counts
    """
    try:
        config = load_config()
        server_conf = config["sqlservers"].get(server_name)
        if not server_conf:
            raise ValueError(f"Server {server_name} not found in configuration")
        
        # Get all tables in the database
        conn = get_sql_connection(server_conf, db_name)
        cursor = conn.cursor()
        tables = []
        
        for row in cursor.tables(tableType='TABLE'):
            schema_name = row.table_schem
            table_name = row.table_name
            if schema_name.lower() != 'sys':  # Skip system tables
                tables.append(f"{schema_name}.{table_name}")
        
        conn.close()
        
        # Calculate delta for each table (only synced portion)
        table_deltas = []
        for table in tables:
            try:
                delta_info = delta_tracking(server_name, db_name, table)
                table_deltas.append({
                    'table': table,
                    'delta_count': delta_info['delta_count'],
                    'total_source_rows': delta_info['total_source_rows'],
                    'total_destination_rows': delta_info['total_destination_rows'],
                    'last_sync_time': delta_info['last_sync_time'],
                    'sync_status': delta_info['sync_status']
                })
            except Exception as e:
                logger.warning(f"Could not calculate delta for {table}: {e}")
                table_deltas.append({
                    'table': table,
                    'delta_count': 0,
                    'total_source_rows': 0,
                    'total_destination_rows': 0,
                    'last_sync_time': None,
                    'sync_status': 'error'
                })
        
        # Sort by delta count (descending)
        table_deltas.sort(key=lambda x: x['delta_count'], reverse=True)
        
        return table_deltas
        
    except Exception as e:
        logger.error(f"Error getting top changed tables for {server_name}.{db_name}: {e}")
        raise


def get_table_sync_status(server_name, db_name, table_name):
    """
    Get sync status and last synced PK for a specific table.
    
    Args:
        server_name (str): Name of the SQL Server
        db_name (str): Database name
        table_name (str): Table name (format: schema.table)
    
    Returns:
        dict: Sync status information
    """
    try:
        config = load_config()
        server_conf = config["sqlservers"].get(server_name)
        if not server_conf:
            raise ValueError(f"Server {server_name} not found in configuration")
        
        # Parse schema and table from table_name
        if '.' in table_name:
            schema, table = table_name.split('.', 1)
        else:
            schema = 'dbo'
            table = table_name
        
        pg_engine = get_pg_engine()
        
        # Get sync status from tracking table
        query = """
        SELECT last_pk_value, updated_at
        FROM sync_table_status
        WHERE server_name = :server_name AND database_name = :database_name 
        AND schema_name = :schema AND table_name = :table
        """
        
        with pg_engine.connect() as conn:
            result = conn.execute(
                text(query),
                {
                    "server_name": server_conf['server'],
                    "database_name": db_name,
                    "schema": schema,
                    "table": table,
                },
            )
            row = result.fetchone()
            
            if row:
                return {
                    'last_pk_value': row[0],
                    'last_sync_time': row[1].isoformat() if row[1] else None,
                    'sync_status': 'synced'
                }
            else:
                return {
                    'last_pk_value': None,
                    'last_sync_time': None,
                    'sync_status': 'never_synced'
                }
        
    except Exception as e:
        logger.error(f"Error getting sync status for {server_name}.{db_name}.{table_name}: {e}")
        raise


def get_sync_health_summary(server_name, db_name):
    """
    Get overall sync health summary for a database.
    
    Args:
        server_name (str): Name of the SQL Server
        db_name (str): Database name
    
    Returns:
        dict: Sync health summary
    """
    try:
        table_deltas = top_changed_tables(server_name, db_name)
        
        synced_tables = [t for t in table_deltas if t['sync_status'] == 'synced']
        never_synced_tables = [t for t in table_deltas if t['sync_status'] == 'never_synced']
        error_tables = [t for t in table_deltas if t['sync_status'] == 'error']
        
        total_mismatches = sum(t['delta_count'] for t in synced_tables)
        
        return {
            'total_tables': len(table_deltas),
            'synced_tables': len(synced_tables),
            'never_synced_tables': len(never_synced_tables),
            'error_tables': len(error_tables),
            'total_mismatches': total_mismatches,
            'tables_with_mismatches': len([t for t in synced_tables if t['delta_count'] > 0]),
            'last_updated': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting sync health summary for {server_name}.{db_name}: {e}")
        raise