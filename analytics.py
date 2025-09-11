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
        
        # Get source data
        sql_engine = get_sqlalchemy_engine(server_conf, db_name)
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
        
        return {
            'rows_source': len(source_df),
            'rows_destination': len(dest_df),
            'missing_rows': missing_rows,
            'extra_rows': extra_rows,
            'source_data': source_df.drop('row_hash', axis=1) if not source_df.empty else pd.DataFrame(),
            'destination_data': dest_df.drop('row_hash', axis=1) if not dest_df.empty else pd.DataFrame()
        }
        
    except Exception as e:
        logger.error(f"Error comparing table rows for {server_name}.{db_name}.{table_name}: {e}")
        raise


def delta_tracking(server_name, db_name, table_name):
    """
    Track rows added/updated since last sync using row_hash comparison.
    
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
        
        # Get source data
        sql_engine = get_sqlalchemy_engine(server_conf, db_name)
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
        
        # Find delta rows (new/updated)
        source_hashes = set(source_df['row_hash'].values) if not source_df.empty else set()
        dest_hashes = set(dest_df['row_hash'].values) if not dest_df.empty else set()
        
        delta_hashes = source_hashes - dest_hashes
        delta_rows = source_df[source_df['row_hash'].isin(delta_hashes)].drop('row_hash', axis=1) if not source_df.empty else pd.DataFrame()
        
        sql_engine.dispose()
        
        return {
            'delta_count': len(delta_rows),
            'delta_rows': delta_rows,
            'total_source_rows': len(source_df),
            'total_destination_rows': len(dest_df),
            'last_check': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error tracking delta for {server_name}.{db_name}.{table_name}: {e}")
        raise


def top_changed_tables(server_name, db_name):
    """
    Get ranked list of tables by number of rows added/updated in last sync.
    
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
        
        # Calculate delta for each table
        table_deltas = []
        for table in tables:
            try:
                delta_info = delta_tracking(server_name, db_name, table)
                table_deltas.append({
                    'table': table,
                    'delta_count': delta_info['delta_count'],
                    'total_source_rows': delta_info['total_source_rows'],
                    'total_destination_rows': delta_info['total_destination_rows']
                })
            except Exception as e:
                logger.warning(f"Could not calculate delta for {table}: {e}")
                table_deltas.append({
                    'table': table,
                    'delta_count': 0,
                    'total_source_rows': 0,
                    'total_destination_rows': 0
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
