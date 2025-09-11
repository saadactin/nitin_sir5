"""
Metrics module for SQL Server → Postgres hybrid sync.
Provides monitoring and metrics functionality.
"""

import pandas as pd
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from hybrid_sync import get_sqlalchemy_engine, get_pg_engine, get_sql_connection, get_table_row_count
from manage_server import load_config

logger = logging.getLogger(__name__)


def get_server_metrics(server_name):
    """
    Get all table metrics for a server.
    
    Args:
        server_name (str): Name of the SQL Server
    
    Returns:
        dict: Server metrics with table-level details
    """
    try:
        config = load_config()
        server_conf = config["sqlservers"].get(server_name)
        if not server_conf:
            raise ValueError(f"Server {server_name} not found in configuration")
        
        # Get all databases for this server
        master_conn = get_sql_connection(server_conf)
        databases = []
        
        query = """
        SELECT name 
        FROM sys.databases 
        WHERE state = 0  
        AND name NOT IN ('master', 'tempdb', 'model', 'msdb', 'distribution', 'ReportServer', 'ReportServerTempDB')
        ORDER BY name
        """
        
        cursor = master_conn.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            databases.append(row[0])
        
        master_conn.close()
        
        # Get metrics for each database
        server_metrics = {
            'server_name': server_name,
            'server_host': server_conf['server'],
            'total_databases': len(databases),
            'databases': {},
            'total_tables': 0,
            'total_source_rows': 0,
            'total_destination_rows': 0,
            'last_updated': datetime.now().isoformat()
        }
        
        pg_engine = get_pg_engine()
        server_clean = ''.join(c for c in server_conf['server'] if c.isalnum() or c in '_-')
        
        for db_name in databases:
            try:
                db_metrics = get_database_metrics(server_name, db_name)
                server_metrics['databases'][db_name] = db_metrics
                server_metrics['total_tables'] += db_metrics['total_tables']
                server_metrics['total_source_rows'] += db_metrics['total_source_rows']
                server_metrics['total_destination_rows'] += db_metrics['total_destination_rows']
            except Exception as e:
                logger.warning(f"Could not get metrics for database {db_name}: {e}")
                server_metrics['databases'][db_name] = {
                    'error': str(e),
                    'total_tables': 0,
                    'total_source_rows': 0,
                    'total_destination_rows': 0
                }
        
        return server_metrics
        
    except Exception as e:
        logger.error(f"Error getting server metrics for {server_name}: {e}")
        raise


def get_database_metrics(server_name, db_name):
    """
    Get metrics for tables in a single database.
    
    Args:
        server_name (str): Name of the SQL Server
        db_name (str): Database name
    
    Returns:
        dict: Database metrics with table-level details
    """
    try:
        config = load_config()
        server_conf = config["sqlservers"].get(server_name)
        if not server_conf:
            raise ValueError(f"Server {server_name} not found in configuration")
        
        # Get all tables in the database
        db_conn = get_sql_connection(server_conf, db_name)
        cursor = db_conn.cursor()
        tables = []
        
        for row in cursor.tables(tableType='TABLE'):
            schema_name = row.table_schem
            table_name = row.table_name
            if schema_name.lower() != 'sys':  # Skip system tables
                tables.append((schema_name, table_name))
        
        db_conn.close()
        
        # Get metrics for each table
        db_metrics = {
            'database_name': db_name,
            'server_name': server_name,
            'total_tables': len(tables),
            'tables': {},
            'total_source_rows': 0,
            'total_destination_rows': 0,
            'last_updated': datetime.now().isoformat()
        }
        
        pg_engine = get_pg_engine()
        server_clean = ''.join(c for c in server_conf['server'] if c.isalnum() or c in '_-')
        schema_name = f"{server_clean}_{db_name}".replace('-', '_').replace(' ', '_')
        
        for schema, table in tables:
            try:
                table_metrics = get_table_metrics(server_name, db_name, f"{schema}.{table}")
                db_metrics['tables'][f"{schema}.{table}"] = table_metrics
                db_metrics['total_source_rows'] += table_metrics['rows_source']
                db_metrics['total_destination_rows'] += table_metrics['rows_destination']
            except Exception as e:
                logger.warning(f"Could not get metrics for table {schema}.{table}: {e}")
                db_metrics['tables'][f"{schema}.{table}"] = {
                    'error': str(e),
                    'rows_source': 0,
                    'rows_destination': 0,
                    'last_pk': None,
                    'delta_count': 0
                }
        
        return db_metrics
        
    except Exception as e:
        logger.error(f"Error getting database metrics for {server_name}.{db_name}: {e}")
        raise


def get_table_metrics(server_name, db_name, table_name):
    """
    Get detailed metrics for a specific table.
    
    Args:
        server_name (str): Name of the SQL Server
        db_name (str): Database name
        table_name (str): Table name (format: schema.table)
    
    Returns:
        dict: Table metrics including row counts, last PK, delta count
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
        
        # Get source row count
        db_conn = get_sql_connection(server_conf, db_name)
        rows_source = get_table_row_count(db_conn, schema, table)
        db_conn.close()
        
        # Get destination row count
        pg_engine = get_pg_engine()
        server_clean = ''.join(c for c in server_conf['server'] if c.isalnum() or c in '_-')
        pg_schema_name = f"{server_clean}_{db_name}".replace('-', '_').replace(' ', '_')
        pg_table_name = f"{schema}_{table}"
        
        try:
            dest_query = f'SELECT COUNT(*) FROM "{pg_schema_name}"."{pg_table_name}"'
            with pg_engine.connect() as conn:
                result = conn.execute(text(dest_query))
                rows_destination = result.fetchone()[0]
        except Exception:
            rows_destination = 0
        
        # Get last synced PK and sync status
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
            
            last_pk = row[0] if row else None
            last_sync_time = row[1].isoformat() if row and row[1] else None
        
        # Calculate delta count (simplified - could be enhanced with actual delta tracking)
        delta_count = max(0, rows_source - rows_destination)
        
        return {
            'table_name': table_name,
            'schema': schema,
            'table': table,
            'rows_source': rows_source,
            'rows_destination': rows_destination,
            'last_pk': last_pk,
            'last_sync_time': last_sync_time,
            'delta_count': delta_count,
            'sync_status': 'synced' if last_sync_time else 'never_synced',
            'row_difference': rows_source - rows_destination
        }
        
    except Exception as e:
        logger.error(f"Error getting table metrics for {server_name}.{db_name}.{table_name}: {e}")
        raise


def get_sync_summary():
    """
    Get overall sync summary across all servers.
    
    Returns:
        dict: Overall sync summary
    """
    try:
        config = load_config()
        sqlservers = config.get("sqlservers", {})
        
        summary = {
            'total_servers': len(sqlservers),
            'servers': {},
            'total_databases': 0,
            'total_tables': 0,
            'total_source_rows': 0,
            'total_destination_rows': 0,
            'last_updated': datetime.now().isoformat()
        }
        
        for server_name in sqlservers.keys():
            try:
                server_metrics = get_server_metrics(server_name)
                summary['servers'][server_name] = server_metrics
                summary['total_databases'] += server_metrics['total_databases']
                summary['total_tables'] += server_metrics['total_tables']
                summary['total_source_rows'] += server_metrics['total_source_rows']
                summary['total_destination_rows'] += server_metrics['total_destination_rows']
            except Exception as e:
                logger.warning(f"Could not get summary for server {server_name}: {e}")
                summary['servers'][server_name] = {'error': str(e)}
        
        return summary
        
    except Exception as e:
        logger.error(f"Error getting sync summary: {e}")
        raise


def get_table_sync_history(server_name, db_name, table_name, limit=10):
    """
    Get sync history for a specific table.
    
    Args:
        server_name (str): Name of the SQL Server
        db_name (str): Database name
        table_name (str): Table name (format: schema.table)
        limit (int): Number of history records to return
    
    Returns:
        list: Sync history records
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
        
        # Get sync history from tracking table
        query = """
        SELECT last_pk_value, updated_at, created_at
        FROM sync_table_status
        WHERE server_name = :server_name AND database_name = :database_name 
        AND schema_name = :schema AND table_name = :table
        ORDER BY updated_at DESC
        LIMIT :limit
        """
        
        with pg_engine.connect() as conn:
            result = conn.execute(
                text(query),
                {
                    "server_name": server_conf['server'],
                    "database_name": db_name,
                    "schema": schema,
                    "table": table,
                    "limit": limit
                },
            )
            
            history = []
            for row in result.fetchall():
                history.append({
                    'last_pk_value': row[0],
                    'updated_at': row[1].isoformat() if row[1] else None,
                    'created_at': row[2].isoformat() if row[2] else None
                })
            
            return history
        
    except Exception as e:
        logger.error(f"Error getting sync history for {server_name}.{db_name}.{table_name}: {e}")
        raise
