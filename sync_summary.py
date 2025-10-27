import pyodbc
import yaml
import os
import time
import logging
import psycopg2
from flask import Blueprint, render_template, jsonify, flash, redirect, url_for
from db_utils import get_pg_connection
from table_filters import is_excluded_schema

# Simple in-memory TTL cache to avoid repeated expensive DB scans
LOG = logging.getLogger(__name__)
CACHE_TTL = int(os.environ.get('SYNC_SUMMARY_CACHE_TTL', '60'))  # seconds
_cache = {}

def _cache_get(key):
    entry = _cache.get(key)
    if not entry:
        return None
    ts, value = entry
    if time.time() - ts > CACHE_TTL:
        LOG.info(f"Cache expired for {key}")
        del _cache[key]
        return None
    LOG.info(f"Cache hit for {key}")
    return value

def _cache_set(key, value):
    _cache[key] = (time.time(), value)

def load_config():
    """Load database configuration from YAML file"""
    CONFIG_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "config/db_connections.yaml"))
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

# Create blueprint with prefix
bp = Blueprint('sync_summary', __name__, url_prefix='/sync-summary')

@bp.route('/')
def index():
    """Show sync summary page"""
    return render_template('sync_summary.html')

@bp.route('/quick.json')
def quick_summary():
    """Get quick summary data for all servers"""
    # Use caching for quick summary data
    cache_key = "quick_summary"
    cached_data = _cache_get(cache_key)
    if cached_data:
        return jsonify(cached_data)

    all_data = get_all_server_comparisons()
    if all_data:
        _cache_set(cache_key, all_data)
    return jsonify(all_data)

@bp.route('/<server_name>/tables')
def server_tables(server_name):
    """Show table comparison page for a specific server"""
    config = load_config()
    sqlservers = config.get("sqlservers", {})
    
    if server_name not in sqlservers:
        flash(f"Server '{server_name}' not found", "error")
        return redirect(url_for('sync_summary.index'))
    
    server_config = sqlservers[server_name]
    target_db = server_config.get('target_postgres_db')
    if not target_db:
        flash(f"No target database configured for server '{server_name}'", "error")
        return redirect(url_for('sync_summary.index'))
    
    return render_template(
        'server_tables.html',
        server_name=server_name,
        target_db=target_db
    )

@bp.route('/<server_name>/tables/data.json')
def server_tables_data(server_name):
    """Get table comparison data for a specific server"""
    # Use caching for table comparison data
    cache_key = f"tables:{server_name}"
    cached_data = _cache_get(cache_key)
    if cached_data:
        return jsonify(cached_data)
    
    comparison = get_table_comparison(server_name)
    if comparison:
        _cache_set(cache_key, comparison)
    return jsonify(comparison)



def build_sql_connection_string(server_config, database=None):
    """Build SQL Server connection string supporting both SQL Auth and Windows Auth"""
    server = server_config['server']
    port = server_config.get('port')
    if port:
        server = f"{server},{port}"
    
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};"
    
    # Check if Windows Authentication should be used
    username = server_config.get('username', '')
    password = server_config.get('password', '')
    
    if username.lower() in ['windows', 'trusted', ''] or password.lower() in ['windows', 'trusted', '']:
        # Use Windows Authentication
        conn_str += "Trusted_Connection=yes;"
    else:
        # Use SQL Server Authentication
        conn_str += f"UID={username};PWD={password};"
    
    if database:
        conn_str += f"DATABASE={database};"
    
    return conn_str

def normalize_table_name(table_key):
    """
    Normalize table names for comparison by removing server prefixes and standardizing format.
    
    Examples:
    - 'BottleDB1.dbo.Alarms' -> 'BottleDB1.dbo.Alarms'
    - 'test1.localhost_BottleDB1.dbo_Alarms' -> 'BottleDB1.dbo.Alarms'
    - 'test1.localhost_Desserts.dbo.Students1' -> 'Desserts.dbo.Students1'
    """
    # Split by dots to analyze components
    parts = table_key.split('.')
    
    if len(parts) >= 3:
        # Handle PostgreSQL format: test1.localhost_DatabaseName.schema.table
        if parts[0].startswith('test') and 'localhost_' in parts[1]:
            # Extract database name from the localhost_ prefix
            db_part = parts[1].replace('localhost_', '')
            # Reconstruct as database.schema.table
            normalized = f"{db_part}.{'.'.join(parts[2:])}"
            return normalized
        else:
            # Already in standard format: database.schema.table
            return table_key
    else:
        # Fallback for unexpected formats
        return table_key

def get_table_comparison(server_name):
    """Get table-by-table comparison for a specific server"""
    LOG.info(f"Starting table comparison for server: {server_name}")
    config = load_config()
    sqlservers = config.get("sqlservers", {})

    if server_name not in sqlservers:
        LOG.error(f"Server '{server_name}' not found in config")
        return {"error": f"Server '{server_name}' not found"}

    server_config = sqlservers[server_name]
    target_db = server_config.get('target_postgres_db')

    if not target_db:
        LOG.error(f"No target database configured for server '{server_name}'")
        return {"error": "No target database configured"}

    try:
        # Get row counts for each user database and table
        sql_counts = {}
        conn_str = build_sql_connection_string(server_config, "master")
        master_conn = pyodbc.connect(conn_str)
        master_cur = master_conn.cursor()
        master_cur.execute("""
            SELECT name FROM sys.databases 
            WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
        """)
        databases = [row[0] for row in master_cur.fetchall()]
        for db_name in databases:
            if db_name in server_config.get('skip_databases', []):
                continue
            try:
                db_conn_str = build_sql_connection_string(server_config, db_name)
                db_conn = pyodbc.connect(db_conn_str)
                db_cur = db_conn.cursor()
                db_cur.execute("""
                    SELECT s.name as schema_name, t.name as table_name
                    FROM sys.tables t
                    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                    WHERE t.is_ms_shipped = 0
                """)
                tables = [(row[0], row[1]) for row in db_cur.fetchall()]
                for schema, table in tables:
                    if not is_excluded_schema(schema) and table not in ["IS2B_BatchRunningDatatest", "Products3"]:
                        try:
                            db_cur.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
                            count = db_cur.fetchone()[0]
                            key = f"{db_name}.{schema}.{table}"
                            sql_counts[key] = count
                        except Exception as e:
                            LOG.exception(f"Error counting rows in SQL Server table {db_name}.{schema}.{table}: {e}")
                            continue
                db_cur.close()
                db_conn.close()
            except Exception as e:
                LOG.exception(f"Error processing database {db_name}: {e}")
                continue
        master_cur.close()
        master_conn.close()
        LOG.info(f"Found {len(sql_counts)} SQL Server tables with accurate row counts")

        # Connect to PostgreSQL
        pg_conn = get_pg_connection()
        pg_cursor = pg_conn.cursor()
        LOG.info("Starting PostgreSQL table count query")
        pg_cursor.execute("""
            SELECT current_database(), schemaname, relname, n_live_tup
            FROM pg_stat_user_tables
        """)
        pg_counts = {}
        for db_name, schema, table, count in pg_cursor:
            if not is_excluded_schema(schema) and table not in ["IS2B_BatchRunningDatatest", "Products3"]:
                key = f"{db_name}.{schema}.{table}"
                pg_counts[key] = count

        # Create normalized comparison maps
        sql_normalized = {}
        pg_normalized = {}
        
        # Normalize SQL Server table names
        for original_key, count in sql_counts.items():
            normalized_key = normalize_table_name(original_key)
            sql_normalized[normalized_key] = count
            LOG.debug(f"SQL: {original_key} -> {normalized_key} = {count}")
        
        # Normalize PostgreSQL table names
        for original_key, count in pg_counts.items():
            normalized_key = normalize_table_name(original_key)
            pg_normalized[normalized_key] = count
            LOG.debug(f"PG: {original_key} -> {normalized_key} = {count}")

        # Compare tables using normalized names
        all_tables = set(sql_normalized.keys()) | set(pg_normalized.keys())
        comparison = []

        for normalized_table in sorted(all_tables):
            sql_count = sql_normalized.get(normalized_table, 0)
            pg_count = pg_normalized.get(normalized_table, 0)
            difference = pg_count - sql_count

            # Update status logic to reflect accurate comparison
            status = "Complete" if difference == 0 else ("Extra in PostgreSQL" if difference > 0 else "Missing in PostgreSQL")

            comparison.append({
                'table_name': normalized_table,
                'sql_rows': sql_count,
                'pg_rows': pg_count,
                'difference': difference,
                'status': status
            })

        return {
            'server_name': server_name,
            'target_db': target_db,
            'tables': comparison,
            'total_tables': len(comparison)
        }

    except Exception as e:
        LOG.error(f"Error during table comparison: {e}")
        return {"error": str(e)}

def get_individual_server_comparison(server_name):
    """Get detailed comparison for a specific SQL Server"""
    try:
        cache_key = f"individual:{server_name}"
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached
        config = load_config()
        sqlservers = config.get("sqlservers", {})
        
        if server_name not in sqlservers:
            return {"error": f"Server '{server_name}' not found"}
        
        server_config = sqlservers[server_name]
        
        # Get SQL Server data for this specific server
        sql_data = get_single_sqlserver_rows(server_name, server_config)
        
        # Get PostgreSQL data for the target database
        target_db = server_config.get('target_postgres_db')
        pg_data = get_postgres_total_rows_for_db(target_db) if target_db else {"total_rows": 0, "error": "No target database configured"}
        
        sql_total = sql_data['total_rows']
        pg_total = pg_data['total_rows']
        
        # Calculate comparison metrics
        difference = pg_total - sql_total
        sync_percentage = (pg_total / sql_total * 100) if sql_total > 0 else 0
        
        result = {
            'server_name': server_name,
            'sql_server': sql_data,
            'postgresql': pg_data,
            'comparison': {
                'sql_total_rows': sql_total,
                'postgres_total_rows': pg_total,
                'difference': difference,
                'sync_percentage': round(sync_percentage, 2),
                'status': 'Complete' if difference >= 0 else 'Incomplete'
            }
        }
        _cache_set(cache_key, result)
        return result
    except Exception as e:
        return {"error": f"Error getting comparison for {server_name}: {str(e)}"}

def get_single_sqlserver_rows(server_name, server_config):
    """Get row count from a single SQL Server"""
    try:
        # Connect to SQL Server using the helper function
        conn_str = build_sql_connection_string(server_config, "master")
        conn = pyodbc.connect(conn_str)
        cur = conn.cursor()
        
        # Get all databases except system databases
        cur.execute("""
            SELECT name FROM sys.databases 
            WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
        """)
        databases = [row[0] for row in cur.fetchall()]
        
        server_row_count = 0
        database_details = []
        
        for db_name in databases:
            if db_name in server_config.get('skip_databases', []):
                continue
                
            try:
                # Connect to specific database using helper function
                db_conn_str = build_sql_connection_string(server_config, db_name)
                db_conn = pyodbc.connect(db_conn_str)
                db_cur = db_conn.cursor()
                
                # Get all tables in this database
                db_cur.execute("""
                    SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_TYPE = 'BASE TABLE'
                """)
                tables = [row[0] for row in db_cur.fetchall()]
                
                db_total_rows = 0
                table_details = []
                
                for table_name in tables:
                    try:
                        db_cur.execute(f"SELECT COUNT(*) FROM [{table_name}]")
                        count = db_cur.fetchone()[0]
                        db_total_rows += count
                        table_details.append({
                            'table_name': table_name,
                            'row_count': count
                        })
                    except Exception as e:
                        LOG.exception(f"Error counting rows in SQL Server table {db_name}.{table_name}: {e}")
                        continue
                
                server_row_count += db_total_rows
                database_details.append({
                    'database_name': db_name,
                    'total_rows': db_total_rows,
                    'table_count': len(table_details),
                    'tables': table_details
                })
                
                db_cur.close()
                db_conn.close()
                
            except Exception as e:
                LOG.exception(f"Error processing database {db_name}: {e}")
                continue
        
        cur.close()
        conn.close()
        
        return {
            'server_name': server_name,
            'host': server_config['server'],
            'port': server_config.get('port', 1433),
            'total_rows': server_row_count,
            'database_count': len(database_details),
            'databases': database_details,
            'target_postgres_db': server_config.get('target_postgres_db')
        }
        
    except Exception as e:
        LOG.exception(f"Error getting SQL Server total rows for {server_name}: {e}")
        return {
            'server_name': server_name,
            'host': server_config.get('server', 'Unknown'),
            'port': server_config.get('port', 1433),
            'total_rows': 0,
            'database_count': 0,
            'databases': [],
            'target_postgres_db': server_config.get('target_postgres_db'),
            'error': str(e)
        }

def get_postgres_total_rows_for_db(target_db):
    """Get PostgreSQL row count for a specific target database"""
    if not target_db:
        return {
            'total_rows': 0,
            'database': 'No target database specified',
            'schema_count': 0,
            'schemas': {},
            'tables': [],
            'error': 'No target database specified'
        }
    
    try:
        cache_key = f"pgdb:{target_db}"
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached
        config = load_config()
        pg_config = config.get('postgresql', {})
        
        # Create connection to target database
        conn = psycopg2.connect(
            host=pg_config.get('host', 'localhost'),
            port=pg_config.get('port', 5432),
            database=target_db,
            user=pg_config.get('username', 'postgres'),
            password=pg_config.get('password', '')
        )
        cur = conn.cursor()
        
        # Get all tables from ALL schemas (excluding system schemas). We'll
        # filter out 'public' and metric-sync schemas using is_excluded_schema.
        cur.execute("""
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_type = 'BASE TABLE' 
            AND table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        """)
        all_tables = [r for r in cur.fetchall() if not is_excluded_schema(r[0])]
        
        total_rows = 0
        schema_details = {}
        table_details = []
        
        for schema_name, table_name in all_tables:
            try:
                cur.execute(f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"')
                count = cur.fetchone()[0]
                total_rows += count
                
                # Track tables by schema
                if schema_name not in schema_details:
                    schema_details[schema_name] = {'tables': 0, 'rows': 0}
                schema_details[schema_name]['tables'] += 1
                schema_details[schema_name]['rows'] += count
                
                table_details.append({
                    'schema_name': schema_name,
                    'table_name': table_name,
                    'row_count': count
                })
            except Exception as e:
                LOG.exception(f"Error counting rows in PostgreSQL table {schema_name}.{table_name}: {e}")
                table_details.append({
                    'schema_name': schema_name,
                    'table_name': table_name,
                    'row_count': 0,
                    'error': str(e)
                })
                continue
        
        cur.close()
        conn.close()
        
        result = {
            'total_rows': total_rows,
            'database': target_db,
            'schema_count': len(schema_details),
            'schemas': schema_details,
            'tables': table_details
        }
        _cache_set(cache_key, result)
        return result
        
    except Exception as e:
        LOG.exception(f"Error getting PostgreSQL total rows for database {target_db}: {e}")
        return {
            'total_rows': 0,
            'database': target_db or 'Unknown',
            'schema_count': 0,
            'schemas': {},
            'tables': [],
            'error': str(e)
        }

def get_all_server_comparisons():
    """Get comparison data for all SQL Servers individually"""
    try:
        cache_key = 'all_servers'
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached

        config = load_config()
        sqlservers = config.get("sqlservers", {})
        
        server_comparisons = []
        
        for server_name, server_config in sqlservers.items():
            comparison = get_individual_server_comparison(server_name)
            server_comparisons.append(comparison)
        
        result = {
            'servers': server_comparisons,
            'total_servers': len(server_comparisons)
        }
        _cache_set(cache_key, result)
        return result
    except Exception as e:
        LOG.exception(f"Error getting all server comparisons: {e}")
        return {
            'servers': [],
            'total_servers': 0,
            'error': str(e)
        }

def get_postgres_total_rows():
    """Get total row count from target PostgreSQL database specified in SQL Server config"""
    try:
        config = load_config()
        sqlservers = config.get("sqlservers", {})
        
        # Get the target PostgreSQL database from the first SQL server configuration
        target_db = None
        for server_name, server_config in sqlservers.items():
            target_db = server_config.get('target_postgres_db')
            if target_db:
                break
        
        if not target_db:
            return {
                'total_rows': 0,
                'database': 'No target database configured',
                'schema_count': 0,
                'schemas': {},
                'tables': [],
                'error': 'No target_postgres_db found in SQL Server configuration'
            }
        
        # Use the same connection but connect to the target database
        conn = get_pg_connection()
        cur = conn.cursor()
        
        # Switch to the target database
        cur.execute(f"SELECT current_database()")
        current_db = cur.fetchone()[0]
        
        if current_db != target_db:
            # Close current connection and create new one for target database
            cur.close()
            conn.close()
            
            # Get PostgreSQL config for connection details
            pg_config = config.get('postgresql', {})
            
            # Create new connection to target database
            conn = psycopg2.connect(
                host=pg_config.get('host', 'localhost'),
                port=pg_config.get('port', 5432),
                database=target_db,
                user=pg_config.get('username', 'postgres'),
                password=pg_config.get('password', '')
            )
            cur = conn.cursor()
        
        # Get all tables from ALL schemas (excluding system schemas). We'll
        # filter out 'public' and metric-sync schemas using is_excluded_schema.
        cur.execute("""
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_type = 'BASE TABLE' 
            AND table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        """)
        all_tables = [r for r in cur.fetchall() if not is_excluded_schema(r[0])]
        
        total_rows = 0
        schema_details = {}
        table_details = []
        
        for schema_name, table_name in all_tables:
            try:
                cur.execute(f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"')
                count = cur.fetchone()[0]
                total_rows += count
                
                # Track tables by schema
                if schema_name not in schema_details:
                    schema_details[schema_name] = {'tables': 0, 'rows': 0}
                schema_details[schema_name]['tables'] += 1
                schema_details[schema_name]['rows'] += count
                
                table_details.append({
                    'schema_name': schema_name,
                    'table_name': table_name,
                    'row_count': count
                })
            except Exception as e:
                LOG.exception(f"Error counting rows in PostgreSQL table {schema_name}.{table_name}: {e}")
                table_details.append({
                    'schema_name': schema_name,
                    'table_name': table_name,
                    'row_count': 0,
                    'error': str(e)
                })
                continue
        
        cur.close()
        conn.close()
        
        return {
            'total_rows': total_rows,
            'database': target_db,
            'schema_count': len(schema_details),
            'schemas': schema_details,
            'tables': table_details
        }
        
    except Exception as e:
        LOG.exception(f"Error getting PostgreSQL total rows: {e}")
        return {
            'total_rows': 0,
            'database': 'Unknown',
            'schema_count': 0,
            'schemas': {},
            'tables': [],
            'error': str(e)
        }

def get_sqlserver_total_rows():
    """Get total row count from all SQL Server databases (legacy function for backward compatibility)"""
    try:
        config = load_config()
        sqlservers = config.get("sqlservers", {})
        
        total_rows = 0
        server_details = []
        
        for server_name, server_config in sqlservers.items():
            server_data = get_single_sqlserver_rows(server_name, server_config)
            total_rows += server_data['total_rows']
            server_details.append(server_data)
        
        # Return data for the first server's target database for backward compatibility
        target_postgres_db = None
        if server_details:
            target_postgres_db = server_details[0].get('target_postgres_db')
        
        return {
            'total_rows': total_rows,
            'servers': server_details,
            'target_postgres_db': target_postgres_db
        }
        
    except Exception as e:
        LOG.exception(f"Error getting SQL Server total rows: {e}")
        return {
            'total_rows': 0,
            'servers': [],
            'target_postgres_db': None,
            'error': str(e)
        }

def get_postgres_total_rows():
    """Get PostgreSQL row count (legacy function - uses first server's target DB for backward compatibility)"""
    try:
        config = load_config()
        sqlservers = config.get("sqlservers", {})
        
        # Get the first server's target database for backward compatibility
        target_db = None
        for server_name, server_config in sqlservers.items():
            target_db = server_config.get('target_postgres_db')
            if target_db:
                break
        
        return get_postgres_total_rows_for_db(target_db)
        
    except Exception as e:
        LOG.exception(f"Error getting PostgreSQL total rows: {e}")
        return {
            'total_rows': 0,
            'database': 'Unknown',
            'schema_count': 0,
            'schemas': {},
            'tables': [],
            'error': str(e)
        }

def get_sync_comparison():
    """Get comparison between SQL Server and PostgreSQL row counts (legacy function)"""
    sql_data = get_sqlserver_total_rows()
    pg_data = get_postgres_total_rows()
    
    sql_total = sql_data['total_rows']
    pg_total = pg_data['total_rows']
    
    # Calculate difference and percentage
    difference = pg_total - sql_total
    sync_percentage = (pg_total / sql_total * 100) if sql_total > 0 else 0
    
    return {
        'sql_server': sql_data,
        'postgresql': pg_data,
        'comparison': {
            'sql_total_rows': sql_total,
            'postgres_total_rows': pg_total,
            'difference': difference,
            'sync_percentage': round(sync_percentage, 2),
            'status': 'Complete' if difference >= 0 else 'Incomplete'
        }
    }

def _determine_sync_status(sql_rows, pg_rows, pg_exists):
    """Determine sync status based on row counts"""
    if not pg_exists:
        return 'Incomplete'
    elif pg_rows >= sql_rows:
        return 'Synced'
    else:
        return 'Incomplete'

def get_detailed_table_comparison(server_name):
    """Get detailed table-by-table comparison between SQL Server and PostgreSQL"""
    try:
        config = load_config()
        sqlservers = config.get("sqlservers", {})
        
        if server_name not in sqlservers:
            return {"error": f"Server '{server_name}' not found"}
        
        server_config = sqlservers[server_name]
        target_db = server_config.get('target_postgres_db')
        
        if not target_db:
            return {"error": "No target PostgreSQL database configured"}
        
        # Get SQL Server table details
        sql_tables = get_sqlserver_table_details(server_name, server_config)
        
        # Get PostgreSQL table details
        pg_tables = get_postgres_table_details(target_db)
        
        # Create side-by-side comparison
        table_comparison = []
        
        # Create a map of PostgreSQL tables for quick lookup
        pg_table_map = {}
        for pg_table in pg_tables:
            # Use schema.table as key for PostgreSQL
            key = f"{pg_table['schema_name']}.{pg_table['table_name']}"
            pg_table_map[key] = pg_table
        
        # Also create a map by reconstructed SQL Server format for better matching
        pg_table_by_sql_format = {}
        for pg_table in pg_tables:
            # Extract SQL Server database name and table name from PostgreSQL naming
            # PostgreSQL schema format: {server}_{database} (e.g., localhost_SampleDB1)
            # PostgreSQL table format: {schema}_{table} (e.g., dbo_Customers)
            schema_parts = pg_table['schema_name'].split('_', 1)  # Split on first underscore
            table_parts = pg_table['table_name'].split('_', 1)   # Split on first underscore
            
            if len(schema_parts) >= 2 and len(table_parts) >= 2:
                sql_db = schema_parts[1]  # e.g., "SampleDB1" from "localhost_SampleDB1"
                sql_table = table_parts[1]  # e.g., "Customers" from "dbo_Customers"
                sql_key = f"{sql_db}.{sql_table}"
                pg_table_by_sql_format[sql_key] = pg_table
        
        # Process SQL Server tables
        for sql_table in sql_tables:
            # Try multiple matching strategies:
            sql_key = f"{sql_table['database_name']}.{sql_table['table_name']}"
            
            # 1. Try reconstructed SQL Server format matching
            pg_match = pg_table_by_sql_format.get(sql_key)
            
            # 2. If not found, try direct schema.table format
            if not pg_match:
                pg_match = pg_table_map.get(sql_key)
            
            # 3. If still not found, try just table name matching (case-insensitive)
            if not pg_match:
                for pg_table in pg_tables:
                    # Check if the table name part matches (after removing schema prefix)
                    pg_table_name_part = pg_table['table_name'].split('_', 1)[-1] if '_' in pg_table['table_name'] else pg_table['table_name']
                    if pg_table_name_part.lower() == sql_table['table_name'].lower():
                        pg_match = pg_table
                        break
            
            comparison_row = {
                'sql_server': {
                    'database': sql_table['database_name'],
                    'table': sql_table['table_name'],
                    'rows': sql_table['row_count'],
                    'exists': True
                },
                'postgresql': {
                    'schema': pg_match['schema_name'] if pg_match else 'N/A',
                    'table': pg_match['table_name'] if pg_match else 'Missing',
                    'rows': pg_match['row_count'] if pg_match else 0,
                    'exists': pg_match is not None
                },
                'difference': (pg_match['row_count'] if pg_match else 0) - sql_table['row_count'],
                'sync_status': _determine_sync_status(sql_table['row_count'], pg_match['row_count'] if pg_match else 0, pg_match is not None)
            }
            table_comparison.append(comparison_row)
            
            # Remove matched PostgreSQL table from map (find the correct key to remove)
            if pg_match:
                key_to_remove = None
                for pg_key, pg_table in pg_table_map.items():
                    if pg_table['schema_name'] == pg_match['schema_name'] and pg_table['table_name'] == pg_match['table_name']:
                        key_to_remove = pg_key
                        break
                if key_to_remove:
                    del pg_table_map[key_to_remove]
        
        # Add remaining PostgreSQL tables (not in SQL Server)
        for pg_key, pg_table in pg_table_map.items():
            comparison_row = {
                'sql_server': {
                    'database': 'N/A',
                    'table': 'Not Found',
                    'rows': 0,
                    'exists': False
                },
                'postgresql': {
                    'schema': pg_table['schema_name'],
                    'table': pg_table['table_name'],
                    'rows': pg_table['row_count'],
                    'exists': True
                },
                'difference': pg_table['row_count'],
                'sync_status': 'Extra Data'
            }
            table_comparison.append(comparison_row)
        
        return {
            'server_name': server_name,
            'target_database': target_db,
            'sql_server_tables': len(sql_tables),
            'postgresql_tables': len(pg_tables),
            'table_comparison': sorted(table_comparison, key=lambda x: x['sql_server']['table']),
            'summary': {
                'total_comparisons': len(table_comparison),
                'synced_tables': len([t for t in table_comparison if t['sync_status'] == 'Synced']),
                'incomplete_tables': len([t for t in table_comparison if t['sync_status'] == 'Incomplete']),
                'extra_pg_tables': len([t for t in table_comparison if t['sync_status'] == 'Extra Data'])
            }
        }
        
    except Exception as e:
        return {"error": f"Error getting detailed comparison for {server_name}: {str(e)}"}

def get_sqlserver_table_details(server_name, server_config):
    """Get detailed table information from SQL Server"""
    tables = []
    try:
        conn_str = build_sql_connection_string(server_config, "master")
        conn = pyodbc.connect(conn_str)
        cur = conn.cursor()
        
        # Get all databases except system databases
        cur.execute("""
            SELECT name FROM sys.databases 
            WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
        """)
        databases = [row[0] for row in cur.fetchall()]
        
        for db_name in databases:
            if db_name in server_config.get('skip_databases', []):
                continue
                
            try:
                db_conn_str = build_sql_connection_string(server_config, db_name)
                db_conn = pyodbc.connect(db_conn_str)
                db_cur = db_conn.cursor()
                
                # Get all tables in this database with row counts
                db_cur.execute("""
                    SELECT t.TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES t
                    WHERE t.TABLE_TYPE = 'BASE TABLE'
                    ORDER BY t.TABLE_NAME
                """)
                table_names = [row[0] for row in db_cur.fetchall()]
                
                for table_name in table_names:
                    try:
                        db_cur.execute(f"SELECT COUNT(*) FROM [{table_name}]")
                        count = db_cur.fetchone()[0]
                        tables.append({
                            'database_name': db_name,
                            'table_name': table_name,
                            'row_count': count
                        })
                    except Exception as e:
                        LOG.exception(f"Error counting rows in {db_name}.{table_name}: {e}")
                        continue
                
                db_cur.close()
                db_conn.close()
                
            except Exception as e:
                LOG.exception(f"Error processing database {db_name}: {e}")
                continue
        
        cur.close()
        conn.close()
        
    except Exception as e:
        LOG.exception(f"Error getting SQL Server table details for {server_name}: {e}")
    
    return tables

def get_postgres_table_details(target_db):
    """Get detailed table information from PostgreSQL"""
    tables = []
    try:
        config = load_config()
        pg_config = config.get('postgresql', {})
        
        conn = psycopg2.connect(
            host=pg_config.get('host', 'localhost'),
            port=pg_config.get('port', 5432),
            database=target_db,
            user=pg_config.get('username', 'postgres'),
            password=pg_config.get('password', '')
        )
        cur = conn.cursor()
        
        # Get all tables from all schemas (excluding system schemas). We'll
        # filter out 'public' and metric-sync schemas using is_excluded_schema.
        cur.execute("""
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_type = 'BASE TABLE' 
            AND table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_schema, table_name
        """)
        
        for schema_name, table_name in cur.fetchall():
            if is_excluded_schema(schema_name):
                continue
            try:
                # Get row count for this table
                cur.execute(f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"')
                count = cur.fetchone()[0]
                tables.append({
                    'schema_name': schema_name,
                    'table_name': table_name,
                    'row_count': count
                })
            except Exception as e:
                LOG.exception(f"Error counting rows in {schema_name}.{table_name}: {e}")
                continue
        
        cur.close()
        conn.close()
        
    except Exception as e:
        LOG.exception(f"Error getting PostgreSQL table details for {target_db}: {e}")
    
    return tables
