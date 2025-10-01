import pyodbc
import yaml
import os
from db_utils import get_pg_connection

def load_config():
    """Load database configuration from YAML file"""
    CONFIG_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "config/db_connections.yaml"))
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

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

def get_individual_server_comparison(server_name):
    """Get detailed comparison for a specific SQL Server"""
    try:
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
        
        return {
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
                        print(f"Error counting rows in SQL Server table {db_name}.{table_name}: {e}")
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
                print(f"Error processing database {db_name}: {e}")
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
        print(f"Error getting SQL Server total rows for {server_name}: {e}")
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
        config = load_config()
        pg_config = config.get('postgresql', {})
        
        # Create connection to target database
        import psycopg2
        conn = psycopg2.connect(
            host=pg_config.get('host', 'localhost'),
            port=pg_config.get('port', 5432),
            database=target_db,
            user=pg_config.get('username', 'postgres'),
            password=pg_config.get('password', '')
        )
        cur = conn.cursor()
        
        # Get all tables from ALL schemas (excluding system schemas and public schema)
        cur.execute("""
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_type = 'BASE TABLE' 
            AND table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast', 'public')
        """)
        all_tables = cur.fetchall()
        
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
                print(f"Error counting rows in PostgreSQL table {schema_name}.{table_name}: {e}")
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
        print(f"Error getting PostgreSQL total rows for database {target_db}: {e}")
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
        config = load_config()
        sqlservers = config.get("sqlservers", {})
        
        server_comparisons = []
        
        for server_name, server_config in sqlservers.items():
            comparison = get_individual_server_comparison(server_name)
            server_comparisons.append(comparison)
        
        return {
            'servers': server_comparisons,
            'total_servers': len(server_comparisons)
        }
    except Exception as e:
        print(f"Error getting all server comparisons: {e}")
        return {
            'servers': [],
            'total_servers': 0,
            'error': str(e)
        }
    """Get total row count from all SQL Server databases"""
    try:
        config = load_config()
        sqlservers = config.get("sqlservers", {})
        
        total_rows = 0
        server_details = []
        
        for server_name, server_config in sqlservers.items():
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
                        
                        db_row_count = 0
                        for table in tables:
                            try:
                                db_cur.execute(f'SELECT COUNT(*) FROM [{table}]')
                                count = db_cur.fetchone()[0]
                                db_row_count += count
                            except Exception as e:
                                print(f"Error counting rows in {db_name}.{table}: {e}")
                                continue
                        
                        server_row_count += db_row_count
                        db_cur.close()
                        db_conn.close()
                        
                    except Exception as e:
                        print(f"Error processing database {db_name}: {e}")
                        continue
                
                server_details.append({
                    'server_name': server_name,
                    'host': server_config['server'],
                    'port': server_config['port'],
                    'total_rows': server_row_count,
                    'target_postgres_db': server_config.get('target_postgres_db', 'Not specified')
                })
                
                total_rows += server_row_count
                cur.close()
                conn.close()
                
            except Exception as e:
                print(f"Error connecting to SQL Server {server_name}: {e}")
                server_details.append({
                    'server_name': server_name,
                    'host': server_config.get('server', 'Unknown'),
                    'port': server_config.get('port', 'Unknown'),
                    'total_rows': 0,
                    'target_postgres_db': server_config.get('target_postgres_db', 'Not specified'),
                    'error': str(e)
                })
                continue
        
        # Get target database name from first server for display
        target_db_name = None
        for server_detail in server_details:
            if 'target_postgres_db' in server_detail:
                target_db_name = server_detail['target_postgres_db']
                break
        
        return {
            'total_rows': total_rows,
            'servers': server_details,
            'target_postgres_db': target_db_name
        }
        
    except Exception as e:
        print(f"Error getting SQL Server total rows: {e}")
        return {
            'total_rows': 0,
            'servers': [],
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
            import psycopg2
            conn = psycopg2.connect(
                host=pg_config.get('host', 'localhost'),
                port=pg_config.get('port', 5432),
                database=target_db,
                user=pg_config.get('username', 'postgres'),
                password=pg_config.get('password', '')
            )
            cur = conn.cursor()
        
        # Get all tables from ALL schemas (excluding system schemas and public schema)
        cur.execute("""
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_type = 'BASE TABLE' 
            AND table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast', 'public')
        """)
        all_tables = cur.fetchall()
        
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
                print(f"Error counting rows in PostgreSQL table {schema_name}.{table_name}: {e}")
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
        print(f"Error getting PostgreSQL total rows: {e}")
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
        print(f"Error getting SQL Server total rows: {e}")
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
        print(f"Error getting PostgreSQL total rows: {e}")
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
                        print(f"Error counting rows in {db_name}.{table_name}: {e}")
                        continue
                
                db_cur.close()
                db_conn.close()
                
            except Exception as e:
                print(f"Error processing database {db_name}: {e}")
                continue
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Error getting SQL Server table details for {server_name}: {e}")
    
    return tables

def get_postgres_table_details(target_db):
    """Get detailed table information from PostgreSQL"""
    tables = []
    try:
        config = load_config()
        pg_config = config.get('postgresql', {})
        
        import psycopg2
        conn = psycopg2.connect(
            host=pg_config.get('host', 'localhost'),
            port=pg_config.get('port', 5432),
            database=target_db,
            user=pg_config.get('username', 'postgres'),
            password=pg_config.get('password', '')
        )
        cur = conn.cursor()
        
        # Get all tables from all schemas (excluding system schemas and public schema)
        cur.execute("""
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_type = 'BASE TABLE' 
            AND table_schema NOT IN ('information_schema', 'pg_catalog', 'public')
            ORDER BY table_schema, table_name
        """)
        
        for schema_name, table_name in cur.fetchall():
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
                print(f"Error counting rows in {schema_name}.{table_name}: {e}")
                continue
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Error getting PostgreSQL table details for {target_db}: {e}")
    
    return tables
