"""
PostgreSQL to ClickHouse Migration Wrapper
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import clickhouse_connect
import logging

logger = logging.getLogger(__name__)

def map_postgresql_to_clickhouse_type(pg_type: str) -> str:
    """Map PostgreSQL data types to ClickHouse data types"""
    type_mapping = {
        'smallint': 'Int16',
        'integer': 'Int32',
        'bigint': 'Int64',
        'serial': 'Int32',
        'bigserial': 'Int64',
        'smallserial': 'Int16',
        'real': 'Float32',
        'double precision': 'Float64',
        'numeric': 'Decimal64(2)',
        'decimal': 'Decimal64(2)',
        'money': 'Decimal64(2)',
        'boolean': 'UInt8',
        'character varying': 'String',
        'varchar': 'String',
        'character': 'FixedString(255)',
        'char': 'FixedString(255)',
        'text': 'String',
        'timestamp without time zone': 'DateTime',
        'timestamp with time zone': 'DateTime',
        'timestamp': 'DateTime',
        'date': 'Date',
        'time without time zone': 'String',
        'time with time zone': 'String',
        'interval': 'String',
        'bytea': 'String',
        'json': 'String',
        'jsonb': 'String',
        'uuid': 'UUID',
        'ARRAY': 'String',
    }
    
    pg_type_lower = pg_type.lower().strip()
    
    if '[]' in pg_type_lower or 'array' in pg_type_lower:
        return 'String'
    
    if pg_type_lower in type_mapping:
        return type_mapping[pg_type_lower]
    
    for pg_key, ch_type in type_mapping.items():
        if pg_type_lower.startswith(pg_key):
            return ch_type
    
    logger.warning(f"Unknown PostgreSQL type: {pg_type}, mapping to String")
    return 'String'

def get_postgresql_tables(conn):
    """Get all table names from PostgreSQL public schema"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """)
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return tables

def get_table_schema(conn, table_name: str):
    """Get column information for a table"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public' 
        AND table_name = %s
        ORDER BY ordinal_position;
    """, (table_name,))
    
    columns = []
    for row in cursor.fetchall():
        col_name, data_type, char_max_len, num_precision, num_scale, is_nullable = row
        
        full_type = data_type
        if char_max_len:
            full_type = f"{data_type}({char_max_len})"
        elif num_precision and num_scale:
            full_type = f"{data_type}({num_precision},{num_scale})"
        elif num_precision:
            full_type = f"{data_type}({num_precision})"
        
        columns.append({
            'name': col_name,
            'type': data_type,
            'full_type': full_type,
            'is_nullable': is_nullable == 'YES'
        })
    
    cursor.close()
    return columns

def get_primary_key_columns(conn, table_name: str):
    """Get primary key column names for a table"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = %s::regclass
        AND i.indisprimary;
    """, (table_name,))
    
    pk_columns = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return pk_columns

def table_exists_in_clickhouse(ch_client, table_name: str):
    """Check if a table exists in ClickHouse"""
    ch_table_name = f"HR_{table_name}"
    try:
        result = ch_client.command(f"EXISTS TABLE {ch_table_name}")
        return result == 1
    except Exception:
        return False

def get_existing_keys_from_clickhouse(ch_client, table_name: str, key_columns):
    """Get existing primary key values from ClickHouse table"""
    ch_table_name = f"HR_{table_name}"
    
    if not key_columns:
        return set()
    
    try:
        key_cols_str = ', '.join([f"`{col}`" for col in key_columns])
        query = f"SELECT {key_cols_str} FROM {ch_table_name}"
        result = ch_client.query(query)
        
        existing_keys = set()
        for row in result.result_rows:
            key_tuple = tuple(None if val is None else val for val in row)
            existing_keys.add(key_tuple)
        
        return existing_keys
    except Exception:
        return set()

def create_clickhouse_table(ch_client, table_name: str, columns):
    """Create a table in ClickHouse based on PostgreSQL schema"""
    ch_table_name = f"HR_{table_name}"
    
    if table_exists_in_clickhouse(ch_client, table_name):
        logger.info(f"Table {ch_table_name} already exists, skipping creation")
        return
    
    column_defs = []
    for col in columns:
        ch_type = map_postgresql_to_clickhouse_type(col['full_type'])
        nullable = "Nullable(" + ch_type + ")" if col['is_nullable'] else ch_type
        column_defs.append(f"`{col['name']}` {nullable}")
    
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {ch_table_name} (
        {', '.join(column_defs)}
    ) ENGINE = MergeTree()
    ORDER BY tuple()
    """
    
    logger.info(f"Creating ClickHouse table: {ch_table_name}")
    ch_client.command(create_sql)
    logger.info(f"Successfully created table: {ch_table_name}")

def migrate_table_data(pg_conn, ch_client, table_name: str, columns, is_new_table: bool = False):
    """Migrate data from PostgreSQL table to ClickHouse"""
    ch_table_name = f"HR_{table_name}"
    
    col_names = [col['name'] for col in columns]
    col_names_str = ', '.join([f'"{col}"' for col in col_names])
    
    logger.info(f"Fetching data from PostgreSQL table: {table_name}")
    pg_cursor = pg_conn.cursor(cursor_factory=RealDictCursor)
    pg_cursor.execute(f'SELECT {col_names_str} FROM "{table_name}"')
    
    rows = pg_cursor.fetchall()
    total_rows = len(rows)
    logger.info(f"Found {total_rows} rows in PostgreSQL table {table_name}")
    
    if total_rows == 0:
        logger.info(f"No data to migrate for table {table_name}")
        pg_cursor.close()
        return
    
    if not is_new_table:
        logger.info(f"Table {ch_table_name} already exists, checking for new rows only")
        
        pk_columns = get_primary_key_columns(pg_conn, table_name)
        
        if pk_columns:
            logger.info(f"Using primary key columns for duplicate detection: {pk_columns}")
            existing_keys = get_existing_keys_from_clickhouse(ch_client, table_name, pk_columns)
            logger.info(f"Found {len(existing_keys)} existing rows in ClickHouse")
            
            new_rows = []
            for row in rows:
                key_values = tuple(None if row[col] is None else row[col] for col in pk_columns)
                if key_values not in existing_keys:
                    new_rows.append(row)
            
            rows = new_rows
            logger.info(f"Found {len(rows)} new rows to insert (after filtering duplicates)")
        else:
            logger.info("No primary key found, using full row comparison")
            try:
                existing_result = ch_client.query(f"SELECT * FROM {ch_table_name}")
                existing_rows_set = set()
                
                for existing_row in existing_result.result_rows:
                    row_tuple = tuple(None if val is None else val for val in existing_row)
                    existing_rows_set.add(row_tuple)
                
                logger.info(f"Found {len(existing_rows_set)} existing rows in ClickHouse")
                
                new_rows = []
                for row in rows:
                    row_tuple = tuple(None if row[col] is None else row[col] for col in col_names)
                    if row_tuple not in existing_rows_set:
                        new_rows.append(row)
                
                rows = new_rows
                logger.info(f"Found {len(rows)} new rows to insert (after filtering duplicates)")
            except Exception as e:
                logger.warning(f"Could not fetch existing data for comparison: {str(e)}")
                logger.info("Proceeding with full migration (may create duplicates)")
    else:
        logger.info(f"New table detected, migrating all {total_rows} rows")
    
    if len(rows) == 0:
        logger.info(f"No new rows to insert for table {table_name}")
        pg_cursor.close()
        return
    
    data_to_insert = []
    for row in rows:
        row_data = []
        for col in col_names:
            value = row[col]
            if value is None:
                row_data.append(None)
            else:
                row_data.append(value)
        data_to_insert.append(row_data)
    
    batch_size = 1000
    inserted_count = 0
    
    for i in range(0, len(data_to_insert), batch_size):
        batch = data_to_insert[i:i + batch_size]
        try:
            ch_client.insert(ch_table_name, batch, column_names=col_names)
            inserted_count += len(batch)
            logger.info(f"Inserted {inserted_count}/{len(data_to_insert)} rows into {ch_table_name}")
        except Exception as e:
            logger.error(f"Error inserting batch into {ch_table_name}: {str(e)}")
            raise
    
    logger.info(f"Successfully migrated {inserted_count} new rows from {table_name} to {ch_table_name}")
    pg_cursor.close()

def run_pg_migration(pg_conn_data, ch_conn_data):
    """
    Main migration function - wrapper for migrate_pg_to_clickhouse.py
    Uses credentials from Jarvis_cred database
    """
    logger.info("Starting PostgreSQL to ClickHouse migration")
    logger.info(f"Using PostgreSQL connection: {pg_conn_data.get('pg_host')}:{pg_conn_data.get('pg_port', 5432)}/{pg_conn_data.get('pg_database')}")
    
    # Import the migration script and run it with credentials from database
    import sys
    import os
    import importlib.util
    
    # Get the base directory
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    script_path = os.path.join(BASE_DIR, "scripts", "migrate_pg_to_clickhouse.py")
    
    if not os.path.exists(script_path):
        error_msg = f"Migration script not found at: {script_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    # Load the migration script as a module
    spec = importlib.util.spec_from_file_location("migrate_pg", script_path)
    if spec is None or spec.loader is None:
        error_msg = f"Failed to load migration script from: {script_path}"
        logger.error(error_msg)
        raise ImportError(error_msg)
    
    migrate_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migrate_module)
    
    # Run main with credentials to load from database
    # The main function will load credentials from Jarvis_cred database
    try:
        logger.info("Calling migration script main function...")
        result = migrate_module.main(
            pg_host=pg_conn_data.get('pg_host'),
            pg_port=pg_conn_data.get('pg_port', 5432),
            pg_username=pg_conn_data.get('pg_username'),
            pg_password=pg_conn_data.get('pg_password')
        )
        
        logger.info(f"Migration script returned: {result}")
        
        if result:
            return result
        else:
            logger.warning("Migration script returned None, assuming success")
            return {
                'message': 'Migration completed successfully',
                'status': 'completed',
                'tables_migrated': 0
            }
    except Exception as e:
        error_msg = f"Migration failed: {str(e)}"
        logger.error(error_msg)
        import traceback
        logger.error(traceback.format_exc())
        raise Exception(error_msg) from e

