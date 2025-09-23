import hashlib
import os
import yaml
import pyodbc
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine, text, inspect
import psycopg2
from load_postgres import create_schema_if_not_exists, create_table_with_proper_types


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hybrid_sync.log'),
        logging.StreamHandler()
    ]
)

# Load DB connection info from YAML
CONFIG_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), 'config/db_connections.yaml')
)

OUTPUT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), 'data/sqlserver_exports/')
)

with open(CONFIG_PATH, 'r') as f:
    config = yaml.safe_load(f)

pg_conf = config['postgresql']

BATCH_SIZE = int(os.environ.get('HYBRID_SYNC_BATCH_SIZE', '10000'))


# ------------------------- Connections -------------------------
def get_sql_connection(conf, database=None):
    """
    Get a pyodbc connection to SQL Server.
    Handles named instances, optional port, and escapes backslashes.
    """
    server = conf['server'].replace("\\", "\\\\")  # Escape backslash for pyodbc
    port = conf.get('port')
    if port:
        # For TCP/IP connection, append port
        server = f"{server},{port}"

    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"UID={conf['username']};PWD={conf['password']}"
    )
    if database:
        conn_str += f";DATABASE={database}"
    conn_str += ";MARS_Connection=Yes;Timeout=30"

    return pyodbc.connect(conn_str)


def get_sqlalchemy_engine(conf, database=None):
    """
    Get a SQLAlchemy engine for SQL Server using pyodbc.
    Handles named instances, optional port, and escaping.
    """
    username = conf['username']
    password = conf['password']
    server = conf['server'].replace("\\", "\\\\")
    port = conf.get('port')
    if port:
        server = f"{server},{port}"  # SQLAlchemy + ODBC accepts comma for port

    db = database if database else "master"

    # URL-encode driver and password
    from urllib.parse import quote_plus
    driver = quote_plus("ODBC Driver 17 for SQL Server")
    password_enc = quote_plus(password)

    conn_url = f"mssql+pyodbc://{username}:{password_enc}@{server}/{db}?driver={driver}"

    return create_engine(conn_url, fast_executemany=True)


def get_pg_engine(target_db=None):
    """Get PostgreSQL engine for a specific target DB"""
    db_name = target_db if target_db else pg_conf['database']
    conn_str = (
        f"postgresql+psycopg2://{pg_conf['username']}:{pg_conf['password']}@"
        f"{pg_conf['host']}:{pg_conf['port']}/{db_name}"
    )
    return create_engine(conn_str)

# ------------------------- Param coercion -------------------------

def _coerce_param(value):
    """Coerce numpy/pandas types to native Python types for pyodbc parameters."""
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    try:
        if hasattr(value, 'item'):
            return value.item()
    except Exception:
        pass
    if isinstance(value, str):
        try:
            if value.isdigit() or (value.startswith('-') and value[1:].isdigit()):
                return int(value)
            return float(value)
        except Exception:
            try:
                return pd.to_datetime(value).to_pydatetime()
            except Exception:
                return value
    return value


# ------------------------- Tracking tables -------------------------

def create_sync_tracking_table(engine):
    """Create table to track database sync status"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS sync_database_status (
        server_name VARCHAR(100),
        database_name VARCHAR(100),
        last_full_sync TIMESTAMP,
        last_incremental_sync TIMESTAMP,
        sync_status VARCHAR(20),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (server_name, database_name)
    )
    """
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()
        logging.info("Sync tracking table created/verified")


def create_table_sync_tracking(engine):
    """Create table to track table-level sync status"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS sync_table_status (
        server_name VARCHAR(100),
        database_name VARCHAR(100),
        schema_name VARCHAR(100),
        table_name VARCHAR(100),
        last_pk_value VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (server_name, database_name, schema_name, table_name)
    )
    """
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()


def get_sync_status(engine, server_name, database_name):
    """Get sync status for a database"""
    query = """
    SELECT last_full_sync, last_incremental_sync, sync_status
    FROM sync_database_status
    WHERE server_name = :server_name AND database_name = :database_name
    """
    with engine.connect() as conn:
        result = conn.execute(
            text(query), {"server_name": server_name, "database_name": database_name}
        )
        row = result.fetchone()
        return row if row else None


def update_sync_status(engine, server_name, database_name, sync_type, sync_status):
    """Update sync status for a database"""
    now = datetime.now()
    if sync_type == 'full':
        query = """
        INSERT INTO sync_database_status (server_name, database_name, last_full_sync, sync_status, updated_at)
        VALUES (:server_name, :database_name, :now, :sync_status, :now)
        ON CONFLICT (server_name, database_name) 
        DO UPDATE SET 
            last_full_sync = EXCLUDED.last_full_sync,
            sync_status = EXCLUDED.sync_status,
            updated_at = EXCLUDED.updated_at
        """
    else:
        query = """
        INSERT INTO sync_database_status (server_name, database_name, last_incremental_sync, sync_status, updated_at)
        VALUES (:server_name, :database_name, :now, :sync_status, :now)
        ON CONFLICT (server_name, database_name) 
        DO UPDATE SET 
            last_incremental_sync = EXCLUDED.last_incremental_sync,
            sync_status = EXCLUDED.sync_status,
            updated_at = EXCLUDED.updated_at
        """
    with engine.connect() as conn:
        conn.execute(
            text(query),
            {
                "server_name": server_name,
                "database_name": database_name,
                "now": now,
                "sync_status": sync_status,
            },
        )
        conn.commit()


def get_last_synced_pk(engine, server_name, database_name, schema, table):
    """Get last synced primary key/timestamp value as string"""
    query = """
    SELECT last_pk_value
    FROM sync_table_status
    WHERE server_name = :server_name AND database_name = :database_name AND schema_name = :schema AND table_name = :table
    """
    with engine.connect() as conn:
        result = conn.execute(
            text(query),
            {
                "server_name": server_name,
                "database_name": database_name,
                "schema": schema,
                "table": table,
            },
        )
        row = result.fetchone()
        return row[0] if row else None


def update_last_synced_pk(engine, server_name, database_name, schema, table, pk_value):
    """Update last synced value; store as string for portability"""
    if hasattr(pk_value, 'item'):
        pk_value = pk_value.item()
    query = """
    INSERT INTO sync_table_status (server_name, database_name, schema_name, table_name, last_pk_value, updated_at)
    VALUES (:server_name, :database_name, :schema, :table, :pk_value, :now)
    ON CONFLICT (server_name, database_name, schema_name, table_name) 
    DO UPDATE SET 
        last_pk_value = EXCLUDED.last_pk_value,
        updated_at = EXCLUDED.updated_at
    """
    with engine.connect() as conn:
        conn.execute(
            text(query),
            {
                "server_name": server_name,
                "database_name": database_name,
                "schema": schema,
                "table": table,
                "pk_value": str(pk_value) if pk_value is not None else None,
                "now": datetime.now(),
            },
        )
        conn.commit()


# ------------------------- Helpers: discovery -------------------------

def get_all_databases(conn):
    """Get list of all user databases on the server"""
    cursor = conn.cursor()
    databases = []
    query = """
    SELECT name 
    FROM sys.databases 
    WHERE state = 0  
    AND name NOT IN ('master', 'tempdb', 'model', 'msdb', 'distribution', 'ReportServer', 'ReportServerTempDB')
    ORDER BY name
    """
    cursor.execute(query)
    for row in cursor.fetchall():
        databases.append(row[0])
    return databases


def should_skip_database(db_name, conf):
    skip_databases = conf.get('skip_databases', [])
    if not skip_databases:
        return False
    if db_name in skip_databases:
        logging.info(f"Skipping database: {db_name} (listed in skip_databases)")
        return True
    return False


def should_skip_table(schema, table):
    if schema.lower() == 'sys':
        return True
    system_tables = {
        'sys.trace_xe_event_map',
        'sys.trace_xe_action_map',
    }
    return f"{schema}.{table}" in system_tables


# ------------------------- Schema evolution (Postgres) -------------------------

def get_pg_columns(engine, schema, table_name):
    insp = inspect(engine)
    try:
        cols = insp.get_columns(table_name, schema=schema)
        return {c['name']: c for c in cols}
    except Exception:
        return {}


def infer_pg_type_from_series(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series):
        return 'BIGINT'
    if pd.api.types.is_float_dtype(series):
        return 'DOUBLE PRECISION'
    if pd.api.types.is_bool_dtype(series):
        return 'BOOLEAN'
    if pd.api.types.is_datetime64_any_dtype(series):
        return 'TIMESTAMP'
    return 'TEXT'


def ensure_table_and_columns(engine, schema, table_name, df: pd.DataFrame):
    create_schema_if_not_exists(engine, schema)
    existing_cols = get_pg_columns(engine, schema, table_name)
    if not existing_cols:
        create_table_with_proper_types(engine, schema, table_name, df)
        return
    missing = [c for c in df.columns if c not in existing_cols]
    if not missing:
        return
    alter_parts = []
    for col in missing:
        col_type = infer_pg_type_from_series(df[col])
        safe_col = ''.join(ch for ch in col if ch.isalnum() or ch in '_-')
        alter_parts.append(f'ADD COLUMN "{safe_col}" {col_type}')
    if alter_parts:
        sql = f'ALTER TABLE "{schema}"."{table_name}" ' + ', '.join(alter_parts)
        with engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()
        logging.info(f"Added columns on {schema}.{table_name}: {missing}")


# ------------------------- Source helpers (SQL Server) -------------------------

def get_primary_key_info(conn, schema, table):
    try:
        query = f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = '{schema}' 
        AND TABLE_NAME = '{table}'
        AND CONSTRAINT_NAME LIKE 'PK_%'
        ORDER BY ORDINAL_POSITION
        """
        cursor = conn.cursor()
        cursor.execute(query)
        return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        logging.warning(f"Could not get PK info for {schema}.{table}: {e}")
        return []


def get_timestamp_column(conn, schema, table):
    try:
        query = f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}' 
        AND TABLE_NAME = '{table}'
        AND DATA_TYPE IN ('datetime', 'datetime2', 'smalldatetime', 'timestamp')
        ORDER BY COLUMN_NAME
        """
        cursor = conn.cursor()
        cursor.execute(query)
        cols = [row[0] for row in cursor.fetchall()]
        return cols[0] if cols else None
    except Exception as e:
        logging.warning(f"Could not get timestamp column for {schema}.{table}: {e}")
        return None


def get_unique_identifier_column(conn, schema, table):
    try:
        query = f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}' 
        AND TABLE_NAME = '{table}'
        AND DATA_TYPE IN ('uniqueidentifier', 'int', 'bigint')
        ORDER BY COLUMN_NAME
        """
        cursor = conn.cursor()
        cursor.execute(query)
        cols = [row[0] for row in cursor.fetchall()]
        return cols[0] if cols else None
    except Exception as e:
        logging.warning(f"Could not get unique identifier column for {schema}.{table}: {e}")
        return None


def get_table_row_count(conn, schema, table):
    try:
        query = f"SELECT COUNT(*) FROM [{schema}].[{table}]"
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchone()[0]
    except Exception as e:
        logging.warning(f"Could not get row count for {schema}.{table}: {e}")
        return 0


def check_for_new_rows(conn, schema, table, sync_col, last_value):
    if last_value is None:
        return True
    query = f"SELECT COUNT(*) FROM [{schema}].[{table}] WHERE [{sync_col}] > ?"
    cursor = conn.cursor()
    cursor.execute(query, [_coerce_param(last_value)])
    return cursor.fetchone()[0] > 0


# ------------------------- Sync core -------------------------

def write_audit_csv(server_clean, db_name, schema, table, df: pd.DataFrame):
    server_dir = os.path.join(OUTPUT_DIR, f"{server_clean}_{db_name}")
    Path(server_dir).mkdir(parents=True, exist_ok=True)
    filename = f"{schema}_{table}.csv"
    filepath = os.path.join(server_dir, filename)
    df.to_csv(filepath, index=False)
    return filepath


def batch_fetch_new_rows(engine, schema, table, sync_column, last_value, batch_size):
    """Yield batches of new rows ordered by sync_column for resume capability."""
    next_marker = last_value
    while True:
        query = f"SELECT TOP ({int(batch_size)}) * FROM [{schema}].[{table}]"
        params = {}
        if next_marker is None:
            query += f" ORDER BY [{sync_column}] ASC"
            df = pd.read_sql(query, engine)
        else:
            query += f" WHERE [{sync_column}] > :marker ORDER BY [{sync_column}] ASC"
            df = pd.read_sql(text(query), engine, params={"marker": _coerce_param(next_marker)})
        if df.empty:
            break
        next_marker = df[sync_column].max()
        yield df, next_marker


def full_sync_table(pg_engine, server_conf, db_name, server_clean, sql_engine, conn, schema, table):
    if should_skip_table(schema, table):
        return 0
    query = f"SELECT * FROM [{schema}].[{table}]"
    df = pd.read_sql(query, sql_engine)
    if df.empty:
        return 0
    schema_name = f"{server_clean}_{db_name}".replace('-', '_').replace(' ', '_')
    table_name = f"{schema}_{table}"
    ensure_table_and_columns(pg_engine, schema_name, table_name, df)
    df.to_sql(table_name, pg_engine, schema=schema_name, if_exists='append', index=False, chunksize=BATCH_SIZE)
    pk_columns = get_primary_key_info(conn, schema, table)
    ts_col = get_timestamp_column(conn, schema, table)
    uid_col = get_unique_identifier_column(conn, schema, table)
    sync_col = pk_columns[0] if pk_columns else (ts_col if ts_col else uid_col)
    if sync_col and sync_col in df.columns:
        update_last_synced_pk(pg_engine, server_conf['server'], db_name, schema, table, df[sync_col].max())
    write_audit_csv(server_clean, db_name, schema, table, df.head(0))
    return len(df)


def incremental_sync_table(pg_engine, server_conf, db_name, server_clean, sql_engine, conn, schema, table):
    if should_skip_table(schema, table):
        return 0
    current_count = get_table_row_count(conn, schema, table)
    last_value = get_last_synced_pk(pg_engine, server_conf['server'], db_name, schema, table)
    if last_value is not None and current_count == 0:
        logging.info(f"Detected empty source (possible TRUNCATE) for {schema}.{table}; skipping to preserve target")
        return 0
    pk_columns = get_primary_key_info(conn, schema, table)
    ts_col = get_timestamp_column(conn, schema, table)
    uid_col = get_unique_identifier_column(conn, schema, table)
    sync_col = pk_columns[0] if pk_columns else (ts_col if ts_col else uid_col)
    schema_name = f"{server_clean}_{db_name}".replace('-', '_').replace(' ', '_')
    table_name = f"{schema}_{table}"
    processed = 0
    if sync_col:
        if last_value is None:
            # When last_value is None, use hash-based deduplication instead of skipping
            logging.info(f"Last value is None for {schema}.{table}, using hash-based deduplication")
            
            # Fetch all data from source
            query = f"SELECT * FROM [{schema}].[{table}]"
            df = pd.read_sql(query, sql_engine)
            if df.empty:
                return 0
            
            ensure_table_and_columns(pg_engine, schema_name, table_name, df)
            
            # Check if target table exists and has data
            with pg_engine.connect() as pg_conn:
                try:
                    dst_df = pd.read_sql(f'SELECT * FROM "{schema_name}"."{table_name}"', pg_conn)
                except Exception:
                    dst_df = pd.DataFrame()
            
            if dst_df.empty:
                # Target table is empty, insert all data
                df.to_sql(table_name, pg_engine, schema=schema_name, if_exists='append', index=False, chunksize=BATCH_SIZE)
                # Update last synced value
                if sync_col in df.columns:
                    update_last_synced_pk(pg_engine, server_conf['server'], db_name, schema, table, df[sync_col].max())
                return len(df)
            else:
                # Target table has data, use hash-based deduplication
                common = list(set(df.columns) & set(dst_df.columns))
                if not common:
                    return 0
                
                src = df[common].fillna('')
                dst = dst_df[common].fillna('')
                
                # Use the same hash logic as the fallback
                def row_md5(row):
                    row_tuple = tuple(str(x) for x in row)
                    return hashlib.md5(str(row_tuple).encode('utf-8')).hexdigest()
                
                src['row_hash'] = src.apply(row_md5, axis=1)
                dst['row_hash'] = dst.apply(row_md5, axis=1)
                
                new_rows_idx = src[~src['row_hash'].isin(dst['row_hash'])].index
                if len(new_rows_idx) > 0:
                    df.iloc[new_rows_idx].to_sql(table_name, pg_engine, schema=schema_name, if_exists='append', index=False, chunksize=BATCH_SIZE)
                    # Update last synced value
                    if sync_col in df.columns:
                        update_last_synced_pk(pg_engine, server_conf['server'], db_name, schema, table, df[sync_col].max())
                    return len(new_rows_idx)
                else:
                    logging.info(f"No new rows found for {schema}.{table}")
                    return 0
        else:
            # Normal incremental sync when last_value is not None
            for df, _ in batch_fetch_new_rows(sql_engine, schema, table, sync_col, last_value, BATCH_SIZE):
                if df.empty:
                    continue
                ensure_table_and_columns(pg_engine, schema_name, table_name, df)
                df.to_sql(table_name, pg_engine, schema=schema_name, if_exists='append', index=False, chunksize=BATCH_SIZE)
                next_marker = df[sync_col].max()
                update_last_synced_pk(pg_engine, server_conf['server'], db_name, schema, table, next_marker)
                processed += len(df)

    else:
        query = f"SELECT * FROM [{schema}].[{table}]"
        df = pd.read_sql(query, sql_engine)
        if df.empty:
            return 0
        ensure_table_and_columns(pg_engine, schema_name, table_name, df)
        with pg_engine.connect() as pg_conn:
            try:
                dst_df = pd.read_sql(f'SELECT * FROM "{schema_name}"."{table_name}"', pg_conn)
            except Exception:
                dst_df = pd.DataFrame()
        common = list(set(df.columns) & set(dst_df.columns)) if not dst_df.empty else list(df.columns)
        if not common:
            return 0
        src = df[common].fillna('')
        if dst_df.empty:
            df.to_sql(table_name, pg_engine, schema=schema_name, if_exists='append', index=False, chunksize=BATCH_SIZE)
            processed = len(df)
        else:
            dst = dst_df[common].fillna('')
            #commented by vikas for the row duplication
            #src['row_hash'] = src.apply(lambda x: hash(tuple(x)), axis=1)
            #dst['row_hash'] = dst.apply(lambda x: hash(tuple(x)), axis=1)
            def row_md5(row):
    # Convert row to tuple of strings to handle NaNs consistently
             row_tuple = tuple(str(x) for x in row)
             return hashlib.md5(str(row_tuple).encode('utf-8')).hexdigest()
 
            src['row_hash'] = src.apply(row_md5, axis=1)
            dst['row_hash'] = dst.apply(row_md5, axis=1)
            
            
            new_rows_idx = src[~src['row_hash'].isin(dst['row_hash'])].index
            if len(new_rows_idx) > 0:
                df.iloc[new_rows_idx].to_sql(table_name, pg_engine, schema=schema_name, if_exists='append', index=False, chunksize=BATCH_SIZE)
                processed = len(new_rows_idx)
    return processed

def full_sync_database(sql_engine, db_name, server_conf, server_clean, output_dir, pg_engine):
    logging.info(f"=== Starting FULL sync for database: {db_name} ===")
    cursor = sql_engine.raw_connection().cursor()
    tables = []
    for row in cursor.tables(tableType='TABLE'):
        tables.append((row.table_schem, row.table_name))

    if not tables:
        logging.warning(f"No tables found in {db_name}.")
        return 0

    processed_count = 0
    for schema, table in tables:
        try:
            logging.info(f"[FULL SYNC] Processing {schema}.{table}")
            processed = full_sync_table(pg_engine, server_conf, db_name, server_clean, sql_engine, cursor, schema, table)
            processed_count += 1 if processed > 0 else 0
        except Exception as e:
            logging.error(f"Failed to export/load {schema}.{table}: {e}")
    logging.info(f"=== FULL sync completed for {db_name}, {processed_count}/{len(tables)} tables processed ===")
    return processed_count


def incremental_sync_database(sql_engine, conn, db_name, server_conf, server_clean, output_dir, pg_engine):
    logging.info(f"=== Starting INCREMENTAL sync for database: {db_name} ===")
    cursor = conn.cursor()
    tables = []
    for row in cursor.tables(tableType='TABLE'):
        tables.append((row.table_schem, row.table_name))

    if not tables:
        logging.warning(f"No tables found in {db_name}.")
        return 0

    processed_count = 0
    for schema, table in tables:
        try:
            # Add debug info
            row_count = get_table_row_count(conn, schema, table)
            pk_columns = get_primary_key_info(conn, schema, table)
            ts_col = get_timestamp_column(conn, schema, table)
            uid_col = get_unique_identifier_column(conn, schema, table)
            sync_col = pk_columns[0] if pk_columns else (ts_col if ts_col else uid_col)
            last_value = get_last_synced_pk(pg_engine, server_conf['server'], db_name, schema, table)
            logging.info(
                f"[INCR SYNC] {schema}.{table}: row_count={row_count}, sync_col={sync_col}, last_value={last_value}"
            )

            processed = incremental_sync_table(
                pg_engine, server_conf, db_name, server_clean, sql_engine, conn, schema, table
            )
            processed_count += 1 if processed > 0 else 0
        except Exception as e:
            logging.error(f"Failed to sync/load {schema}.{table}: {e}")

    logging.info(
        f"=== INCREMENTAL sync completed for {db_name}, {processed_count}/{len(tables)} tables processed ==="
    )
    return processed_count






# -----------------------------------------------------------------
def cleanup_system_tables(engine, schema_name):
    system_tables = [
        'sys_trace_xe_event_map',
        'sys_trace_xe_action_map',
    ]
    for tbl in system_tables:
        try:
            with engine.connect() as conn:
                conn.execute(text(f'DROP TABLE IF EXISTS "{schema_name}"."{tbl}"'))
                conn.commit()
                logging.info(f"Cleaned up system table: {schema_name}.{tbl}")
        except Exception as e:
            logging.warning(f"Could not clean up {schema_name}.{tbl}: {e}")


def process_sql_server_hybrid(server_name, server_conf):
    try:
        pg_engine = get_pg_engine(server_conf.get("target_postgres_db"))
        create_sync_tracking_table(pg_engine)
        create_table_sync_tracking(pg_engine)

        master_conn = get_sql_connection(server_conf)
        logging.info(f"Connected to SQL Server: {server_conf['server']}")
        databases = get_all_databases(master_conn)
        master_conn.close()

        if not databases:
            logging.warning(f"No user databases found on {server_conf['server']}.")
            return

        logging.info(f"Found {len(databases)} databases on {server_conf['server']}")
        server_clean = ''.join(c for c in server_conf['server'] if c.isalnum() or c in '_-')

        for db_name in databases:
            if should_skip_database(db_name, server_conf):
                continue

            schema_name = f"{server_clean}_{db_name}".replace('-', '_').replace(' ', '_')
            cleanup_system_tables(pg_engine, schema_name)

            sync_status = get_sync_status(pg_engine, server_conf['server'], db_name)
            db_conn = get_sql_connection(server_conf, db_name)
            sql_engine = get_sqlalchemy_engine(server_conf, db_name)

            try:
                if sync_status is None:
                    # First time → full sync
                    processed = full_sync_database(sql_engine, db_name, server_conf, server_clean, OUTPUT_DIR, pg_engine)
                    update_sync_status(pg_engine, server_conf['server'], db_name, 'full', 'COMPLETED')
                else:
                    # Later runs → incremental
                    processed = incremental_sync_database(sql_engine, db_conn, db_name, server_conf, server_clean, OUTPUT_DIR, pg_engine)
                    update_sync_status(pg_engine, server_conf['server'], db_name, 'incremental', 'COMPLETED')

                logging.info(f"{server_name}/{db_name}: processed {processed} tables")
            finally:
                db_conn.close()
                sql_engine.dispose()

        logging.info(f"Completed {server_name}")
    except Exception as e:
        logging.error(f"Error processing {server_name}: {e}")
def main():
    sqlservers = config.get('sqlservers', {})
    if not sqlservers:
        logging.error("No SQL servers configured in db_connections.yaml")
        return
    logging.info(f"Starting hybrid sync for {len(sqlservers)} SQL servers")
    for server_name, server_conf in sqlservers.items():
        logging.info(f"Processing SQL Server: {server_name}")
        process_sql_server_hybrid(server_name, server_conf)
    logging.info("Hybrid sync complete for all servers.")


if __name__ == "__main__":
    main()
