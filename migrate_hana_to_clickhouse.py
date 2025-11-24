"""
HANA SAP Database to ClickHouse Migration Script
Combines the best of both approaches:
- Handles structured exports (with create.sql)
- Handles unstructured CSV files
- Creates tables even if they're empty
- Robust encoding detection and type mapping

Requirements:
    pip install clickhouse-connect pandas

Note: This script uses clickhouse-connect (not clickhouse-driver) for better
pandas integration with insert_df() method.
"""

import os
import re
import pandas as pd
from clickhouse_connect import get_client
import warnings
from typing import Optional, Tuple, List, Dict
import sys

warnings.filterwarnings("ignore")  # Hide date parsing warnings

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Import config loaders
try:
    from db_utils import load_clickhouse_config
except ImportError:
    load_clickhouse_config = None

# ---------------- CONFIG FROM .ENV ----------------
def get_clickhouse_config():
    """Get ClickHouse configuration from .env or function parameters."""
    if load_clickhouse_config:
        try:
            config = load_clickhouse_config()
            return {
                'host': config['host'],
                'port': config.get('port', 9000),
                'username': config['user'],
                'password': config['password'],
                'database': os.environ.get('CLICKHOUSE_DATABASE', 'JARVIS_DB')
            }
        except Exception as e:
            print(f"⚠️ Could not load ClickHouse config from db_utils: {e}")
    
    # Fallback to direct environment variables
    return {
        'host': os.environ.get('CLICKHOUSE_HOST', 'localhost'),
        'port': int(os.environ.get('CLICKHOUSE_PORT', '9000')),
        'username': os.environ.get('CLICKHOUSE_USER', 'default'),
        'password': os.environ.get('CLICKHOUSE_PASSWORD', ''),
        'database': os.environ.get('CLICKHOUSE_DATABASE', 'JARVIS_DB')
    }

def get_base_dir():
    """Get base directory from .env or use default."""
    return os.environ.get('HANA_EXPORT_BASE_DIR', r'E:\SAP\2\index\Z_TEST_PRODUCTION_20250925')

# Global client (will be initialized in main or by caller)
client = None

# ---------------- UTILITY FUNCTIONS ----------------

def sanitize_column(name: str) -> str:
    """Sanitize column names for ClickHouse compatibility."""
    name = str(name).strip().replace(" ", "_")
    name = "".join(c if c.isalnum() or c == "_" else "_" for c in name)
    if name and name[0].isdigit():
        name = "col_" + name
    if not name:
        name = "unnamed_col"
    return name


def map_hana_to_clickhouse(hana_type: str) -> str:
    """Map HANA data types to ClickHouse types."""
    hana_type = hana_type.upper()
    
    if any(x in hana_type for x in ['INTEGER', 'SMALLINT', 'CS_INT', 'TINYINT']):
        return 'Nullable(Int32)'
    elif any(x in hana_type for x in ['BIGINT', 'CS_BIGINT']):
        return 'Nullable(Int64)'
    elif any(x in hana_type for x in ['DECIMAL', 'NUMERIC']):
        # Extract precision and scale if available
        match = re.search(r'DECIMAL\((\d+),(\d+)\)', hana_type)
        if match:
            precision, scale = match.groups()
            return f'Nullable(Decimal({precision},{scale}))'
        return 'Nullable(Decimal(18,2))'
    elif any(x in hana_type for x in ['DOUBLE', 'FLOAT', 'REAL']):
        return 'Nullable(Float64)'
    elif any(x in hana_type for x in ['DATE', 'LONGDATE']):
        return 'Nullable(Date)'
    elif any(x in hana_type for x in ['TIME', 'TIMESTAMP', 'SECONDDATE']):
        return 'Nullable(DateTime)'
    elif any(x in hana_type for x in ['NVARCHAR', 'VARCHAR', 'CHAR', 'NCHAR', 'TEXT', 'CLOB']):
        return 'Nullable(String)'
    elif any(x in hana_type for x in ['BLOB', 'BINARY', 'VARBINARY']):
        return 'Nullable(String)'  # Store as base64 string
    else:
        return 'Nullable(String)'  # Default to String


def detect_nullable_type(series: pd.Series) -> str:
    """Detect ClickHouse column type from pandas Series."""
    if series.empty:
        return 'Nullable(String)'  # Default for empty columns
    
    dtype = str(series.dtype)
    
    # Check for numeric types
    if 'int' in dtype:
        if series.max() > 2147483647 or series.min() < -2147483648:
            return 'Nullable(Int64)'
        return 'Nullable(Int32)'
    elif 'float' in dtype:
        return 'Nullable(Float64)'
    elif 'datetime' in dtype or 'timestamp' in dtype:
        return 'Nullable(DateTime)'
    elif 'bool' in dtype:
        return 'Nullable(UInt8)'
    else:
        # Try to detect datetime-like strings
        sample_nonnull = series.dropna().astype(str).head(10).tolist()
        date_patterns = ['-', '/', '202', '20', ':', 'T']
        if any(any(pattern in s for pattern in date_patterns) for s in sample_nonnull if len(s) > 4):
            # Try parsing to confirm
            try:
                test_parse = pd.to_datetime(series.dropna().head(5), errors='coerce')
                if test_parse.notna().sum() > 0:
                    return 'Nullable(DateTime)'
            except:
                pass
        return 'Nullable(String)'


def safe_read_csv(csv_file: str, col_names: Optional[List[str]] = None) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """Safely read CSV with multiple encoding attempts."""
    encodings = ['utf-8', 'latin1', 'cp1252', 'utf-16', 'iso-8859-1', 'windows-1250']
    
    for enc in encodings:
        try:
            df = pd.read_csv(
                csv_file,
                header=None if col_names else 0,
                names=col_names,
                dtype=str,
                sep=',',
                quotechar='"',
                on_bad_lines='skip',
                encoding=enc,
                low_memory=False
            )
            return df, enc
        except Exception as e:
            continue
    
    return None, None


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and normalize DataFrame before inserting into ClickHouse."""
    if df.empty:
        return df
    
    for col in df.columns:
        # Replace empty strings and whitespace with None
        df[col] = df[col].replace({'': None, ' ': None, pd.NA: None, 'NULL': None, 'null': None})
        
        # Try to detect and parse dates
        if df[col].dtype == 'object':
            try:
                # Try common date formats
                for fmt in ["%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%d/%m/%Y", 
                           "%Y-%m-%d %H:%M:%S", "%d-%m-%Y %H:%M:%S",
                           "%Y/%m/%d %H:%M:%S", "%d/%m/%Y %H:%M:%S"]:
                    parsed = pd.to_datetime(df[col], format=fmt, errors='coerce')
                    if parsed.notna().sum() > len(df) * 0.5:  # If >50% parseable
                        df[col] = parsed
                        break
            except Exception:
                pass
    
    return df


def parse_create_sql(create_sql_file: str) -> Tuple[Optional[List[str]], Optional[List[str]]]:
    """Parse HANA create.sql file to extract column names and types."""
    try:
        with open(create_sql_file, 'r', encoding='utf-8') as f:
            sql_text = f.read()
    except Exception as e:
        print(f"⚠️ Could not read create.sql: {e}")
        return None, None
    
    try:
        # Extract column definitions from CREATE TABLE statement
        col_section_match = re.search(r'\((.*?)\)\s*(?:UNLOAD|WITH|$)', sql_text, re.DOTALL)
        if not col_section_match:
            return None, None
        
        col_section = col_section_match.group(1)
        # Split by comma, but respect parentheses (for DECIMAL(18,2) etc.)
        raw_cols = re.split(r',\s*(?![^()]*\))', col_section)
        
        sanitized_cols = []
        col_types = []
        
        for col_def in raw_cols:
            col_def = col_def.strip()
            if not col_def:
                continue
            
            # Match column name and type
            # Handles: "COL_NAME" TYPE or COL_NAME TYPE
            match = re.match(r'"?([\w]+)"?\s+([A-Z0-9_()]+)', col_def, re.IGNORECASE)
            if match:
                raw_name, hana_type = match.groups()
                sanitized_cols.append(sanitize_column(raw_name))
                col_types.append(map_hana_to_clickhouse(hana_type))
        
        if sanitized_cols and col_types:
            return sanitized_cols, col_types
    except Exception as e:
        print(f"⚠️ Failed to parse create.sql: {e}")
    
    return None, None


def create_table_safe(table_name: str, columns: List[str], types: List[str]) -> bool:
    """Safely create ClickHouse table with schema validation."""
    try:
        # Build column definitions
        col_defs = [f"`{col}` {dtype}" for col, dtype in zip(columns, types)]
        col_sql = ", ".join(col_defs)
        
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {col_sql}
        ) ENGINE = MergeTree()
        ORDER BY tuple()
        """
        
        client.command(create_sql)
        
        # Verify table was created
        desc_df = client.query_df(f"DESCRIBE TABLE {table_name}")
        existing_cols = desc_df["name"].tolist()
        
        if set(existing_cols) != set(columns):
            print(f"⚠️ Schema mismatch for {table_name}. Dropping and recreating...")
            client.command(f"DROP TABLE IF EXISTS {table_name}")
            client.command(create_sql)
        
        return True
    except Exception as e:
        print(f"❌ Failed to create table {table_name}: {e}")
        return False


def insert_data_safe(table_name: str, df: pd.DataFrame, column_names: List[str]) -> bool:
    """Safely insert data into ClickHouse table."""
    try:
        if df.empty:
            print(f"ℹ️ Table {table_name} is empty - table structure created")
            return True
        
        # Clean data types before insert
        for col in column_names:
            if col not in df.columns:
                continue
            
            # Convert numeric columns
            if df[col].dtype == 'object':
                # Try numeric conversion
                numeric_series = pd.to_numeric(df[col], errors='coerce')
                if numeric_series.notna().sum() > len(df) * 0.5:
                    df[col] = numeric_series
            
            # Replace NaN/NaT with None for ClickHouse
            df[col] = df[col].where(pd.notnull(df[col]), None)
        
        client.insert_df(table_name, df, column_names=column_names)
        print(f"✅ Inserted {len(df)} rows into {table_name}")
        return True
    except Exception as e:
        print(f"❌ Insert failed for {table_name}: {e}")
        return False


# ---------------- MAIN MIGRATION LOGIC ----------------

def migrate_structured_table(table_path: str, table_dir: str) -> bool:
    """Migrate table with create.sql and data.csv structure."""
    table_name = f"SAP_{table_dir}"
    create_sql_file = os.path.join(table_path, 'create.sql')
    csv_file = os.path.join(table_path, 'data.csv')
    
    if not os.path.exists(create_sql_file):
        return False
    
    print(f"\n📦 Processing structured table: {table_name}")
    
    # Parse create.sql
    sanitized_cols, col_types = parse_create_sql(create_sql_file)
    if not sanitized_cols or not col_types:
        print(f"❌ Failed to parse create.sql for {table_name}")
        return False
    
    # Read CSV
    if os.path.exists(csv_file):
        df, used_encoding = safe_read_csv(csv_file, sanitized_cols)
        if df is None:
            print(f"❌ Could not decode CSV for {table_name}")
            # Still create table structure even if CSV is unreadable
            if create_table_safe(table_name, sanitized_cols, col_types):
                print(f"✅ Table structure created for {table_name} (empty)")
            return False
        else:
            print(f"✔ CSV loaded for {table_name} using encoding: {used_encoding}")
            df = clean_dataframe(df)
    else:
        # Create empty table if CSV doesn't exist
        df = pd.DataFrame(columns=sanitized_cols)
        print(f"ℹ️ No CSV file found, creating empty table structure")
    
    # Create table
    if not create_table_safe(table_name, sanitized_cols, col_types):
        return False
    
    # Insert data (even if empty)
    return insert_data_safe(table_name, df, sanitized_cols)


def migrate_unstructured_csv(csv_path: str) -> bool:
    """Migrate CSV file without create.sql structure."""
    folder_name = os.path.basename(os.path.dirname(csv_path))
    table_name = f"SAP_{folder_name.upper()}"
    
    print(f"\n📦 Processing CSV file: {os.path.basename(csv_path)}")
    print(f"➡️ Target table: {table_name}")
    
    # Read CSV
    df, used_encoding = safe_read_csv(csv_path)
    if df is None:
        print(f"❌ Failed to read CSV: {csv_path}")
        return False
    
    if used_encoding:
        print(f"✔ CSV loaded using encoding: {used_encoding}")
    
    # Sanitize column names
    df.columns = [sanitize_column(col) for col in df.columns]
    
    # Clean dataframe
    df = clean_dataframe(df)
    
    # Detect column types
    col_types = [detect_nullable_type(df[col]) for col in df.columns]
    
    # Create table (even if empty)
    if not create_table_safe(table_name, df.columns.tolist(), col_types):
        return False
    
    # Insert data
    return insert_data_safe(table_name, df, df.columns.tolist())


# ---------------- DIRECT HANA CONNECTION MIGRATION ----------------

def migrate_from_hana_direct(hana_config: Dict,
                            clickhouse_config: Optional[Dict] = None,
                            target_database: Optional[str] = None,
                            schemas_filter: Optional[List[str]] = None,
                            max_tables: Optional[int] = None) -> Dict:
    """
    Migrate directly from HANA database to ClickHouse.
    
    Args:
        hana_config: HANA connection config with 'host', 'port', 'username', 'password'
        clickhouse_config: ClickHouse connection config (default: from .env)
        target_database: Target database name (default: from .env)
        schemas_filter: List of schema names to migrate (None = all)
        max_tables: Maximum number of tables to migrate (None = all)
    
    Returns:
        dict with migration statistics
    """
    global client
    
    # Check if hdbcli is available
    try:
        import hdbcli.dbapi as hana_dbapi
    except ImportError:
        error_msg = "hdbcli library not installed. Cannot perform direct HANA migration."
        print(f"❌ {error_msg}")
        return {'success': False, 'error': error_msg, 'processed': 0, 'successful': 0, 'failed': 0}
    
    # Get ClickHouse configuration
    if clickhouse_config is None:
        clickhouse_config = get_clickhouse_config()
    
    if target_database:
        clickhouse_config['database'] = target_database
    
    # Connect to HANA
    try:
        hana_conn = hana_dbapi.connect(
            address=hana_config['host'],
            port=int(hana_config['port']),
            user=hana_config['username'],
            password=hana_config.get('password', ''),
            encrypt=True,
            sslValidateCertificate=False
        )
        print(f"✅ Connected to HANA: {hana_config['host']}:{hana_config['port']}")
    except Exception as e:
        error_msg = f"❌ Failed to connect to HANA: {e}"
        print(error_msg)
        return {'success': False, 'error': error_msg, 'processed': 0, 'successful': 0, 'failed': 0}
    
    # Connect to ClickHouse
    try:
        client = get_client(
            host=clickhouse_config['host'],
            port=clickhouse_config.get('port', 9000),
            username=clickhouse_config['username'],
            password=clickhouse_config['password'],
            database=clickhouse_config['database']
        )
        print(f"✅ Connected to ClickHouse: {clickhouse_config['host']}/{clickhouse_config['database']}")
    except Exception as e:
        error_msg = f"❌ Failed to connect to ClickHouse: {e}"
        print(error_msg)
        hana_conn.close()
        return {'success': False, 'error': error_msg, 'processed': 0, 'successful': 0, 'failed': 0}
    
    processed_count = 0
    success_count = 0
    error_count = 0
    total_rows = 0
    
    try:
        # Get schemas from HANA
        cursor = hana_conn.cursor()
        cursor.execute("""
            SELECT SCHEMA_NAME 
            FROM SYS.SCHEMAS 
            WHERE SCHEMA_NAME NOT IN ('SYS', '_SYS_BI', '_SYS_BIC', '_SYS_EPM', '_SYS_REPO', '_SYS_STATISTICS', 'SYSTEM')
            ORDER BY SCHEMA_NAME
        """)
        all_schemas = [row[0] for row in cursor.fetchall()]
        cursor.close()
        
        if schemas_filter:
            schemas = [s for s in all_schemas if s in schemas_filter]
        else:
            schemas = all_schemas
        
        print(f"\n🚀 Starting HANA to ClickHouse migration")
        print(f"📊 Found {len(schemas)} schema(s) to process\n")
        
        # Process each schema
        for schema in schemas:
            print(f"\n📦 Processing schema: {schema}")
            
            # Get tables in schema
            cursor = hana_conn.cursor()
            cursor.execute("""
                SELECT TABLE_NAME, TABLE_TYPE
                FROM SYS.TABLES
                WHERE SCHEMA_NAME = ?
                ORDER BY TABLE_NAME
            """, (schema,))
            tables = [{'name': row[0], 'type': row[1]} for row in cursor.fetchall()]
            cursor.close()
            
            if not tables:
                print(f"  ℹ️ No tables found in schema {schema}")
                continue
            
            print(f"  Found {len(tables)} table(s)")
            
            # Process each table
            for table_info in tables:
                if max_tables and processed_count >= max_tables:
                    print(f"\n⚠️ Reached max_tables limit ({max_tables})")
                    break
                
                table = table_info['name']
                table_type = table_info['type']
                
                # Skip views
                if table_type != 'TABLE':
                    continue
                
                processed_count += 1
                table_name = f"HANA_{schema}_{table}".upper().replace(' ', '_')
                
                print(f"\n  📋 Processing table: {schema}.{table} ({processed_count})")
                
                try:
                    # Get table schema
                    cursor = hana_conn.cursor()
                    cursor.execute("""
                        SELECT COLUMN_NAME, DATA_TYPE_NAME, LENGTH, SCALE, IS_NULLABLE
                        FROM SYS.TABLE_COLUMNS
                        WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?
                        ORDER BY POSITION
                    """, (schema, table))
                    
                    columns = []
                    col_types = []
                    for row in cursor.fetchall():
                        col_name = sanitize_column(row[0])
                        hana_type = row[1]
                        length = row[2]
                        scale = row[3]
                        nullable = row[4] == 'TRUE'
                        
                        columns.append(col_name)
                        ch_type = map_hana_to_clickhouse(hana_type)
                        if length and 'DECIMAL' in hana_type.upper() and scale:
                            ch_type = f'Nullable(Decimal({length},{scale}))'
                        col_types.append(ch_type)
                    cursor.close()
                    
                    if not columns:
                        print(f"  ⚠️ No columns found for {schema}.{table}")
                        error_count += 1
                        continue
                    
                    # Create ClickHouse table
                    if not create_table_safe(table_name, columns, col_types):
                        error_count += 1
                        continue
                    
                    # Migrate data
                    import pandas as pd
                    query = f'SELECT * FROM "{schema}"."{table}"'
                    df = pd.read_sql(query, hana_conn)
                    
                    if df.empty:
                        print(f"  ℹ️ Table {schema}.{table} is empty - structure created")
                        success_count += 1
                    else:
                        # Clean column names to match
                        df.columns = [sanitize_column(col) for col in df.columns]
                        df = clean_dataframe(df)
                        
                        if insert_data_safe(table_name, df, columns):
                            total_rows += len(df)
                            success_count += 1
                        else:
                            error_count += 1
                
                except Exception as e:
                    print(f"  ❌ Error processing {schema}.{table}: {e}")
                    error_count += 1
        
        print(f"\n{'='*60}")
        print(f"🎉 Migration Summary:")
        print(f"   Total tables processed: {processed_count}")
        print(f"   ✅ Successful: {success_count}")
        print(f"   ❌ Failed: {error_count}")
        print(f"   📊 Total rows migrated: {total_rows:,}")
        print(f"{'='*60}\n")
        
        return {
            'success': True,
            'processed': processed_count,
            'successful': success_count,
            'failed': error_count,
            'total_rows': total_rows
        }
    
    except Exception as e:
        error_msg = f"Migration error: {e}"
        print(f"❌ {error_msg}")
        return {'success': False, 'error': error_msg, 'processed': processed_count, 
                'successful': success_count, 'failed': error_count}
    finally:
        hana_conn.close()


# ---------------- MAIN PROCESS ----------------

def migrate_from_exported_files(base_dir: Optional[str] = None, 
                                  clickhouse_config: Optional[Dict] = None,
                                  target_database: Optional[str] = None) -> Dict:
    """
    Migrate HANA exported files to ClickHouse.
    
    Args:
        base_dir: Directory containing exported HANA files (default: from .env)
        clickhouse_config: ClickHouse connection config (default: from .env)
        target_database: Target database name (default: from .env)
    
    Returns:
        dict with migration statistics
    """
    global client
    
    # Get configuration
    if clickhouse_config is None:
        clickhouse_config = get_clickhouse_config()
    
    if target_database:
        clickhouse_config['database'] = target_database
    
    if base_dir is None:
        base_dir = get_base_dir()
    
    # Connect to ClickHouse
    try:
        client = get_client(
            host=clickhouse_config['host'],
            port=clickhouse_config.get('port', 9000),
            username=clickhouse_config['username'],
            password=clickhouse_config['password'],
            database=clickhouse_config['database']
        )
        print(f"✅ Connected to ClickHouse: {clickhouse_config['host']}/{clickhouse_config['database']}")
    except Exception as e:
        error_msg = f"❌ Failed to connect to ClickHouse: {e}"
        print(error_msg)
        return {'success': False, 'error': error_msg, 'processed': 0, 'successful': 0, 'failed': 0}
    
    if not os.path.exists(base_dir):
        error_msg = f"❌ Base directory does not exist: {base_dir}"
        print(error_msg)
        return {'success': False, 'error': error_msg, 'processed': 0, 'successful': 0, 'failed': 0}
    
    print(f"\n🚀 Starting HANA to ClickHouse migration")
    print(f"📁 Source directory: {base_dir}")
    print(f"🎯 Target database: {clickhouse_config['database']}\n")
    
    processed_count = 0
    success_count = 0
    error_count = 0
    
    # First pass: Process structured tables (with create.sql)
    for subdir in os.listdir(base_dir):
        subdir_path = os.path.join(base_dir, subdir)
        if not os.path.isdir(subdir_path):
            continue
        
        for table_dir in os.listdir(subdir_path):
            table_path = os.path.join(subdir_path, table_dir)
            if not os.path.isdir(table_path):
                continue
            
            processed_count += 1
            if migrate_structured_table(table_path, table_dir):
                success_count += 1
            else:
                error_count += 1
    
    # Second pass: Process any remaining CSV files (without create.sql)
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.lower().endswith('.csv') and file.lower() != 'data.csv':
                csv_path = os.path.join(root, file)
                processed_count += 1
                if migrate_unstructured_csv(csv_path):
                    success_count += 1
                else:
                    error_count += 1
    
    # Summary
    print(f"\n{'='*60}")
    print(f"🎉 Migration Summary:")
    print(f"   Total tables processed: {processed_count}")
    print(f"   ✅ Successful: {success_count}")
    print(f"   ❌ Failed: {error_count}")
    print(f"{'='*60}\n")
    
    return {
        'success': True,
        'processed': processed_count,
        'successful': success_count,
        'failed': error_count
    }


def main():
    """Main migration process (CLI entry point)."""
    result = migrate_from_exported_files()
    if not result.get('success'):
        sys.exit(1)


if __name__ == "__main__":
    main()

