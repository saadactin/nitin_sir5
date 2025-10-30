import psycopg2
import os
import yaml
import json
import bcrypt
from cryptography.fernet import Fernet
import base64
import hashlib
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Path to YAML config
CONFIG_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "config/db_connections.yaml")
)

# Secret key for encryption (prefer environment variable)
SECRET_KEY = os.environ.get('SECRET_KEY', os.environ.get('SECRET', None) or "NITIN_SIR")

def get_encryption_key():
    """Generate encryption key from secret"""
    key = hashlib.sha256(SECRET_KEY.encode()).digest()
    return base64.urlsafe_b64encode(key)

def encrypt_password(password):
    """Encrypt password using the secret key"""
    f = Fernet(get_encryption_key())
    return f.encrypt(password.encode()).decode()

def decrypt_password(encrypted_password):
    """Decrypt password using the secret key"""
    try:
        f = Fernet(get_encryption_key())
        return f.decrypt(encrypted_password.encode()).decode()
    except Exception as e:
        logger.error(f"Error decrypting password: {e}")
        return None

def load_pg_config():
    """Load PostgreSQL config from YAML"""
    # First try to load from database
    try:
        conn = get_pg_connection_without_config()
        if conn:
            pg_config = get_connection_from_db('postgresql')
            if pg_config:
                # Validate required fields are present and non-empty
                required_keys = ['database', 'username', 'password', 'host', 'port']
                if all(pg_config.get(k) not in (None, "") for k in required_keys):
                    return pg_config
                else:
                    logger.warning("PostgreSQL config loaded from DB is incomplete (likely password decryption failed); falling back to YAML")
    except Exception as e:
        logger.warning(f"Failed to load PostgreSQL config from DB: {e}")
    
    # Fallback to YAML if DB loading fails
    try:
        with open(CONFIG_PATH, "r") as f:
            config = yaml.safe_load(f) or {}
        # Return an empty dict if postgres section is missing so callers can
        # detect missing keys explicitly instead of using silent hardcoded
        # fallbacks.
        return config.get("postgresql", {})
    except FileNotFoundError:
        logger.error(f"Config file not found at {CONFIG_PATH}")
        return {}
    except Exception as e:
        logger.error(f"Error loading YAML config {CONFIG_PATH}: {e}")
        return {}


def get_pg_connection_without_config():
    """Try to connect using environment variables or default values"""
    try:
        # Prefer environment variables, then YAML config defaults, then hardcoded fallbacks.
        yaml_defaults = {}
        try:
            with open(CONFIG_PATH, 'r') as f:
                cfg = yaml.safe_load(f) or {}
                if isinstance(cfg, dict):
                    yaml_defaults = cfg.get('postgresql', {}) or {}
        except Exception:
            yaml_defaults = {}

        def _env_yaml(name_variants, yaml_key):
            # name_variants: list of env var names to try in order
            for n in name_variants:
                v = os.environ.get(n)
                if v is not None and v != "":
                    return v
            # then YAML
            v = yaml_defaults.get(yaml_key)
            if v is not None and v != "":
                return v
            # Do not return hardcoded fallbacks — caller should decide how to handle missing keys
            return None
        # Also support PG_* environment variables as primary source for sensitive values
        dbname = os.environ.get('PG_DATABASE') or _env_yaml(["POSTGRES_DB", "PG_DB"], 'database')
        user = os.environ.get('PG_USERNAME') or _env_yaml(["POSTGRES_USER"], 'username')
        password = os.environ.get('PG_PASSWORD') or _env_yaml(["POSTGRES_PASSWORD"], 'password')
        host = os.environ.get('PG_HOST') or _env_yaml(["POSTGRES_HOST"], 'host')
        port_raw = os.environ.get('PG_PORT') or _env_yaml(["POSTGRES_PORT"], 'port')

        # If any required value is missing, don't attempt a connection here.
        missing = [k for k, v in (('database', dbname), ('username', user), ('password', password), ('host', host), ('port', port_raw)) if not v]
        if missing:
            logger.warning(f"Postgres connection parameters missing (env or YAML): {missing}. Skipping connection attempt.")
            return None

        # coerce port to int when possible (psycopg2 accepts int or str)
        try:
            port = int(port_raw)
        except Exception:
            port = port_raw

        return psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
        )
    except Exception as e:
        # Log non-sensitive connection details to aid troubleshooting
        try:
            _host = locals().get('host', 'unknown')
            _port = locals().get('port_raw', locals().get('port', 'unknown'))
            _user = locals().get('user', 'unknown')
            # Mask sensitive information in logs
            masked_user = _user[:2] + '*' * (len(_user) - 2) if _user and len(_user) > 2 else '***'
            logger.error(f"Failed to connect to PostgreSQL without config: {e} (host={_host}, port={_port}, user={masked_user})")
        except Exception:
            logger.error(f"Failed to connect to PostgreSQL without config: {e}")
        return None

def get_pg_connection():
    """Return a live PostgreSQL connection"""
    try:
        conf = load_pg_config()
        # If YAML/DB didn't provide a password, try environment variables directly as a last resort
        password = conf.get("password") or os.environ.get('PG_PASSWORD') or os.environ.get('POSTGRES_PASSWORD')
        if not password:
            raise RuntimeError("PostgreSQL password not supplied. Set PG_PASSWORD or POSTGRES_PASSWORD in .env, or provide in YAML.")
        return psycopg2.connect(
            dbname=conf["database"],
            user=conf["username"],
            password=password,
            host=conf["host"],
            port=conf["port"],
        )
    except psycopg2.OperationalError as e:
        error_msg = str(e)
        if 'database' in error_msg and 'does not exist' in error_msg:
            # Extract database name from config
            try:
                conf = load_pg_config()
                db_name = conf.get("database", "unknown")
                logger.error(f"\n{'='*70}")
                logger.error(f"DATABASE ERROR: PostgreSQL database '{db_name}' does not exist!")
                logger.error(f"{'='*70}")
                logger.error(f"SOLUTION: Run this command to create the database:")
                logger.error(f"  python create_database.py")
                logger.error(f"{'='*70}\n")
            except:
                pass
        raise

def get_connection_from_db(connection_type):
    """Load connection configuration from database"""
    try:
        conn = get_pg_connection_without_config()
        if not conn:
            return None
        
        cur = conn.cursor()
        cur.execute("""
            SELECT config_data FROM metrics_sync_tables.connections 
            WHERE connection_type = %s
        """, (connection_type,))
        
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        if row:
            config = json.loads(row[0])
            # Decrypt password if present
            if 'password' in config:
                config['password'] = decrypt_password(config['password'])
            return config
        return None
    except Exception as e:
        logger.error(f"Error retrieving connection from DB: {e}")
        return None


def init_pg_schema():
    """Create schema + all required tables if not exists"""
    try:
        conn = get_pg_connection_without_config()
        if not conn:
            conn = get_pg_connection()
    except:
        try:
            # Fallback to standard config
            conn = get_pg_connection()
        except Exception as e:
            logger.error(f"Failed to create database schema: {e}")
            return False
            
    cur = conn.cursor()

    # Create schema
    cur.execute("CREATE SCHEMA IF NOT EXISTS metrics_sync_tables;")

    # Create schedules table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS metrics_sync_tables.schedules (
            id SERIAL PRIMARY KEY,
            server_name TEXT NOT NULL,
            job_type TEXT NOT NULL,
            -- scheduling fields
            minutes INTEGER,
            hour INTEGER,
            minute INTEGER,
            -- runtime/status fields
            last_run TIMESTAMP,
            status TEXT,
            error TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)

    # Ensure older installations get the new columns if the table existed prior
    try:
        cur.execute("ALTER TABLE metrics_sync_tables.schedules ADD COLUMN IF NOT EXISTS minutes INTEGER;")
        cur.execute("ALTER TABLE metrics_sync_tables.schedules ADD COLUMN IF NOT EXISTS hour INTEGER;")
        cur.execute("ALTER TABLE metrics_sync_tables.schedules ADD COLUMN IF NOT EXISTS minute INTEGER;")
    except Exception:
        # Non-fatal: some PG versions may behave differently; ignore failures here
        pass

    # Create a unique index on (server_name, job_type) so ON CONFLICT clauses work
    try:
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_schedules_server_job ON metrics_sync_tables.schedules (server_name, job_type);")
    except Exception:
        # ignore if index cannot be created
        pass

    # Create sync_history table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS metrics_sync_tables.sync_history (
            id SERIAL PRIMARY KEY,
            server_name TEXT NOT NULL,
            sync_time TIMESTAMP DEFAULT NOW(),
            status TEXT NOT NULL,
            details TEXT
        );
    """)

    # Create users table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS metrics_sync_tables.users (
            id SERIAL PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            role TEXT NOT NULL CHECK (role IN ('admin', 'operator', 'viewer')),
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)
    
    # Create connections table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS metrics_sync_tables.connections (
            id SERIAL PRIMARY KEY,
            connection_type TEXT NOT NULL,
            connection_name TEXT NOT NULL,
            config_data TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(connection_type, connection_name)
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
    return True

def save_connection_to_db(connection_type, connection_name, config_data):
    """
    Save a connection configuration to the database
    
    Args:
        connection_type: 'postgresql' or 'sqlservers'
        connection_name: For postgresql use 'default', for sqlservers use server name
        config_data: Dictionary with connection details
    """
    try:
        # Create a copy to avoid modifying the original
        config_copy = dict(config_data)
        
        # Encrypt password if present
        if 'password' in config_copy:
            config_copy['password'] = encrypt_password(config_copy['password'])
        
        conn = get_pg_connection()
        cur = conn.cursor()
        
        # Convert config dictionary to JSON string
        config_json = json.dumps(config_copy)
        
        # Insert or update the connection
        cur.execute("""
            INSERT INTO metrics_sync_tables.connections 
                (connection_type, connection_name, config_data, updated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (connection_type, connection_name)
            DO UPDATE SET 
                config_data = EXCLUDED.config_data,
                updated_at = NOW()
        """, (connection_type, connection_name, config_json))
        
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error saving connection to database: {e}")
        return False

def delete_connection_from_db(connection_type, connection_name):
    """
    Delete a connection configuration from the database
    
    Args:
        connection_type: 'postgresql' or 'sqlservers'
        connection_name: Server name
    """
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        
        cur.execute("""
            DELETE FROM metrics_sync_tables.connections 
            WHERE connection_type = %s AND connection_name = %s
        """, (connection_type, connection_name))
        
        rows_deleted = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        return rows_deleted > 0
    except Exception as e:
        logger.error(f"Error deleting connection from database: {e}")
        return False

def get_all_connections(connection_type=None):
    """
    Get all connections of specified type or all connections if type is None
    
    Args:
        connection_type: Optional filter by 'postgresql' or 'sqlservers'
    """
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        
        if connection_type:
            cur.execute("""
                SELECT connection_type, connection_name, config_data 
                FROM metrics_sync_tables.connections
                WHERE connection_type = %s
            """, (connection_type,))
        else:
            cur.execute("""
                SELECT connection_type, connection_name, config_data 
                FROM metrics_sync_tables.connections
            """)
        
        results = []
        for row in cur.fetchall():
            conn_type, conn_name, config_json = row
            config = json.loads(config_json)
            
            # Don't decrypt passwords here for security
            # Only decrypt when actually using the connection
            
            results.append({
                'connection_type': conn_type,
                'connection_name': conn_name,
                'config': config
            })
        
        cur.close()
        conn.close()
        return results
    except Exception as e:
        logger.error(f"Error retrieving connections: {e}")
        return []


def load_clickhouse_config():
    """Load ClickHouse config from environment variables or YAML"""
    try:
        # First try environment variables
        host = os.environ.get('CLICKHOUSE_HOST')
        port = os.environ.get('CLICKHOUSE_PORT')
        user = os.environ.get('CLICKHOUSE_USER')
        password = os.environ.get('CLICKHOUSE_PASSWORD')
        
        if host:
            return {
                'host': host,
                'port': int(port) if port else 9000,
                'user': user or 'default',
                'password': password or ''
            }
        
        # Fallback to YAML
        with open(CONFIG_PATH, "r") as f:
            config = yaml.safe_load(f) or {}
        ch_config = config.get("clickhouse", {})
        
        return {
            'host': ch_config.get('host', 'localhost'),
            'port': int(ch_config.get('port', 9000)),
            'user': ch_config.get('user', 'default'),
            'password': ch_config.get('password', '')
        }
    except Exception as e:
        logger.error(f"Error loading ClickHouse config: {e}")
        return {
            'host': 'localhost',
            'port': 9000,
            'user': 'default',
            'password': ''
        }

