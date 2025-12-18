"""
Credential Manager - Stores and retrieves credentials from PostgreSQL database
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import json
from datetime import datetime

logger = logging.getLogger(__name__)

def setup_jarvis_cred_database(pg_host, pg_port, pg_username, pg_password, pg_database='postgres', create_db=True):
    """
    Create Jarvis_cred database and table if they don't exist
    If create_db=False, assumes database already exists and only creates the table
    """
    try:
        # If create_db is True, try to create the database
        if create_db:
            try:
                # Connect to default postgres database to create new database
                conn = psycopg2.connect(
                    host=pg_host,
                    port=pg_port,
                    database=pg_database,  # Connect to default postgres database
                    user=pg_username,
                    password=pg_password
                )
                conn.autocommit = True
                cursor = conn.cursor()
                
                # Create jarvis_cred database if it doesn't exist (try both cases)
                cursor.execute("SELECT 1 FROM pg_database WHERE datname IN ('jarvis_cred', 'Jarvis_cred')")
                if not cursor.fetchone():
                    cursor.execute("CREATE DATABASE jarvis_cred")
                    logger.info("Created database: jarvis_cred")
                else:
                    logger.info("Database jarvis_cred already exists")
                
                cursor.close()
                conn.close()
            except Exception as e:
                logger.warning(f"Could not create database (may already exist or insufficient permissions): {str(e)}")
                logger.info("Attempting to connect to existing Jarvis_cred database...")
        
        # Now connect to jarvis_cred database to create table
        # Try both lowercase and mixed case
        db_names = ['jarvis_cred', 'Jarvis_cred']
        conn = None
        
        for db_name in db_names:
            try:
                conn = psycopg2.connect(
                    host=pg_host,
                    port=pg_port,
                    database=db_name,
                    user=pg_username,
                    password=pg_password
                )
                logger.info(f"Connected to database: {db_name}")
                break
            except psycopg2.OperationalError as e:
                if db_name == db_names[-1]:  # Last attempt
                    error_msg = (
                        f"Database 'jarvis_cred' or 'Jarvis_cred' does not exist. "
                        "Please create it manually first using: CREATE DATABASE jarvis_cred;"
                    )
                    logger.error(error_msg)
                    raise Exception(error_msg) from e
                continue
        
        cursor = conn.cursor()
        
        # Create Jarvis_cred table if it doesn't exist
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS Jarvis_cred (
            id SERIAL PRIMARY KEY,
            connection_type VARCHAR(50) NOT NULL,
            pg_host VARCHAR(255),
            pg_port INTEGER,
            pg_database VARCHAR(255),
            pg_username VARCHAR(255),
            pg_password VARCHAR(255),
            ch_host VARCHAR(255),
            ch_user VARCHAR(255),
            ch_password VARCHAR(255),
            ch_database VARCHAR(255),
            access_token TEXT,
            organization VARCHAR(255),
            refresh_token TEXT,
            client_id VARCHAR(255),
            client_secret TEXT,
            api_domain VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(connection_type)
        )
        """
        
        cursor.execute(create_table_sql)
        conn.commit()
        
        # Add Zoho columns if they don't exist (for existing tables)
        try:
            cursor.execute("""
                DO $$ 
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                   WHERE table_name='Jarvis_cred' AND column_name='refresh_token') THEN
                        ALTER TABLE Jarvis_cred ADD COLUMN refresh_token TEXT;
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                   WHERE table_name='Jarvis_cred' AND column_name='client_id') THEN
                        ALTER TABLE Jarvis_cred ADD COLUMN client_id VARCHAR(255);
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                   WHERE table_name='Jarvis_cred' AND column_name='client_secret') THEN
                        ALTER TABLE Jarvis_cred ADD COLUMN client_secret TEXT;
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                   WHERE table_name='Jarvis_cred' AND column_name='api_domain') THEN
                        ALTER TABLE Jarvis_cred ADD COLUMN api_domain VARCHAR(255);
                    END IF;
                END $$;
            """)
            conn.commit()
        except Exception as e:
            logger.warning(f"Could not add Zoho columns (may already exist): {str(e)}")
        cursor.execute(create_table_sql)
        conn.commit()
        logger.info("Created/verified table: Jarvis_cred")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Error setting up Jarvis_cred database: {str(e)}")
        raise

def save_credentials_to_db(pg_host, pg_port, pg_username, pg_password, connection_type, credentials):
    """
    Save credentials to jarvis_cred table
    """
    try:
        # Try both lowercase and mixed case database names
        db_names = ['jarvis_cred', 'Jarvis_cred']
        conn = None
        
        for db_name in db_names:
            try:
                conn = psycopg2.connect(
                    host=pg_host,
                    port=pg_port,
                    database=db_name,
                    user=pg_username,
                    password=pg_password
                )
                break
            except psycopg2.OperationalError:
                if db_name == db_names[-1]:
                    raise
                continue
        cursor = conn.cursor()
        
        # Prepare data for insertion/update
        if connection_type == 'postgres':
            insert_sql = """
            INSERT INTO Jarvis_cred (
                connection_type, pg_host, pg_port, pg_database, pg_username, pg_password, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (connection_type) 
            DO UPDATE SET 
                pg_host = EXCLUDED.pg_host,
                pg_port = EXCLUDED.pg_port,
                pg_database = EXCLUDED.pg_database,
                pg_username = EXCLUDED.pg_username,
                pg_password = EXCLUDED.pg_password,
                updated_at = CURRENT_TIMESTAMP
            """
            cursor.execute(insert_sql, (
                connection_type,
                credentials.get('pg_host'),
                credentials.get('pg_port', 5432),
                credentials.get('pg_database'),
                credentials.get('pg_username'),
                credentials.get('pg_password')
            ))
        
        elif connection_type == 'clickhouse':
            insert_sql = """
            INSERT INTO Jarvis_cred (
                connection_type, ch_host, ch_user, ch_password, ch_database, updated_at
            ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (connection_type) 
            DO UPDATE SET 
                ch_host = EXCLUDED.ch_host,
                ch_user = EXCLUDED.ch_user,
                ch_password = EXCLUDED.ch_password,
                ch_database = EXCLUDED.ch_database,
                updated_at = CURRENT_TIMESTAMP
            """
            cursor.execute(insert_sql, (
                connection_type,
                credentials.get('ch_host'),
                credentials.get('ch_user'),
                credentials.get('ch_password'),
                credentials.get('ch_database')
            ))
        
        elif connection_type == 'devops':
            insert_sql = """
            INSERT INTO Jarvis_cred (
                connection_type, access_token, organization, updated_at
            ) VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (connection_type) 
            DO UPDATE SET 
                access_token = EXCLUDED.access_token,
                organization = EXCLUDED.organization,
                updated_at = CURRENT_TIMESTAMP
            """
            cursor.execute(insert_sql, (
                connection_type,
                credentials.get('access_token'),
                credentials.get('organization')
            ))
        
        elif connection_type == 'zoho':
            insert_sql = """
            INSERT INTO Jarvis_cred (
                connection_type, refresh_token, client_id, client_secret, api_domain, updated_at
            ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (connection_type) 
            DO UPDATE SET 
                refresh_token = EXCLUDED.refresh_token,
                client_id = EXCLUDED.client_id,
                client_secret = EXCLUDED.client_secret,
                api_domain = EXCLUDED.api_domain,
                updated_at = CURRENT_TIMESTAMP
            """
            cursor.execute(insert_sql, (
                connection_type,
                credentials.get('refresh_token'),
                credentials.get('client_id'),
                credentials.get('client_secret'),
                credentials.get('api_domain', 'https://www.zohoapis.in')
            ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Saved credentials for {connection_type} to Jarvis_cred database")
        return True
        
    except Exception as e:
        logger.error(f"Error saving credentials to database: {str(e)}")
        raise

def get_credentials_from_db(pg_host, pg_port, pg_username, pg_password, connection_type):
    """
    Retrieve credentials from Jarvis_cred table
    """
    try:
        # Try both lowercase and mixed case database names
        db_names = ['jarvis_cred', 'Jarvis_cred']
        conn = None
        
        for db_name in db_names:
            try:
                conn = psycopg2.connect(
                    host=pg_host,
                    port=pg_port,
                    database=db_name,
                    user=pg_username,
                    password=pg_password
                )
                break
            except psycopg2.OperationalError:
                if db_name == db_names[-1]:
                    raise
                continue
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute(
            "SELECT * FROM Jarvis_cred WHERE connection_type = %s",
            (connection_type,)
        )
        
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if result:
            # Convert to regular dict
            return dict(result)
        return None
        
    except Exception as e:
        logger.error(f"Error retrieving credentials from database: {str(e)}")
        raise

def get_all_migration_credentials(pg_host, pg_port, pg_username, pg_password):
    """
    Get both PostgreSQL and ClickHouse credentials for migration
    Returns a tuple (pg_credentials, ch_credentials)
    """
    pg_creds = get_credentials_from_db(pg_host, pg_port, pg_username, pg_password, 'postgres')
    ch_creds = get_credentials_from_db(pg_host, pg_port, pg_username, pg_password, 'clickhouse')
    
    return pg_creds, ch_creds

def setup_migration_history_table(pg_host, pg_port, pg_username, pg_password):
    """
    Create migration_history table if it doesn't exist
    """
    try:
        # Try both lowercase and mixed case database names
        db_names = ['jarvis_cred', 'Jarvis_cred']
        conn = None
        
        for db_name in db_names:
            try:
                conn = psycopg2.connect(
                    host=pg_host,
                    port=pg_port,
                    database=db_name,
                    user=pg_username,
                    password=pg_password
                )
                break
            except psycopg2.OperationalError:
                if db_name == db_names[-1]:
                    raise
                continue
        
        cursor = conn.cursor()
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS migration_history (
            id SERIAL PRIMARY KEY,
            migration_id VARCHAR(255) NOT NULL UNIQUE,
            migration_type VARCHAR(50) NOT NULL,
            status VARCHAR(20) NOT NULL,
            started_at TIMESTAMP NOT NULL,
            completed_at TIMESTAMP,
            progress INTEGER DEFAULT 0,
            message TEXT,
            error TEXT,
            result JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        cursor.execute(create_table_sql)
        conn.commit()
        logger.info("Created/verified table: migration_history")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Error setting up migration_history table: {str(e)}")
        raise

def save_migration_history(pg_host, pg_port, pg_username, pg_password, migration_data):
    """
    Save migration history to database
    migration_data should contain: migration_id, type, status, started_at, completed_at, progress, message, error, result
    """
    try:
        # Ensure table exists
        setup_migration_history_table(pg_host, pg_port, pg_username, pg_password)
        
        # Try both lowercase and mixed case database names
        db_names = ['jarvis_cred', 'Jarvis_cred']
        conn = None
        
        for db_name in db_names:
            try:
                conn = psycopg2.connect(
                    host=pg_host,
                    port=pg_port,
                    database=db_name,
                    user=pg_username,
                    password=pg_password
                )
                break
            except psycopg2.OperationalError:
                if db_name == db_names[-1]:
                    raise
                continue
        
        cursor = conn.cursor()
        
        # Convert result to JSON string if it's a dict
        result_json = None
        if migration_data.get('result'):
            result_json = json.dumps(migration_data.get('result'))
        
        insert_sql = """
        INSERT INTO migration_history (
            migration_id, migration_type, status, started_at, completed_at, 
            progress, message, error, result
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (migration_id) DO UPDATE
        SET status = EXCLUDED.status,
            completed_at = EXCLUDED.completed_at,
            progress = EXCLUDED.progress,
            message = EXCLUDED.message,
            error = EXCLUDED.error,
            result = EXCLUDED.result
        """
        
        cursor.execute(insert_sql, (
            migration_data.get('migration_id'),
            migration_data.get('type') or migration_data.get('migration_type'),
            migration_data.get('status'),
            migration_data.get('started_at'),
            migration_data.get('completed_at'),
            migration_data.get('progress', 0),
            migration_data.get('message'),
            migration_data.get('error'),
            result_json
        ))
        
        conn.commit()
        logger.info(f"Saved migration history: {migration_data.get('migration_id')}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Error saving migration history: {str(e)}")
        raise

def get_migration_history(pg_host, pg_port, pg_username, pg_password, limit=100):
    """
    Retrieve migration history from database
    Returns list of migration records ordered by started_at DESC
    """
    try:
        # Try both lowercase and mixed case database names
        db_names = ['jarvis_cred', 'Jarvis_cred']
        conn = None
        
        for db_name in db_names:
            try:
                conn = psycopg2.connect(
                    host=pg_host,
                    port=pg_port,
                    database=db_name,
                    user=pg_username,
                    password=pg_password
                )
                break
            except psycopg2.OperationalError:
                if db_name == db_names[-1]:
                    raise
                continue
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("""
            SELECT * FROM migration_history 
            ORDER BY started_at DESC 
            LIMIT %s
        """, (limit,))
        
        results = cursor.fetchall()
        
        # Convert to list of dicts and parse JSON result
        history = []
        for row in results:
            record = dict(row)
            # Parse JSON result if present
            if record.get('result') and isinstance(record.get('result'), str):
                try:
                    record['result'] = json.loads(record['result'])
                except:
                    pass
            # Convert datetime objects to ISO format strings for template compatibility
            if record.get('started_at') and hasattr(record.get('started_at'), 'isoformat'):
                record['started_at'] = record['started_at'].isoformat()
            if record.get('completed_at') and hasattr(record.get('completed_at'), 'isoformat'):
                record['completed_at'] = record['completed_at'].isoformat()
            history.append(record)
        
        cursor.close()
        conn.close()
        
        logger.info(f"Retrieved {len(history)} migration history records")
        return history
        
    except Exception as e:
        logger.error(f"Error retrieving migration history: {str(e)}")
        # If table doesn't exist, return empty list
        if 'does not exist' in str(e).lower():
            return []
        raise

