"""
Database migration to add Zoho OAuth fields to data_sources table
"""
import psycopg2
from db_utils import load_pg_config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def migrate_database():
    """Add OAuth-related columns to data_sources table"""
    try:
        pg_conf = load_pg_config()
        conn = psycopg2.connect(
            dbname=pg_conf.get('database'),
            user=pg_conf.get('username'),
            password=pg_conf.get('password'),
            host=pg_conf.get('host'),
            port=int(pg_conf.get('port', 5432))
        )
        cursor = conn.cursor()
        
        logger.info("Starting database migration for Zoho OAuth support...")
        
        # Add columns for Zoho OAuth token storage
        # These will be stored in connection_details JSONB, but we also add explicit columns for easier querying
        migrations = [
            # Check if columns already exist before adding
            """
            DO $$ 
            BEGIN
                -- Add oauth_refresh_token if not exists
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name='data_sources' AND column_name='oauth_refresh_token'
                ) THEN
                    ALTER TABLE data_sources ADD COLUMN oauth_refresh_token TEXT;
                    RAISE NOTICE 'Added oauth_refresh_token column';
                END IF;
                
                -- Add oauth_client_id if not exists
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name='data_sources' AND column_name='oauth_client_id'
                ) THEN
                    ALTER TABLE data_sources ADD COLUMN oauth_client_id TEXT;
                    RAISE NOTICE 'Added oauth_client_id column';
                END IF;
                
                -- Add oauth_client_secret if not exists
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name='data_sources' AND column_name='oauth_client_secret'
                ) THEN
                    ALTER TABLE data_sources ADD COLUMN oauth_client_secret TEXT;
                    RAISE NOTICE 'Added oauth_client_secret column';
                END IF;
                
                -- Add oauth_access_token if not exists
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name='data_sources' AND column_name='oauth_access_token'
                ) THEN
                    ALTER TABLE data_sources ADD COLUMN oauth_access_token TEXT;
                    RAISE NOTICE 'Added oauth_access_token column';
                END IF;
                
                -- Add oauth_token_expiry if not exists
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name='data_sources' AND column_name='oauth_token_expiry'
                ) THEN
                    ALTER TABLE data_sources ADD COLUMN oauth_token_expiry TIMESTAMP;
                    RAISE NOTICE 'Added oauth_token_expiry column';
                END IF;
                
                -- Add oauth_api_domain if not exists
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name='data_sources' AND column_name='oauth_api_domain'
                ) THEN
                    ALTER TABLE data_sources ADD COLUMN oauth_api_domain TEXT;
                    RAISE NOTICE 'Added oauth_api_domain column';
                END IF;
            END $$;
            """
        ]
        
        for migration in migrations:
            cursor.execute(migration)
            logger.info("Migration executed successfully")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info("✅ Database migration completed successfully!")
        logger.info("   Added columns: oauth_refresh_token, oauth_client_id, oauth_client_secret, oauth_access_token, oauth_token_expiry, oauth_api_domain")
        
    except Exception as e:
        logger.exception(f"❌ Migration failed: {e}")
        raise

if __name__ == "__main__":
    migrate_database()

