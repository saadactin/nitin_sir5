"""
Script to create the PostgreSQL database if it doesn't exist.
Run this before starting the application.
"""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import yaml

def create_database():
    # Read config
    with open('config/db_connections.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    pg_config = config['postgresql']
    database_name = pg_config['database']
    
    print(f"Attempting to create database: {database_name}")
    
    try:
        # Connect to 'postgres' database (always exists) to create new database
        conn = psycopg2.connect(
            dbname='postgres',  # Connect to default postgres database
            user=pg_config['username'],
            password=pg_config['password'],
            host=pg_config['host'],
            port=pg_config['port']
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{database_name}'")
        exists = cursor.fetchone()
        
        if exists:
            print(f"✓ Database '{database_name}' already exists!")
        else:
            # Create database
            cursor.execute(f'CREATE DATABASE "{database_name}"')
            print(f"✓ Database '{database_name}' created successfully!")
        
        cursor.close()
        conn.close()
        
        # Now connect to the new database and create schema
        conn = psycopg2.connect(
            dbname=database_name,
            user=pg_config['username'],
            password=pg_config['password'],
            host=pg_config['host'],
            port=pg_config['port']
        )
        cursor = conn.cursor()
        
        # Create metrics_sync_tables schema
        cursor.execute("CREATE SCHEMA IF NOT EXISTS metrics_sync_tables")
        print(f"✓ Schema 'metrics_sync_tables' created/verified!")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("\n✅ Database setup complete! You can now run 'python app.py'")
        
    except psycopg2.Error as e:
        print(f"\n❌ Error: {e}")
        print("\nPossible solutions:")
        print("1. Make sure PostgreSQL is running")
        print("2. Check username/password in config/db_connections.yaml")
        print("3. Ensure the user has permission to create databases")
        return False
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        return False
    
    return True

if __name__ == "__main__":
    print("="*60)
    print("PostgreSQL Database Setup")
    print("="*60)
    create_database()
