#!/usr/bin/env python
"""
SQL Server to PostgreSQL Synchronization Worker

This script runs the synchronization process for SQL Server to PostgreSQL,
handling all configured servers or a specific server if specified.

Usage:
  python run_sync_worker.py [--server SERVER_NAME] [--all] [--once] [--debug]

Options:
  --server SERVER_NAME  Run sync only for the specified server
  --all                 Run sync for all configured servers (default)
  --once                Run the sync once and exit (don't run as a scheduled job)
  --debug               Enable debug logging
  --databases DB1,DB2   Only sync specific databases (comma-separated list)

Examples:
  - Sync all servers continuously: python run_sync_worker.py
  - Sync only server1 once: python run_sync_worker.py --server server1 --once
  - Sync all servers with debug logs: python run_sync_worker.py --all --debug
  - Sync specific databases: python run_sync_worker.py --server server1 --databases db1,db2
"""

import sys
import os
import time
import argparse
import logging
from datetime import datetime
import yaml
import schedule

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('run_sync_worker.log'),
        logging.StreamHandler()
    ],
    force=True
)
logger = logging.getLogger()

# Import hybrid_sync functions
try:
    from hybrid_sync import sync_database_hybrid, get_all_sqlserver_databases
except ImportError:
    logging.error("Failed to import required modules from hybrid_sync.py.")
    logging.error("Make sure hybrid_sync.py is in the same directory.")
    sys.exit(1)

# Path to YAML config
CONFIG_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "config/db_connections.yaml")
)

def load_config():
    """Load configuration from YAML file"""
    try:
        with open(CONFIG_PATH, 'r') as f:
            return yaml.safe_load(f) or {"postgresql": {}, "sqlservers": {}}
    except Exception as e:
        logging.error(f"Failed to load configuration: {str(e)}")
        return {"postgresql": {}, "sqlservers": {}}

def sync_server(server_name, target_databases=None):
    """
    Synchronize a specific SQL Server to PostgreSQL
    
    Args:
        server_name (str): Name of the server from configuration
        target_databases (list): Optional list of specific databases to sync
    
    Returns:
        bool: True if sync succeeded, False otherwise
    """
    config = load_config()
    
    if server_name not in config.get('sqlservers', {}):
        logging.error(f"Server '{server_name}' not found in configuration")
        return False
    
    server_conf = config['sqlservers'][server_name]
    server = server_conf['server']
    
    logging.info(f"=== Starting sync for {server_name} ({server}) ===")
    start_time = datetime.now()
    
    try:
        # Get all databases on this server
        databases = get_all_sqlserver_databases(server_conf)
        
        # Filter databases if target_databases is provided
        if target_databases:
            databases = [db for db in databases if db in target_databases]
            logging.info(f"Filtered to {len(databases)} specified databases: {', '.join(databases)}")
        
        # Skip databases from configuration
        skip_databases = server_conf.get('skip_databases', [])
        if skip_databases:
            original_count = len(databases)
            databases = [db for db in databases if db not in skip_databases]
            logging.info(f"Skipping {original_count - len(databases)} databases as configured: {', '.join(skip_databases)}")
        
        if not databases:
            logging.warning(f"No databases to sync for {server_name}")
            return False
        
        # Log which databases will be synced
        logging.info(f"Will sync {len(databases)} databases from {server_name}: {', '.join(databases)}")
        
        # Sync each database
        for i, database in enumerate(databases):
            db_start = datetime.now()
            logging.info(f"[{i+1}/{len(databases)}] Starting sync for {database}")
            
            try:
                # Use the target_postgres_db from server config if available
                pg_database = server_conf.get('target_postgres_db', 'postgres')
                
                # Call the hybrid sync function
                sync_database_hybrid(
                    server_conf=server_conf,
                    database_name=database,
                    pg_database=pg_database
                )
                
                db_end = datetime.now()
                db_duration = (db_end - db_start).total_seconds()
                logging.info(f"Finished sync for {database} in {db_duration:.1f} seconds")
            
            except Exception as e:
                logging.error(f"Error syncing database {database}: {str(e)}")
        
        # Log completion
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logging.info(f"=== Completed sync for {server_name} ({server}) in {duration:.1f} seconds ===")
        return True
    
    except Exception as e:
        logging.error(f"Failed to sync server {server_name}: {str(e)}")
        return False

def sync_all_servers(target_databases=None):
    """
    Synchronize all configured SQL Servers to PostgreSQL
    
    Args:
        target_databases (list): Optional list of specific databases to sync
    """
    config = load_config()
    servers = config.get('sqlservers', {})
    
    if not servers:
        logging.warning("No SQL servers found in configuration!")
        return
    
    logging.info(f"Starting sync for all {len(servers)} configured servers")
    
    for server_name in servers:
        sync_server(server_name, target_databases)

def main():
    """Main function to parse arguments and run sync"""
    parser = argparse.ArgumentParser(description="SQL Server to PostgreSQL Synchronization Worker")
    parser.add_argument('--server', help='Run sync only for the specified server')
    parser.add_argument('--all', action='store_true', help='Run sync for all configured servers')
    parser.add_argument('--once', action='store_true', help='Run the sync once and exit')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--databases', help='Only sync specific databases (comma-separated list)')
    
    args = parser.parse_args()
    
    # Set debug logging if requested
    if args.debug:
        logger.setLevel(logging.DEBUG)
        for handler in logger.handlers:
            handler.setLevel(logging.DEBUG)
        logging.debug("Debug logging enabled")
    
    # Parse target databases if provided
    target_databases = None
    if args.databases:
        target_databases = [db.strip() for db in args.databases.split(',')]
        logging.info(f"Will only sync these databases: {', '.join(target_databases)}")
    
    # Run sync once if requested
    if args.once:
        if args.server:
            sync_server(args.server, target_databases)
        else:
            sync_all_servers(target_databases)
        return
    
    # Set up scheduled jobs
    if args.server:
        logging.info(f"Setting up scheduled sync for server: {args.server}")
        schedule.every(15).minutes.do(sync_server, args.server, target_databases)
        # Run immediately first time
        sync_server(args.server, target_databases)
    else:
        logging.info("Setting up scheduled sync for all servers")
        schedule.every(30).minutes.do(sync_all_servers, target_databases)
        # Run immediately first time
        sync_all_servers(target_databases)
    
    # Run the scheduler
    logging.info("Sync worker started. Press Ctrl+C to exit.")
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Sync worker stopped by user.")
    except Exception as e:
        logging.error(f"Error in sync worker: {str(e)}")

if __name__ == "__main__":
    main()
