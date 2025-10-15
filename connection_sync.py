"""
This module handles synchronizing the YAML config file with the database.
It ensures that the connection data is persisted in the database while still allowing
the YAML file to be used as a fallback or initial configuration source.
"""

import os
import yaml
import json
from db_utils import (
    init_pg_schema,
    save_connection_to_db,
    delete_connection_from_db,
    encrypt_password,
    decrypt_password,
    get_all_connections
)

# Path to YAML config
CONFIG_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "config/db_connections.yaml")
)

def sync_yaml_to_db():
    """
    Sync the YAML configuration to the database on application startup.
    This ensures the DB has the latest connection information.
    """
    # First ensure the schema exists
    if not init_pg_schema():
        print("Failed to initialize database schema")
        return False
    
    try:
        # Load the YAML config
        with open(CONFIG_PATH, "r") as f:
            config = yaml.safe_load(f)
        
        # Store PostgreSQL connection
        if "postgresql" in config:
            save_connection_to_db("postgresql", "default", config["postgresql"])
        
        # Store SQL Server connections
        if "sqlservers" in config:
            for server_name, server_config in config["sqlservers"].items():
                save_connection_to_db("sqlservers", server_name, server_config)
        
        return True
    except Exception as e:
        print(f"Error syncing YAML to database: {e}")
        return False

def sync_db_to_yaml():
    """
    Write the database connection information back to the YAML file.
    This allows external tools to still use the YAML file.
    """
    try:
        config = {"postgresql": {}, "sqlservers": {}}
        
        # Get all connections from the database
        connections = get_all_connections()
        
        for conn in connections:
            conn_type = conn['connection_type']
            conn_name = conn['connection_name']
            conn_config = conn['config']
            
            # Decrypt passwords for writing to YAML
            if 'password' in conn_config:
                try:
                    conn_config['password'] = decrypt_password(conn_config['password'])
                except:
                    # If decryption fails, keep the encrypted password
                    pass
            
            if conn_type == 'postgresql' and conn_name == 'default':
                config['postgresql'] = conn_config
            elif conn_type == 'sqlservers':
                if 'sqlservers' not in config:
                    config['sqlservers'] = {}
                config['sqlservers'][conn_name] = conn_config
        
        # Write the config back to YAML
        with open(CONFIG_PATH, "w") as f:
            yaml.dump(config, f, default_flow_style=False)
        
        return True
    except Exception as e:
        print(f"Error syncing database to YAML: {e}")
        return False

def update_connection(conn_type, conn_name, config_data):
    """
    Update a connection in both the database and YAML file
    
    Args:
        conn_type: 'postgresql' or 'sqlservers'
        conn_name: For postgresql use 'default', for sqlservers use server name
        config_data: Dictionary with connection details
    """
    # Update in database
    if save_connection_to_db(conn_type, conn_name, config_data):
        # Update YAML file
        return sync_db_to_yaml()
    return False

def remove_connection(conn_type, conn_name):
    """
    Remove a connection from both the database and YAML file
    
    Args:
        conn_type: 'postgresql' or 'sqlservers'
        conn_name: Server name
    """
    # Remove from database
    if delete_connection_from_db(conn_type, conn_name):
        # Update YAML file
        return sync_db_to_yaml()
    return False