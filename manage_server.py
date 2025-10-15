"""
Server Management Tool for SQL Server to PostgreSQL Sync

This tool helps manage SQL Server connections for the sync process.
Supports standard servers, named instances, and special handling for SQL2019_Second.

Usage:
  - List servers:     python manage_server.py --list
  - Add server:       python manage_server.py --add NAME SERVER USERNAME PASSWORD
  - Add interactive:  python manage_server.py --add-interactive
  - Test connection:  python manage_server.py --test NAME
  - Delete server:    python manage_server.py --delete NAME

Special Notes:
  - For named instances (server\\instance), port is automatically detected.
  - SQL2019_Second instance has special handling with direct port 14344.
  - Windows Authentication can be used with 'windows' as username and password.
"""

import yaml
import argparse
import os
import json

# For backward compatibility
CONFIG_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), 'config/db_connections.yaml')
)

OUTPUT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), 'data/sqlserver_exports/')
)

# Import the connection sync functions
try:
    from connection_sync import (
        sync_yaml_to_db,
        sync_db_to_yaml,
        update_connection,
        remove_connection
    )
    from db_utils import get_all_connections, decrypt_password
    USE_DB_STORAGE = True
except ImportError:
    USE_DB_STORAGE = False

def load_config():
    """Load configuration from database, falling back to YAML if needed"""
    if USE_DB_STORAGE:
        try:
            # Build config from database connections
            config = {"postgresql": {}, "sqlservers": {}}
            connections = get_all_connections()
            
            for conn in connections:
                conn_type = conn['connection_type']
                conn_name = conn['connection_name']
                conn_config = conn['config'].copy()  # Copy to avoid modifying original
                
                # Decrypt password for use
                if 'password' in conn_config:
                    try:
                        conn_config['password'] = decrypt_password(conn_config['password'])
                    except:
                        pass  # Keep encrypted if can't decrypt
                
                if conn_type == 'postgresql' and conn_name == 'default':
                    config['postgresql'] = conn_config
                elif conn_type == 'sqlservers':
                    config.setdefault('sqlservers', {})
                    config['sqlservers'][conn_name] = conn_config
            
            # If we got data from the DB, return it
            if config.get('postgresql') or config.get('sqlservers'):
                return config
        except Exception as e:
            print(f"Error loading from database, falling back to YAML: {e}")
    
    # Fallback to YAML
    if not os.path.exists(CONFIG_PATH):
        return {"postgresql": {}, "sqlservers": {}}
    with open(CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f) or {"postgresql": {}, "sqlservers": {}}

def save_config(config):
    """Save configuration to both database and YAML"""
    if USE_DB_STORAGE:
        try:
            # Save PostgreSQL connection
            if "postgresql" in config:
                update_connection("postgresql", "default", config["postgresql"])
            
            # Save SQL Server connections
            if "sqlservers" in config:
                for server_name, server_config in config["sqlservers"].items():
                    update_connection("sqlservers", server_name, server_config)
            
            # Also update the YAML file as backup
            sync_db_to_yaml()
            return
        except Exception as e:
            print(f"Error saving to database, falling back to YAML: {e}")
    
    # Fallback to YAML
    with open(CONFIG_PATH, 'w') as f:
        yaml.safe_dump(config, f, default_flow_style=False)

def list_servers():
    config = load_config()
    servers = config.get('sqlservers', {})
    if not servers:
        print("WARNING: No servers configured yet.")
        return
        
    print("\n=== Configured SQL Servers ===")
    for name, conf in servers.items():
        server = conf['server']
        auth_type = "Windows Auth" if conf['username'].lower() in ['windows', 'trusted', ''] else "SQL Auth"
        
        # Format server information
        if "\\" in server:  # Named instance
            print(f"{name} -> {server} (Named Instance, {auth_type})")
            if "SQL2019_Second" in server:
                print(f"  NOTE: SQL2019_Second uses special connection handling (direct port 14344)")
        elif 'port' in conf:  # Server with explicit port
            print(f"{name} -> {server}:{conf['port']} ({auth_type})")
        else:  # Default instance
            print(f"{name} -> {server} (Default Instance, {auth_type})")
        
        print(f"  Target PostgreSQL DB: {conf.get('target_postgres_db', 'postgres')}")
        print(f"  Sync Mode: {conf.get('sync_mode', 'hybrid')}")
        
    print("\nTo test connections: python manage_server.py --test <server_name>")
    return servers

def add_server(name, host, username, password, port=None, pg_database=None):
    config = load_config()
    config.setdefault('sqlservers', {})

    if name in config['sqlservers']:
        print(f"ERROR: Server name '{name}' already exists! Use a different name.")
        return

    # Check if this is a named instance (e.g., "server\instance")
    server_spec = host
    
    # Don't include port in server string if it's a named instance
    if "\\" in host and port:
        print(f"NOTE: Named instance detected ({host}). Port will be automatically determined.")
        port = None
    
    # Check if this is the special SQL2019_Second instance
    if "\\" in host and "SQL2019_Second" in host:
        print(f"NOTE: SQL2019_Second instance detected. Special connection handling will be used.")
        print(f"      Make sure SQL Browser service is running.")
    
    config['sqlservers'][name] = {
        'server': server_spec,
        'username': username,
        'password': password,
        'check_new_databases': True,
        'skip_databases': [],
        'sync_mode': 'hybrid',
        'target_postgres_db': pg_database or "postgres"  # Default to postgres if not specified
    }
    
    # Only add port if explicitly provided and not a named instance
    if port and "\\" not in host:
        config['sqlservers'][name]['port'] = port
        
    save_config(config)
    print(f"Server '{name}' added. Target Postgres DB: {config['sqlservers'][name]['target_postgres_db']}")
    
    # Show usage instructions
    print("\nUsage instructions:")
    print(f"1. Test connection: python test_connections.py")
    print(f"2. For SQL2019_Second instance: python test_sql2019_second.py")
    print(f"3. Start synchronization: python run_sync_worker.py --server {name}")

def delete_server(name):
    """Delete a server from both database and YAML config"""
    config = load_config()
    if name in config.get('sqlservers', {}):
        # Delete from config
        del config['sqlservers'][name]
        
        if USE_DB_STORAGE:
            try:
                # Delete directly from database
                remove_connection("sqlservers", name)
            except Exception as e:
                print(f"Error deleting from database, falling back to YAML: {e}")
                # Fallback to YAML only
                save_config(config)
        else:
            # YAML only
            save_config(config)
            
        print(f"Server '{name}' deleted!")
    else:
        print(f"ERROR: Server '{name}' not found!")

def discover_sql_servers():
    """
    Attempt to discover SQL Server instances on the local network.
    This is a basic implementation that checks common servers.
    
    Returns:
        list: List of discovered server names
    """
    import socket
    servers = []
    
    try:
        # Add local hostname
        hostname = socket.gethostname()
        servers.append(hostname)
        
        # Add localhost
        if hostname.lower() != 'localhost':
            servers.append('localhost')
        
        # TODO: Add network discovery if needed
    except Exception as e:
        print(f"Error during server discovery: {e}")
        
    return servers

def test_server_connection(server_name):
    """
    Test connection to a SQL Server.
    
    Args:
        server_name: Name of the server in the config
        
    Returns:
        bool: True if connection succeeded
    """
    config = load_config()
    
    if server_name not in config.get('sqlservers', {}):
        print(f"ERROR: Server '{server_name}' not found in configuration!")
        return False
    
    server_conf = config['sqlservers'][server_name]
    server = server_conf['server']
    
    print(f"Testing connection to {server_name} ({server})...")
    
    try:
        # Import here to avoid circular imports
        from hybrid_sync import get_sql_connection
        
        # Test connection
        conn = get_sql_connection(server_conf)
        cursor = conn.cursor()
        cursor.execute("SELECT @@SERVERNAME, @@VERSION")
        server_info = cursor.fetchone()
        
        print(f"✅ Connection successful! Connected to {server_info[0]}")
        print(f"   Server version: {server_info[1][:60]}...")
        
        # Get list of databases
        cursor.execute("""
            SELECT name 
            FROM sys.databases 
            WHERE state = 0 -- Online
            AND name NOT IN ('master', 'tempdb', 'model', 'msdb')  -- Skip system DBs
            ORDER BY name
        """)
        
        databases = [row[0] for row in cursor.fetchall()]
        print(f"\nFound {len(databases)} database(s):")
        for db in databases:
            print(f"  - {db}")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Connection failed: {str(e)}")
        
        if "SQL2019_Second" in server:
            print("\nNOTE: For SQL2019_Second instance:")
            print("  - Make sure SQL Browser service is running")
            print("  - Try running: python test_sql2019_second.py")
        else:
            print("\nTroubleshooting tips:")
            print("  - Verify server name and credentials")
            print("  - Check if SQL Server is running")
            print("  - For named instances, ensure SQL Browser service is running")
            
        return False

def interactive_add_server():
    """Interactive wizard to add a new SQL Server"""
    print("\n=== Add New SQL Server ===")
    
    # Try to discover servers
    print("Looking for SQL Servers...")
    discovered = discover_sql_servers()
    
    if discovered:
        print(f"Found {len(discovered)} potential server(s):")
        for i, server in enumerate(discovered):
            print(f"{i+1}. {server}")
        print(f"{len(discovered)+1}. Enter custom server name")
        
        choice = input("Select server or enter custom (number): ")
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(discovered):
                server = discovered[idx]
            else:
                server = input("Enter server name (hostname or hostname\\instance): ")
        except ValueError:
            server = input("Enter server name (hostname or hostname\\instance): ")
    else:
        print("No servers automatically discovered.")
        server = input("Enter server name (hostname or hostname\\instance): ")
    
    # Get server details
    name = input("Short name for this server (used in config): ")
    if not name:
        # Generate default name based on server
        if "\\" in server:
            host, instance = server.split("\\")
            name = f"{host.split('.')[0]}_{instance}"
        else:
            name = server.split('.')[0]
    
    auth_type = input("Authentication type (1 = Windows, 2 = SQL Server): ")
    
    if auth_type == "2":
        username = input("Username: ")
        password = input("Password: ")
    else:
        username = "windows"
        password = "windows"
    
    target_pg = input("Target PostgreSQL database (empty for 'postgres'): ") or "postgres"
    
    # Add the server
    add_server(name, server, username, password, pg_database=target_pg)
    
    # Test the connection
    print("\nTesting connection...")
    test_server_connection(name)
    
    return True

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Manage SQL Server connections for the sync tool.")
    parser.add_argument('--list', action='store_true', help='List SQL servers')
    parser.add_argument('--add', nargs=4, metavar=('NAME','HOST','USER','PASSWORD'), help='Add SQL server')
    parser.add_argument('--add-interactive', action='store_true', help='Add server with interactive wizard')
    parser.add_argument('--test', metavar='NAME', help='Test connection to a server')
    parser.add_argument('--delete', metavar='NAME', help='Delete SQL server')
    args = parser.parse_args()

    if args.list:
        list_servers()
    elif args.add:
        add_server(*args.add)
    elif args.add_interactive:
        interactive_add_server()
    elif args.test:
        test_server_connection(args.test)
    elif args.delete:
        delete_server(args.delete)
    else:
        parser.print_help()
