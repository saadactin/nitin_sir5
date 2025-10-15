"""
Test script to verify the SQL Server connection fixes
"""

import os
import sys
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger()

# Import functions from hybrid_sync
try:
    from hybrid_sync import (
        get_sql_connection,
        get_sqlalchemy_engine,
        connect_to_named_instance
    )
    
    import yaml
    
    # Path to YAML config
    CONFIG_PATH = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "config", "db_connections.yaml")
    )
    
    # Load DB connection info from YAML
    with open(CONFIG_PATH, 'r') as f:
        config = yaml.safe_load(f)
        
    def test_server_connections():
        """Test connections to all configured SQL servers"""
        print("\n=== Testing SQL Server Connections ===")
        from datetime import datetime
        print(f"Test started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        servers = config.get('sqlservers', {})
        if not servers:
            print("No SQL servers found in configuration!")
            return
        
        results = []
        
        for server_name, server_conf in servers.items():
            print(f"\n>>> Testing {server_name}: {server_conf['server']} <<<")
            
            result = {
                "server_name": server_name,
                "server": server_conf["server"],
                "success_pyodbc": False,
                "success_sqlalchemy": False,
                "error_pyodbc": None,
                "error_sqlalchemy": None,
                "databases": []
            }
            
            # Test pyodbc connection
            print("1. Testing pyodbc connection...")
            try:
                conn = get_sql_connection(server_conf)
                cursor = conn.cursor()
                
                # Test basic connectivity
                cursor.execute("SELECT @@SERVERNAME, @@VERSION")
                server_info = cursor.fetchone()
                print(f"✅ PYODBC SUCCESS! Connected to {server_info[0]}")
                print(f"   Version: {server_info[1][:60]}...")
                
                # Get database list
                cursor.execute("""
                    SELECT name 
                    FROM sys.databases 
                    WHERE state = 0 -- Online
                    AND name NOT IN ('master', 'tempdb', 'model', 'msdb')  -- Skip system DBs
                    ORDER BY name
                """)
                
                result["databases"] = [row[0] for row in cursor.fetchall()]
                print(f"   Found {len(result['databases'])} user database(s):")
                for db in result["databases"][:5]:  # Show first 5 databases
                    print(f"     - {db}")
                if len(result["databases"]) > 5:
                    print(f"     - ... and {len(result['databases'])-5} more")
                
                result["success_pyodbc"] = True
                cursor.close()
                conn.close()
            except Exception as e:
                result["error_pyodbc"] = str(e)
                print(f"❌ PYODBC ERROR: {str(e)}")
            
            # Test SQLAlchemy connection
            print("\n2. Testing SQLAlchemy connection...")
            try:
                engine = get_sqlalchemy_engine(server_conf)
                conn = engine.connect()
                from sqlalchemy import text
                server_info = conn.execute(text("SELECT @@SERVERNAME, @@VERSION")).fetchone()
                print(f"✅ SQLALCHEMY SUCCESS! Connected to {server_info[0]}")
                print(f"   Version: {server_info[1][:60]}...")
                result["success_sqlalchemy"] = True
                conn.close()
                engine.dispose()
            except Exception as e:
                result["error_sqlalchemy"] = str(e)
                print(f"❌ SQLALCHEMY ERROR: {str(e)}")
            
            # Add results
            results.append(result)
            print("\n" + "-" * 60)
        
        # Print summary
        print("\n=== CONNECTION TEST SUMMARY ===")
        for result in results:
            name = result["server_name"]
            server = result["server"]
            pyodbc = "✅" if result["success_pyodbc"] else "❌"
            sqlalchemy = "✅" if result["success_sqlalchemy"] else "❌"
            dbs = len(result["databases"]) if "databases" in result else 0
            
            print(f"{name} ({server}): PyODBC={pyodbc}, SQLAlchemy={sqlalchemy}, Databases={dbs}")
        
        print("\nTroubleshooting:")
        print("- For SQL2019_Second issues: run python test_sql2019_second.py")
        print("- For connection errors: check server name, authentication, and firewall")
        print("- For SQLAlchemy errors: ensure proper text() usage in queries")
        
        return results
    
    def test_specific_server(server_name):
        """Test a specific server by name"""
        servers = config.get('sqlservers', {})
        if not servers:
            print("No SQL servers found in configuration!")
            return None
            
        if server_name not in servers:
            print(f"Server '{server_name}' not found in configuration!")
            print(f"Available servers: {', '.join(servers.keys())}")
            return None
            
        print(f"\n=== Testing SQL Server Connection: {server_name} ===")
        server_conf = servers[server_name]
        
        print(f"Server: {server_conf['server']}")
        print(f"Auth Type: {'Windows' if server_conf['username'].lower() in ['windows', 'trusted', ''] else 'SQL Server'}")
        
        # Test pyodbc connection
        print("\n1. Testing pyodbc connection...")
        try:
            conn = get_sql_connection(server_conf)
            cursor = conn.cursor()
            cursor.execute("SELECT @@SERVERNAME, @@VERSION")
            result = cursor.fetchone()
            print(f"✅ PYODBC SUCCESS! Connected to {result[0]}")
            print(f"   Version: {result[1]}")
            
            # Get database list
            cursor.execute("""
                SELECT name 
                FROM sys.databases 
                WHERE state = 0 -- Online
                AND name NOT IN ('master', 'tempdb', 'model', 'msdb')  -- Skip system DBs
                ORDER BY name
            """)
            
            databases = [row[0] for row in cursor.fetchall()]
            print(f"\n   Found {len(databases)} user database(s):")
            for db in databases:
                print(f"     - {db}")
                
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"❌ PYODBC ERROR: {str(e)}")
        
        # Test SQLAlchemy connection
        print("\n2. Testing SQLAlchemy connection...")
        try:
            engine = get_sqlalchemy_engine(server_conf)
            conn = engine.connect()
            from sqlalchemy import text
            result = conn.execute(text("SELECT @@SERVERNAME, @@VERSION")).fetchone()
            print(f"✅ SQLALCHEMY SUCCESS! Connected to {result[0]}")
            print(f"   Version: {result[1]}")
            conn.close()
            engine.dispose()
        except Exception as e:
            print(f"❌ SQLALCHEMY ERROR: {str(e)}")
            
        # If SQL2019_Second, suggest specialized diagnostic
        if "SQL2019_Second" in server_conf['server']:
            print("\nNOTE: For SQL2019_Second instance, you can run:")
            print("      python test_sql2019_second.py")
            print("      for more detailed diagnostics and connection tests")
    
    if __name__ == "__main__":
        import argparse
        parser = argparse.ArgumentParser(description="Test SQL Server connections")
        parser.add_argument("--server", help="Test a specific server (by name)")
        args = parser.parse_args()
        
        if args.server:
            test_specific_server(args.server)
        else:
            test_server_connections()
        
except ImportError as e:
    print(f"Error importing modules: {e}")
    print("Make sure you're running this script from the project directory.")
except Exception as e:
    print(f"Error: {e}")
"""
Test script to verify the SQL Server connection fixes
"""

import os
import sys
import logging

# Ensure project root is on path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger()

from hybrid_sync import (
    get_sql_connection,
    get_sqlalchemy_engine,
    connect_to_named_instance
)

import yaml

# Path to YAML config
CONFIG_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "config", "db_connections.yaml")
)

# Load DB connection info from YAML
with open(CONFIG_PATH, 'r') as f:
    config = yaml.safe_load(f)

def test_server_connections():
    """Test connections to all configured SQL servers"""
    print("\n=== Testing SQL Server Connections ===")
    from datetime import datetime
    print(f"Test started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    servers = config.get('sqlservers', {})
    if not servers:
        print("No SQL servers found in configuration!")
        return

    results = []

    for server_name, server_conf in servers.items():
        print(f"\n>>> Testing {server_name}: {server_conf['server']} <<<")
        
        result = {
            "server_name": server_name,
            "server": server_conf["server"],
            "success_pyodbc": False,
            "success_sqlalchemy": False,
            "error_pyodbc": None,
            "error_sqlalchemy": None,
            "databases": []
        }
        
        # Test pyodbc connection
        print("1. Testing pyodbc connection...")
        try:
            conn = get_sql_connection(server_conf)
            cursor = conn.cursor()
            
            # Test basic connectivity
            cursor.execute("SELECT @@SERVERNAME, @@VERSION")
            server_info = cursor.fetchone()
            print(f"✅ PYODBC SUCCESS! Connected to {server_info[0]}")
            print(f"   Version: {server_info[1][:60]}...")
            
            # Get database list
            cursor.execute("""
                SELECT name 
                FROM sys.databases 
                WHERE state = 0 -- Online
                AND name NOT IN ('master', 'tempdb', 'model', 'msdb')  -- Skip system DBs
                ORDER BY name
            """)
            
            result["databases"] = [row[0] for row in cursor.fetchall()]
            print(f"   Found {len(result['databases'])} user database(s):")
            for db in result["databases"][:5]:  # Show first 5 databases
                print(f"     - {db}")
            if len(result["databases"]) > 5:
                print(f"     - ... and {len(result['databases'])-5} more")
            
            result["success_pyodbc"] = True
            cursor.close()
            conn.close()
        except Exception as e:
            result["error_pyodbc"] = str(e)
            print(f"❌ PYODBC ERROR: {str(e)}")
        
        # Test SQLAlchemy connection
        print("\n2. Testing SQLAlchemy connection...")
        try:
            engine = get_sqlalchemy_engine(server_conf)
            conn = engine.connect()
            from sqlalchemy import text
            server_info = conn.execute(text("SELECT @@SERVERNAME, @@VERSION")).fetchone()
            print(f"✅ SQLALCHEMY SUCCESS! Connected to {server_info[0]}")
            print(f"   Version: {server_info[1][:60]}...")
            result["success_sqlalchemy"] = True
            conn.close()
            engine.dispose()
        except Exception as e:
            result["error_sqlalchemy"] = str(e)
            print(f"❌ SQLALCHEMY ERROR: {str(e)}")
        
        # Add results
        results.append(result)
        print("\n" + "-" * 60)
    
    # Print summary
    print("\n=== CONNECTION TEST SUMMARY ===")
    for result in results:
        name = result["server_name"]
        server = result["server"]
        pyodbc = "✅" if result["success_pyodbc"] else "❌"
        sqlalchemy = "✅" if result["success_sqlalchemy"] else "❌"
        dbs = len(result["databases"]) if "databases" in result else 0
        
        print(f"{name} ({server}): PyODBC={pyodbc}, SQLAlchemy={sqlalchemy}, Databases={dbs}")
    
    print("\nTroubleshooting:")
    print("- For SQL2019_Second issues: run python test_sql2019_second.py")
    print("- For connection errors: check server name, authentication, and firewall")
    print("- For SQLAlchemy errors: ensure proper text() usage in queries")
    
    return results

def test_specific_server(server_name):
    """Test a specific server by name"""
    servers = config.get('sqlservers', {})
    if not servers:
        print("No SQL servers found in configuration!")
        return None
        
    if server_name not in servers:
        print(f"Server '{server_name}' not found in configuration!")
        print(f"Available servers: {', '.join(servers.keys())}")
        return None
        
    print(f"\n=== Testing SQL Server Connection: {server_name} ===")
    server_conf = servers[server_name]
    
    print(f"Server: {server_conf['server']}")
    print(f"Auth Type: {'Windows' if server_conf['username'].lower() in ['windows', 'trusted', ''] else 'SQL Server'}")
    
    # Test pyodbc connection
    print("\n1. Testing pyodbc connection...")
    try:
        conn = get_sql_connection(server_conf)
        cursor = conn.cursor()
        cursor.execute("SELECT @@SERVERNAME, @@VERSION")
        result = cursor.fetchone()
        print(f"✅ PYODBC SUCCESS! Connected to {result[0]}")
        print(f"   Version: {result[1]}")
        
        # Get database list
        cursor.execute("""
            SELECT name 
            FROM sys.databases 
            WHERE state = 0 -- Online
            AND name NOT IN ('master', 'tempdb', 'model', 'msdb')  -- Skip system DBs
            ORDER BY name
        """)
        
        databases = [row[0] for row in cursor.fetchall()]
        print(f"\n   Found {len(databases)} user database(s):")
        for db in databases:
            print(f"     - {db}")
            
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ PYODBC ERROR: {str(e)}")
    
    # Test SQLAlchemy connection
    print("\n2. Testing SQLAlchemy connection...")
    try:
        engine = get_sqlalchemy_engine(server_conf)
        conn = engine.connect()
        from sqlalchemy import text
        result = conn.execute(text("SELECT @@SERVERNAME, @@VERSION")).fetchone()
        print(f"✅ SQLALCHEMY SUCCESS! Connected to {result[0]}")
        print(f"   Version: {result[1]}")
        conn.close()
        engine.dispose()
    except Exception as e:
        print(f"❌ SQLALCHEMY ERROR: {str(e)}")
        
    # If SQL2019_Second, suggest specialized diagnostic
    if "SQL2019_Second" in server_conf['server']:
        print("\nNOTE: For SQL2019_Second instance, you can run:")
        print("      python test_sql2019_second.py")
        print("      for more detailed diagnostics and connection tests")
    
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Test SQL Server connections")
    parser.add_argument("--server", help="Test a specific server (by name)")
    args = parser.parse_args()
    
    if args.server:
        test_specific_server(args.server)
    else:
        test_server_connections()
