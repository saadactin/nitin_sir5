import sys
import os
import logging
import pyodbc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

def test_sql_instance_connection(server_name, username="windows", password="windows", database=None):
    """
    Test connection to a SQL Server instance with detailed diagnostics.
    
    Args:
        server_name (str): Server name or server\instance
        username (str): SQL Server username or "windows" for Windows authentication
        password (str): SQL Server password or "windows" for Windows authentication
        database (str): Optional database name
    """
    print(f"\n{'=' * 60}")
    print(f"TESTING CONNECTION TO: {server_name}")
    print(f"{'=' * 60}")
    
    is_named_instance = "\\" in server_name
    if is_named_instance:
        host, instance = server_name.split("\\", 1)
        print(f"Host: {host}")
        print(f"Instance: {instance}")
    else:
        print(f"Host: {server_name}")
        print(f"Instance: DEFAULT")
    
    # Try to ping the server
    host_to_ping = server_name.split("\\")[0] if is_named_instance else server_name
    print(f"\n[TEST 1] Pinging {host_to_ping}...")
    ping_result = os.system(f"ping -n 2 {host_to_ping}")
    if ping_result == 0:
        print(f"✅ Ping successful to {host_to_ping}")
    else:
        print(f"⚠️ Ping failed to {host_to_ping} - server might be unreachable")
    
    # Prepare connection string
    server_escaped = server_name.replace("\\", "\\\\")
    
    # Try multiple connection strategies
    connection_strategies = [
        {
            "name": "Standard connection",
            "conn_str": f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_escaped};" +
                      (f"DATABASE={database};" if database else "") +
                      ("Trusted_Connection=yes;" if username.lower() in ["windows", "trusted"] else f"UID={username};PWD={password};") +
                      "Timeout=30;"
        },
        {
            "name": "Connection with encryption disabled",
            "conn_str": f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_escaped};" +
                      (f"DATABASE={database};" if database else "") +
                      ("Trusted_Connection=yes;" if username.lower() in ["windows", "trusted"] else f"UID={username};PWD={password};") +
                      "Timeout=30;Encrypt=no;"
        },
        {
            "name": "Connection with explicit TCP protocol",
            "conn_str": f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER=tcp:{server_escaped};" +
                      (f"DATABASE={database};" if database else "") +
                      ("Trusted_Connection=yes;" if username.lower() in ["windows", "trusted"] else f"UID={username};PWD={password};") +
                      "Timeout=30;"
        },
        {
            "name": "Connection with all options",
            "conn_str": f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_escaped};" +
                      (f"DATABASE={database};" if database else "") +
                      ("Trusted_Connection=yes;" if username.lower() in ["windows", "trusted"] else f"UID={username};PWD={password};") +
                      "Timeout=60;Encrypt=no;TrustServerCertificate=yes;Pooling=no;MARS_Connection=Yes;"
        }
    ]
    
    # If it's a named instance, also try with explicit port
    if is_named_instance:
        connection_strategies.append({
            "name": "Explicit port 14344 (for SQL2019_Second)",
            "conn_str": f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host_to_ping},14344;" +
                      (f"DATABASE={database};" if database else "") +
                      ("Trusted_Connection=yes;" if username.lower() in ["windows", "trusted"] else f"UID={username};PWD={password};") +
                      "Timeout=30;"
        })
    
    # Try each connection strategy
    for i, strategy in enumerate(connection_strategies, 1):
        print(f"\n[TEST {i+1}] Attempting {strategy['name']}...")
        conn_str = strategy['conn_str']
        print(f"Connection string: {conn_str}")
        
        try:
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            
            # Get server info
            cursor.execute("SELECT @@SERVERNAME, @@VERSION")
            server_info = cursor.fetchone()
            print(f"✅ CONNECTION SUCCESSFUL!")
            print(f"   Server Name: {server_info[0]}")
            print(f"   Version: {server_info[1][:50]}...")
            
            # If database specified, get database info
            if database:
                cursor.execute("SELECT DB_NAME(), DATABASEPROPERTYEX(DB_NAME(), 'Collation')")
                db_info = cursor.fetchone()
                print(f"   Database: {db_info[0]}")
                print(f"   Collation: {db_info[1]}")
            
            cursor.close()
            conn.close()
            print(f"   Connection closed successfully.")
            print(f"\n✅ STRATEGY {i+1} WORKED: {strategy['name']}")
            break
        except Exception as e:
            print(f"❌ Connection failed: {str(e)}")
    else:
        print(f"\n❌ ALL CONNECTION STRATEGIES FAILED")
    
    print(f"{'=' * 60}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python test_sql_instance.py <server_name> [username] [password] [database]")
        print("       For Windows Authentication, use 'windows' as username and password")
        sys.exit(1)
    
    server = sys.argv[1]
    username = sys.argv[2] if len(sys.argv) > 2 else "windows"
    password = sys.argv[3] if len(sys.argv) > 3 else "windows"
    database = sys.argv[4] if len(sys.argv) > 4 else None
    
    test_sql_instance_connection(server, username, password, database)
import sys
import os
import logging
import pyodbc

# Ensure project root is on path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

def test_sql_instance_connection(server_name, username="windows", password="windows", database=None):
    """
    Test connection to a SQL Server instance with detailed diagnostics.
    
    Args:
        server_name (str): Server name or server\instance
        username (str): SQL Server username or "windows" for Windows authentication
        password (str): SQL Server password or "windows" for Windows authentication
        database (str): Optional database name
    """
    print(f"\n{'=' * 60}")
    print(f"TESTING CONNECTION TO: {server_name}")
    print(f"{'=' * 60}")
    
    is_named_instance = "\\" in server_name
    if is_named_instance:
        host, instance = server_name.split("\\", 1)
        print(f"Host: {host}")
        print(f"Instance: {instance}")
    else:
        print(f"Host: {server_name}")
        print(f"Instance: DEFAULT")
    
    # Try to ping the server
    host_to_ping = server_name.split("\\")[0] if is_named_instance else server_name
    print(f"\n[TEST 1] Pinging {host_to_ping}...")
    ping_result = os.system(f"ping -n 2 {host_to_ping}")
    if ping_result == 0:
        print(f"✅ Ping successful to {host_to_ping}")
    else:
        print(f"⚠️ Ping failed to {host_to_ping} - server might be unreachable")
    
    # Prepare connection string
    server_escaped = server_name.replace("\\", "\\\\")
    
    # Try multiple connection strategies
    connection_strategies = [
        {
            "name": "Standard connection",
            "conn_str": f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_escaped};" +
                      (f"DATABASE={database};" if database else "") +
                      ("Trusted_Connection=yes;" if username.lower() in ["windows", "trusted"] else f"UID={username};PWD={password};") +
                      "Timeout=30;"
        },
        {
            "name": "Connection with encryption disabled",
            "conn_str": f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_escaped};" +
                      (f"DATABASE={database};" if database else "") +
                      ("Trusted_Connection=yes;" if username.lower() in ["windows", "trusted"] else f"UID={username};PWD={password};") +
                      "Timeout=30;Encrypt=no;"
        },
        {
            "name": "Connection with explicit TCP protocol",
            "conn_str": f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER=tcp:{server_escaped};" +
                      (f"DATABASE={database};" if database else "") +
                      ("Trusted_Connection=yes;" if username.lower() in ["windows", "trusted"] else f"UID={username};PWD={password};") +
                      "Timeout=30;"
        },
        {
            "name": "Connection with all options",
            "conn_str": f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_escaped};" +
                      (f"DATABASE={database};" if database else "") +
                      ("Trusted_Connection=yes;" if username.lower() in ["windows", "trusted"] else f"UID={username};PWD={password};") +
                      "Timeout=60;Encrypt=no;TrustServerCertificate=yes;Pooling=no;MARS_Connection=Yes;"
        }
    ]
    
    # If it's a named instance, also try with explicit port
    if is_named_instance:
        connection_strategies.append({
            "name": "Explicit port 14344 (for SQL2019_Second)",
            "conn_str": f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host_to_ping},14344;" +
                      (f"DATABASE={database};" if database else "") +
                      ("Trusted_Connection=yes;" if username.lower() in ["windows", "trusted"] else f"UID={username};PWD={password};") +
                      "Timeout=30;"
        })
    
    # Try each connection strategy
    for i, strategy in enumerate(connection_strategies, 1):
        print(f"\n[TEST {i+1}] Attempting {strategy['name']}...")
        conn_str = strategy['conn_str']
        print(f"Connection string: {conn_str}")
        
        try:
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            
            # Get server info
            cursor.execute("SELECT @@SERVERNAME, @@VERSION")
            server_info = cursor.fetchone()
            print(f"✅ CONNECTION SUCCESSFUL!")
            print(f"   Server Name: {server_info[0]}")
            print(f"   Version: {server_info[1][:50]}...")
            
            # If database specified, get database info
            if database:
                cursor.execute("SELECT DB_NAME(), DATABASEPROPERTYEX(DB_NAME(), 'Collation')")
                db_info = cursor.fetchone()
                print(f"   Database: {db_info[0]}")
                print(f"   Collation: {db_info[1]}")
            
            cursor.close()
            conn.close()
            print(f"   Connection closed successfully.")
            print(f"\n✅ STRATEGY {i+1} WORKED: {strategy['name']}")
            break
        except Exception as e:
            print(f"❌ Connection failed: {str(e)}")
    else:
        print(f"\n❌ ALL CONNECTION STRATEGIES FAILED")
    
    print(f"{'=' * 60}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python test_sql_instance.py <server_name> [username] [password] [database]")
        print("       For Windows Authentication, use 'windows' as username and password")
        sys.exit(1)
    
    server = sys.argv[1]
    username = sys.argv[2] if len(sys.argv) > 2 else "windows"
    password = sys.argv[3] if len(sys.argv) > 3 else "windows"
    database = sys.argv[4] if len(sys.argv) > 4 else None
    
    test_sql_instance_connection(server, username, password, database)
