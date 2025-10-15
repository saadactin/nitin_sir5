import os
import sys
import pyodbc
import logging

# Add parent directory to path to import from hybrid_sync
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

def diagnose_sql2019_second():
    """
    Specialized diagnostic for the SQL2019_Second named instance
    """
    print("\n" + "=" * 80)
    print("SPECIAL DIAGNOSTIC FOR SQL2019_Second NAMED INSTANCE")
    print("=" * 80)
    
    # Connection configurations to try
    connection_methods = [
        {
            "name": "Standard named instance",
            "conn_str": "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost\\SQL2019_Second;Trusted_Connection=yes;Timeout=30;"
        },
        {
            "name": "Direct port 14344 connection",
            "conn_str": "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost,14344;Trusted_Connection=yes;Timeout=30;"
        },
        {
            "name": "Named instance with TCP protocol",
            "conn_str": "DRIVER={ODBC Driver 17 for SQL Server};SERVER=tcp:localhost\\SQL2019_Second;Trusted_Connection=yes;Timeout=30;"
        },
        {
            "name": "Named instance with encryption disabled",
            "conn_str": "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost\\SQL2019_Second;Trusted_Connection=yes;Timeout=30;Encrypt=No;"
        },
        {
            "name": "Direct port with encryption disabled",
            "conn_str": "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost,14344;Trusted_Connection=yes;Timeout=30;Encrypt=No;"
        },
        {
            "name": "Named instance with all options",
            "conn_str": "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost\\SQL2019_Second;Trusted_Connection=yes;Timeout=60;Encrypt=No;TrustServerCertificate=Yes;Pooling=No;MARS_Connection=Yes;"
        }
    ]
    
    # Try each connection method
    success_methods = []
    for i, method in enumerate(connection_methods, 1):
        print(f"\n[TEST {i}] Trying {method['name']}...")
        print(f"Connection string: {method['conn_str']}")
        
        try:
            conn = pyodbc.connect(method['conn_str'])
            cursor = conn.cursor()
            cursor.execute("SELECT @@SERVERNAME, @@VERSION")
            server_info = cursor.fetchone()
            print(f"\u2705 SUCCESS! Connected to server {server_info[0]}")
            print(f"   Version: {server_info[1][:50]}...")
            cursor.close()
            conn.close()
            success_methods.append(method['name'])
        except Exception as e:
            print(f"\u274c Failed: {str(e)}")
    
    # Summary
    print("\n" + "=" * 80)
    if success_methods:
        print(f"SUCCESS! The following methods worked:")
        for method in success_methods:
            print(f"\u2705 {method}")
        
        # Recommend the best method
        print("\nRECOMMENDATION: Use the following connection string:")
        if "Direct port with encryption disabled" in success_methods:
            print("DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost,14344;Trusted_Connection=yes;Timeout=30;Encrypt=No;")
        elif "Named instance with all options" in success_methods:
            print("DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost\\SQL2019_Second;Trusted_Connection=yes;Timeout=60;Encrypt=No;TrustServerCertificate=Yes;Pooling=No;MARS_Connection=Yes;")
        else:
            print(connection_methods[success_methods.index(success_methods[0])]['conn_str'])
    else:
        print("\u274c All connection methods failed.")
        print("Recommendations:")
        print("1. Verify that SQL Server is running")
        print("2. Check if SQL Browser service is running")
        print("3. Verify that firewall allows connections to SQL Server")
        print("4. Check if the instance name is correct")
    
    print("=" * 80)
    
try:
    from hybrid_sync import connect_to_named_instance
    
    print("\n" + "=" * 80)
    print("TESTING SPECIALIZED NAMED INSTANCE CONNECTION FUNCTION")
    print("=" * 80)
    
    try:
        conn = connect_to_named_instance("localhost", "SQL2019_Second")
        cursor = conn.cursor()
        cursor.execute("SELECT @@SERVERNAME, @@VERSION")
        server_info = cursor.fetchone()
        print(f"\u2705 SUCCESS! Connected to server {server_info[0]}")
        print(f"   Version: {server_info[1][:50]}...")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"\u274c Specialized connection function failed: {str(e)}")
except ImportError:
    print("Could not import connect_to_named_instance function from hybrid_sync")
    
if __name__ == "__main__":
    diagnose_sql2019_second()
