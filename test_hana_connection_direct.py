"""Direct HANA connection test to diagnose protocol error"""
import hdbcli.dbapi as hana_dbapi
import os
from dotenv import load_dotenv

load_dotenv()

host = os.environ.get('HANA_HOST', 'localhost')
port = int(os.environ.get('HANA_PORT', '39013'))
username = os.environ.get('HANA_USERNAME', 'SYSTEM')
password = os.environ.get('HANA_PASSWORD', 'YourPassword123')

print(f"Testing HANA connection:")
print(f"  Host: {host}")
print(f"  Port: {port}")
print(f"  Username: {username}")
print(f"  Password: {'***' if password else '(empty)'}")
print()

try:
    # Try different connection methods
    print("Method 1: Standard connection...")
    conn = hana_dbapi.connect(
        address=host,
        port=port,
        user=username,
        password=password
    )
    print("SUCCESS: Connected!")
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_USER FROM DUMMY")
    result = cursor.fetchone()
    print(f"Current user: {result[0]}")
    cursor.close()
    conn.close()
except Exception as e:
    print(f"FAILED: {e}")
    print()
    
    # Try with encrypt disabled
    try:
        print("Method 2: Without encryption...")
        conn = hana_dbapi.connect(
            address=host,
            port=port,
            user=username,
            password=password,
            encrypt=False
        )
        print("SUCCESS: Connected without encryption!")
        conn.close()
    except Exception as e2:
        print(f"FAILED: {e2}")
        print()
        
        # Try with communicationTimeout
        try:
            print("Method 3: With communicationTimeout...")
            conn = hana_dbapi.connect(
                address=host,
                port=port,
                user=username,
                password=password,
                communicationTimeout=30000
            )
            print("SUCCESS: Connected with timeout!")
            conn.close()
        except Exception as e3:
            print(f"FAILED: {e3}")
            print("\nPossible issues:")
            print("1. HANA is still starting up - wait 2-3 more minutes")
            print("2. Wrong port - try 39013 instead of 39017")
            print("3. HANA service not ready - check docker logs")

