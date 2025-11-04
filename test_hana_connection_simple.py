"""Simple HANA connection test"""
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
print()

# Try multiple connection methods
methods = [
    ("Standard (encrypt=True)", {"encrypt": True, "sslValidateCertificate": False}),
    ("Without encryption", {"encrypt": False}),
    ("Minimal (defaults)", {}),
]

for method_name, extra_params in methods:
    print(f"\nTrying: {method_name}...")
    try:
        params = {
            "address": host,
            "port": port,
            "user": username,
            "password": password,
        }
        params.update(extra_params)
        
        conn = hana_dbapi.connect(**params)
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER, CURRENT_TIMESTAMP FROM DUMMY")
        result = cursor.fetchone()
        print(f"[OK] Connected successfully!")
        print(f"     User: {result[0]}")
        print(f"     Timestamp: {result[1]}")
        cursor.close()
        conn.close()
        print("\n[SUCCESS] HANA connection works!")
        break
    except Exception as e:
        error_msg = str(e)
        print(f"[FAILED] {error_msg[:100]}")

