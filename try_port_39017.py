"""Try connecting to HANA on port 39017 instead of 39013"""
import hdbcli.dbapi as hana_dbapi
import os
from dotenv import load_dotenv

load_dotenv()

host = os.environ.get('HANA_HOST', 'localhost')
# Try port 39017
port = 39017
username = os.environ.get('HANA_USERNAME', 'SYSTEM')
password = os.environ.get('HANA_PASSWORD', 'YourPassword123')

print(f"Trying HANA connection on port 39017:")
print(f"  Host: {host}")
print(f"  Port: {port}")
print(f"  Username: {username}")
print()

try:
    conn = hana_dbapi.connect(
        address=host,
        port=port,
        user=username,
        password=password,
        encrypt=True,
        sslValidateCertificate=False
    )
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_USER, CURRENT_TIMESTAMP FROM DUMMY")
    result = cursor.fetchone()
    print("[OK] SUCCESS! Connected to HANA on port 39017!")
    print(f"     User: {result[0]}")
    print(f"     Timestamp: {result[1]}")
    cursor.close()
    conn.close()
    print("\n[SOLUTION] Use port 39017 instead of 39013!")
except Exception as e:
    print(f"[FAILED] Port 39017: {e}")
    print("\nTrying port 39013...")
    try:
        port = 39013
        conn = hana_dbapi.connect(
            address=host,
            port=port,
            user=username,
            password=password,
            encrypt=False  # Try without encryption
        )
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER FROM DUMMY")
        result = cursor.fetchone()
        print(f"[OK] SUCCESS! Connected on port 39013 WITHOUT encryption!")
        print(f"     User: {result[0]}")
        cursor.close()
        conn.close()
        print("\n[SOLUTION] Use port 39013 WITHOUT encryption!")
    except Exception as e2:
        print(f"[FAILED] Port 39013 without encryption: {e2}")
        print("\n[INFO] Both ports failed. HANA Express might need different setup.")

