"""Wait for HANA to be ready"""
import time
import hdbcli.dbapi as hana_dbapi
import os
from dotenv import load_dotenv

load_dotenv()

host = os.environ.get('HANA_HOST', 'localhost')
port = int(os.environ.get('HANA_PORT', '39013'))
username = os.environ.get('HANA_USERNAME', 'SYSTEM')
password = os.environ.get('HANA_PASSWORD', 'YourPassword123')

print(f"Waiting for HANA at {host}:{port} to be ready...")
print("This may take 5-10 minutes on first start...")

max_wait = 600  # 10 minutes
check_interval = 10  # Check every 10 seconds
elapsed = 0

while elapsed < max_wait:
    try:
        conn = hana_dbapi.connect(
            address=host,
            port=port,
            user=username,
            password=password,
            encrypt=False
        )
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER FROM DUMMY")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        print(f"\n[OK] HANA is ready! (waited {elapsed} seconds)")
        print(f"Current user: {result[0]}")
        exit(0)
    except Exception as e:
        if elapsed % 30 == 0:  # Print every 30 seconds
            print(f"   Still waiting... ({elapsed}s) - {str(e)[:50]}")
        time.sleep(check_interval)
        elapsed += check_interval

print(f"\n[FAILED] HANA not ready after {max_wait} seconds")
exit(1)

