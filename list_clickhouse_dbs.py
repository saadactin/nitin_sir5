"""
Simple script to list ClickHouse databases.
Run this to verify ClickHouse connectivity before using the web UI.
"""
import os
import sys

# Try to load .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("[OK] Loaded .env file")
except:
    print("[WARN] python-dotenv not installed, using system environment variables")

# Try clickhouse-driver
try:
    from clickhouse_driver import Client
except ImportError:
    print("[ERROR] clickhouse-driver not installed!")
    print("Install it with: pip install clickhouse-driver")
    sys.exit(1)

# Get connection details from environment
ch_host = os.environ.get('CLICKHOUSE_HOST')
ch_port = os.environ.get('CLICKHOUSE_PORT')
ch_user = os.environ.get('CLICKHOUSE_USER')
ch_password = os.environ.get('CLICKHOUSE_PASSWORD', '')

print("\n" + "="*60)
print("ClickHouse Connection Test")
print("="*60)
print(f"Host:     {ch_host}")
print(f"Port:     {ch_port}")
print(f"User:     {ch_user}")
print(f"Password: {'(empty)' if not ch_password else '(set)'}")
print("="*60 + "\n")

if not ch_host or not ch_port or not ch_user:
    print("[ERROR] Missing required environment variables!")
    print("Please set in .env:")
    print("  CLICKHOUSE_HOST=localhost")
    print("  CLICKHOUSE_PORT=9000")
    print("  CLICKHOUSE_USER=default")
    print("  CLICKHOUSE_PASSWORD=")
    sys.exit(1)

try:
    print(f"Connecting to ClickHouse at {ch_host}:{ch_port}...")
    client = Client(
        host=ch_host,
        port=int(ch_port),
        user=ch_user,
        password=ch_password if ch_password else ''
    )
    
    print("[OK] Connected successfully!\n")
    
    # Get databases
    print("Fetching databases...")
    result = client.execute('SHOW DATABASES')
    
    print("\n" + "="*60)
    print(f"Found {len(result)} databases:")
    print("="*60)
    
    for idx, row in enumerate(result, 1):
        db_name = row[0] if isinstance(row, (tuple, list)) else row
        print(f"{idx}. {db_name}")
    
    print("="*60 + "\n")
    print("[SUCCESS] ClickHouse is accessible and working!")
    
except Exception as e:
    print(f"\n[ERROR] Failed to connect to ClickHouse:")
    print(f"  {type(e).__name__}: {e}")
    print("\nCommon fixes:")
    print("  1. Check if ClickHouse Docker container is running:")
    print("     docker ps | grep clickhouse")
    print("  2. Check if default user needs a password:")
    print("     docker exec -it my-clickhouse-server clickhouse-client")
    print("  3. Verify .env settings match your ClickHouse setup")
    sys.exit(1)
