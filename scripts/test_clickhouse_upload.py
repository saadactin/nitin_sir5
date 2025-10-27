"""
Small helper script to test ClickHouse connectivity and upload a small CSV-like dataset directly using clickhouse-driver.

Usage:
  - Set CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD in your environment or in a copied .env file
  - Run: python scripts/test_clickhouse_upload.py

This script does NOT use the Flask upload endpoint (authentication). It's a lightweight tool to verify ClickHouse connectivity and round-trip inserts.
"""
import os
import sys
from clickhouse_driver import Client

SAMPLE_DB = os.environ.get('CLICKHOUSE_TEST_DB', 'saadtest')
SAMPLE_TABLE = os.environ.get('CLICKHOUSE_TEST_TABLE', 'test_upload')

def main():
    ch_host = os.environ.get('CLICKHOUSE_HOST')
    ch_port = os.environ.get('CLICKHOUSE_PORT')
    ch_user = os.environ.get('CLICKHOUSE_USER')
    ch_password = os.environ.get('CLICKHOUSE_PASSWORD')

    if not ch_host or not ch_port or not ch_user:
        print('Please set CLICKHOUSE_HOST, CLICKHOUSE_PORT and CLICKHOUSE_USER in the environment (or .env)')
        sys.exit(1)

    # First, try to list databases using TCP client
    dbs = []
    try:
        client = Client(host=ch_host, port=int(ch_port), user=ch_user, password=ch_password)
        print(f"Connected to ClickHouse (TCP) at {ch_host}:{ch_port} as {ch_user}")
        rows = client.execute('SHOW DATABASES')
        for r in rows:
            dbs.append(r[0] if isinstance(r, (list, tuple)) else r)
    except Exception as e:
        print('TCP discovery failed:', e)
        # Try HTTP fallback
        try:
            import urllib.request, urllib.parse, json
            http_port = int(os.environ.get('CLICKHOUSE_HTTP_PORT', 8123))
            params = {'query': 'SHOW DATABASES', 'format': 'JSONCompact'}
            full_url = f"http://{ch_host}:{http_port}/?{urllib.parse.urlencode(params)}"
            password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
            password_mgr.add_password(None, full_url, ch_user, ch_password or '')
            handler = urllib.request.HTTPBasicAuthHandler(password_mgr)
            opener = urllib.request.build_opener(handler)
            with opener.open(full_url, timeout=5) as resp:
                parsed = json.loads(resp.read().decode('utf-8'))
                data = parsed.get('data') or []
                dbs = [row[0] for row in data]
            print(f"Connected to ClickHouse (HTTP) at {ch_host}:{http_port}")
        except Exception as e2:
            print('HTTP discovery failed:', e2)

    print('Databases discovered:', dbs)

    # If dbs empty, exit early
    if not dbs:
        print('No ClickHouse databases discovered — aborting test upload')
        sys.exit(1)

    # Choose SAMPLE_DB if present, else first discovered
    if SAMPLE_DB not in dbs:
        SAMPLE_DB = dbs[0]

    # Create database if not exists (via TCP client if available)
    try:
        client = Client(host=ch_host, port=int(ch_port), user=ch_user, password=ch_password)
        print(f"Creating database {SAMPLE_DB} (if not exists)")
        client.execute(f"CREATE DATABASE IF NOT EXISTS `{SAMPLE_DB}`")
    except Exception:
        # fallback: skip create
        pass

    # Create table
    print(f"Creating table {SAMPLE_DB}.{SAMPLE_TABLE}")
    client.execute(f"CREATE TABLE IF NOT EXISTS `{SAMPLE_DB}`.`{SAMPLE_TABLE}` (id UInt32, name Nullable(String), value Nullable(String)) ENGINE = Memory")

    # Insert sample rows
    rows = [ (1, 'alpha', '10'), (2, 'beta', '20'), (3, 'gamma', '30') ]
    print(f"Inserting {len(rows)} rows into {SAMPLE_DB}.{SAMPLE_TABLE}")
    client.execute(f"INSERT INTO `{SAMPLE_DB}`.`{SAMPLE_TABLE}` (id, name, value) VALUES", rows)

    # Verify
    res = client.execute(f"SELECT count() FROM `{SAMPLE_DB}`.`{SAMPLE_TABLE}`")
    print('Row count in table:', res[0][0] if res and isinstance(res[0], tuple) else res)

    print('Sample query:')
    print(client.execute(f"SELECT * FROM `{SAMPLE_DB}`.`{SAMPLE_TABLE}` LIMIT 10"))

if __name__ == '__main__':
    main()
