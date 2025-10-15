import os
import sys

# Add project root to sys.path so this script can be run from scripts/ directory
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from db_utils import get_pg_connection


def main():
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        cur.execute("SELECT username, role, created_at FROM metrics_sync_tables.users WHERE username='admin';")
        row = cur.fetchone()
        print('admin row:', row)
        cur.close()
        conn.close()
    except Exception as e:
        print('ERROR:', repr(e))

if __name__ == '__main__':
    main()
