from dotenv import load_dotenv
load_dotenv()
import sys
from db_utils import get_pg_connection
try:
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute('SELECT 1')
    print('PG OK', cur.fetchone()[0])
    conn.close()
except Exception as e:
    print('PG ERROR:', e)
    sys.exit(1)








