import os, yaml, psycopg2, traceback
CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config', 'db_connections.yaml')
try:
    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f) or {}
    pg = cfg.get('postgresql', {})
    print('Using config:', pg)
    conn = psycopg2.connect(
        dbname=pg.get('database') or 'postgres',
        user=pg.get('username'),
        password=pg.get('password'),
        host=pg.get('host'),
        port=int(pg.get('port'))
    )
    cur = conn.cursor()
    cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
    print('DBs:', [r[0] for r in cur.fetchall()])
    conn.close()
except Exception:
    traceback.print_exc()
