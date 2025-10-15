import yaml, os, traceback
import psycopg2

CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config', 'db_connections.yaml')

try:
    with open(CONFIG_PATH, 'r') as f:
        cfg = yaml.safe_load(f) or {}
    pg = cfg.get('postgresql', {})
    print('Loaded PostgreSQL config from YAML:')
    print(' host=', pg.get('host'))
    print(' port=', pg.get('port'))
    print(' database=', pg.get('database'))
    print(' username=', pg.get('username'))
    # don't print password

    if not all([pg.get('host'), pg.get('port'), pg.get('database'), pg.get('username'), pg.get('password')]):
        missing = [k for k in ('host','port','database','username','password') if not pg.get(k)]
        print('Missing required keys in YAML:', missing)
    else:
        try:
            conn = psycopg2.connect(
                dbname=pg.get('database'),
                user=pg.get('username'),
                password=pg.get('password'),
                host=pg.get('host'),
                port=int(pg.get('port'))
            )
            print('Connection successful to PostgreSQL')
            conn.close()
        except Exception as e:
            print('Connection failed:')
            traceback.print_exc()
except Exception as e:
    print('Error reading config or running test:')
    traceback.print_exc()
