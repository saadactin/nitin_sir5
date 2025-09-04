import yaml
import argparse
import os

CONFIG_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), 'config/db_connections.yaml')
)

OUTPUT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), 'data/sqlserver_exports/')
)

def load_config():
    with open(CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f)

def save_config(config):
    with open(CONFIG_PATH, 'w') as f:
        yaml.safe_dump(config, f)

def list_servers():
    config = load_config()
    for name, conf in config.get('sqlservers', {}).items():
        print(f"{name}: {conf['server']}:{conf['port']}")

def add_server(name, host, username, password, port=1433):
    config = load_config()
    config.setdefault('sqlservers', {})[name] = {
        'server': host,
        'username': username,
        'password': password,
        'port': port,
        'check_new_databases': True,
        'skip_databases': [],
        'sync_mode': 'hybrid'
    }
    save_config(config)
    print(f"Server {name} added!")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--list', action='store_true', help='List SQL servers')
    parser.add_argument('--add', nargs=4, metavar=('NAME','HOST','USER','PASSWORD'), help='Add SQL server')
    args = parser.parse_args()

    if args.list:
        list_servers()
    elif args.add:
        add_server(*args.add)
