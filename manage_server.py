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
    if not os.path.exists(CONFIG_PATH):
        return {"sqlservers": {}}
    with open(CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f) or {"sqlservers": {}}

def save_config(config):
    with open(CONFIG_PATH, 'w') as f:
        yaml.safe_dump(config, f, default_flow_style=False)

def list_servers():
    config = load_config()
    servers = config.get('sqlservers', {})
    if not servers:
        print("⚠️ No servers configured yet.")
        return
    for name, conf in servers.items():
        print(f"{name} -> {conf['server']}:{conf['port']} (user: {conf['username']})")

def add_server(name, host, username, password, port=1433):
    config = load_config()
    config.setdefault('sqlservers', {})
    if name in config['sqlservers']:
        print(f"❌ Server name '{name}' already exists! Use a different name.")
        return
    config['sqlservers'][name] = {
        'server': host,
        'username': username,
        'password': password,
        'port': port,
        'check_new_databases': True,
        'skip_databases': [],
        'sync_mode': 'hybrid'
    }
    save_config(config)
    print(f"✅ Server '{name}' added! ({host}:{port})")

def delete_server(name):
    config = load_config()
    if name in config.get('sqlservers', {}):
        del config['sqlservers'][name]
        save_config(config)
        print(f"✅ Server '{name}' deleted!")
    else:
        print(f"❌ Server '{name}' not found!")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--list', action='store_true', help='List SQL servers')
    parser.add_argument('--add', nargs=4, metavar=('NAME','HOST','USER','PASSWORD'), help='Add SQL server')
    parser.add_argument('--delete', metavar='NAME', help='Delete SQL server')
    args = parser.parse_args()

    if args.list:
        list_servers()
    elif args.add:
        add_server(*args.add)
    elif args.delete:
        delete_server(args.delete)
