"""
Fix ClickHouse Environment Variables in .env file
Updates the .env file with correct ClickHouse settings for remote server
"""
import os
import re

def fix_clickhouse_env():
    """Fix ClickHouse environment variables in .env file"""
    env_file = '.env'
    
    if not os.path.exists(env_file):
        print(f"[ERROR] {env_file} file not found!")
        return False
    
    print("=" * 70)
    print("FIXING CLICKHOUSE ENVIRONMENT VARIABLES")
    print("=" * 70)
    
    # Read current .env file
    with open(env_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    print("\nCurrent ClickHouse settings:")
    print("-" * 70)
    
    # Extract current values
    host_match = re.search(r'CLICKHOUSE_HOST=(.+)', content)
    port_match = re.search(r'CLICKHOUSE_PORT=(.+)', content)
    user_match = re.search(r'CLICKHOUSE_USER=(.+)', content)
    pass_match = re.search(r'CLICKHOUSE_PASSWORD=(.+)', content)
    
    current_host = host_match.group(1).strip() if host_match else 'not set'
    current_port = port_match.group(1).strip() if port_match else 'not set'
    current_user = user_match.group(1).strip() if user_match else 'not set'
    current_pass = '***' if pass_match else 'not set'
    
    print(f"  CLICKHOUSE_HOST = {current_host}")
    print(f"  CLICKHOUSE_PORT = {current_port}")
    print(f"  CLICKHOUSE_USER = {current_user}")
    print(f"  CLICKHOUSE_PASSWORD = {current_pass}")
    
    # Fix host (remove http:// if present)
    new_host = '74.225.251.123'
    if current_host.startswith('http://'):
        new_host = current_host.replace('http://', '').replace('https://', '').split(':')[0].strip()
        print(f"\n[FIX] Removing 'http://' prefix from host: {current_host} -> {new_host}")
    elif current_host == '74.225.251.123':
        new_host = current_host
        print(f"\n[OK] Host is already correct: {current_host}")
    else:
        print(f"\n[INFO] Using provided host: {current_host}")
        new_host = current_host.replace('http://', '').replace('https://', '').split(':')[0].strip()
    
    # Fix port (should be 9000 for native protocol, not 8123)
    new_port = '9000'
    if current_port == '8123':
        print(f"\n[FIX] Changing port from 8123 (HTTP) to 9000 (native protocol)")
    elif current_port == '9000':
        print(f"\n[OK] Port is already correct: {current_port}")
    else:
        print(f"\n[FIX] Setting port to 9000 (native protocol required)")
    
    # Keep user and password as is
    new_user = current_user if user_match else 'default'
    new_pass = pass_match.group(1).strip() if pass_match else ''
    
    # Replace in content
    content = re.sub(
        r'CLICKHOUSE_HOST=.*',
        f'CLICKHOUSE_HOST={new_host}',
        content
    )
    content = re.sub(
        r'CLICKHOUSE_PORT=.*',
        f'CLICKHOUSE_PORT={new_port}',
        content
    )
    
    # Write back
    with open(env_file, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print("\n" + "=" * 70)
    print("UPDATED .env FILE:")
    print("=" * 70)
    print(f"  CLICKHOUSE_HOST={new_host}")
    print(f"  CLICKHOUSE_PORT={new_port}")
    print(f"  CLICKHOUSE_USER={new_user}")
    print(f"  CLICKHOUSE_PASSWORD={'***' if new_pass else '(empty)'}")
    
    print("\n" + "=" * 70)
    print("IMPORTANT NOTES:")
    print("=" * 70)
    print("1. Port 9000 is for native protocol (required by clickhouse_driver)")
    print("2. Port 8123 is for HTTP interface (web UI only)")
    print("3. Make sure port 9000 is accessible from your network")
    print("4. If connection fails, check firewall settings")
    print("\nNext step: Run 'python test_clickhouse_connection.py' to verify")
    print("=" * 70)
    
    return True

if __name__ == '__main__':
    fix_clickhouse_env()

