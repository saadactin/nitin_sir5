"""Test HANA Express container connectivity and setup"""
import sys

print("="*80)
print("SAP HANA Express Container Connection Test")
print("="*80)

# Test 1: Check if hdbcli is installed
print("\n[Step 1] Checking hdbcli installation...")
try:
    from hdbcli import dbapi
    print("✓ hdbcli is installed")
except ImportError:
    print("✗ hdbcli is NOT installed")
    print("\nTo install hdbcli:")
    print("  pip install hdbcli")
    sys.exit(1)

# Test 2: Try to connect to HANA Express
print("\n[Step 2] Attempting to connect to HANA Express...")
print("  Host: localhost")
print("  Port: 39013 (39013 for HXE tenant)")
print("  User: SYSTEM")

# Common default passwords for HANA Express
passwords_to_try = [
    "HXEHana1",      # Default HANA Express password
    "HanaExpress1",  # Alternative
    "Manager1",      # Alternative
]

# Try both ports - 39013 for system DB and 39013 for HXE tenant
ports_to_try = [39013, 39041]  # System DB and HXE tenant ports

connection = None
successful_password = None
successful_port = None

for port in ports_to_try:
    print(f"\n  Trying port: {port}")
    for pwd in passwords_to_try:
        try:
            print(f"    Password: {pwd[:4]}{'*' * (len(pwd)-4)}")
            connection = dbapi.connect(
                address='localhost',
                port=port,
                user='SYSTEM',
                password=pwd,
                databaseName='HXE'  # Connect to HXE tenant
            )
            print(f"    ✓ Connected successfully!")
            successful_password = pwd
            successful_port = port
            break
        except Exception as e:
            error_msg = str(e)
            print(f"    ✗ Failed: {error_msg[:80]}")
            continue
    if connection:
        break

if not connection:
    print("\n✗ Could not connect with any default password/port")
    print("\nTrying manual connection...")
    print("Enter SYSTEM password (default: HXEHana1):")
    manual_pwd = input("Password: ").strip() or "HXEHana1"
    for port in [39013, 39041, 30015, 30013]:
        try:
            print(f"  Trying port {port}...")
            connection = dbapi.connect(
                address='localhost',
                port=port,
                user='SYSTEM',
                password=manual_pwd,
                databaseName='HXE'
            )
            successful_password = manual_pwd
            successful_port = port
            print(f"✓ Connected successfully on port {port}!")
            break
        except Exception as e:
            print(f"  Failed: {str(e)[:60]}")
            continue
    
    if not connection:
        print(f"\n✗ All connection attempts failed")
        sys.exit(1)

# Test 3: Query HANA version and basic info
print("\n[Step 3] Querying HANA system information...")
try:
    cursor = connection.cursor()
    
    # Get HANA version
    cursor.execute("SELECT VERSION FROM SYS.M_DATABASE")
    version = cursor.fetchone()[0]
    print(f"  HANA Version: {version}")
    
    # Get database name
    cursor.execute("SELECT DATABASE_NAME FROM SYS.M_DATABASE")
    db_name = cursor.fetchone()[0]
    print(f"  Database Name: {db_name}")
    
    # List existing schemas
    cursor.execute("SELECT SCHEMA_NAME FROM SYS.SCHEMAS WHERE SCHEMA_OWNER != 'SYS' ORDER BY SCHEMA_NAME")
    schemas = cursor.fetchall()
    print(f"\n  Existing user schemas ({len(schemas)}):")
    for schema in schemas[:10]:  # Show first 10
        print(f"    - {schema[0]}")
    if len(schemas) > 10:
        print(f"    ... and {len(schemas) - 10} more")
    
    cursor.close()
    
except Exception as e:
    print(f"✗ Query failed: {e}")
    connection.close()
    sys.exit(1)

# Test 4: Save credentials for future use
print("\n[Step 4] Saving HANA connection details...")
config = {
    'host': 'localhost',
    'port': successful_port,
    'user': 'SYSTEM',
    'password': successful_password
}

import json
with open('hana_credentials.json', 'w') as f:
    json.dump(config, f, indent=2)
print("  ✓ Credentials saved to hana_credentials.json")

connection.close()

print("\n" + "="*80)
print("✓ HANA CONNECTION TEST PASSED")
print("="*80)
print("\nConnection Details:")
print(f"  Host: {config['host']}")
print(f"  Port: {config['port']}")
print(f"  User: {config['user']}")
print(f"  Password: {config['password']}")
print("\nYou can now use these credentials in the Add Source form.")
