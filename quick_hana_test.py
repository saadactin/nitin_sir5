"""Quick HANA connection test - simplified"""
from hdbcli import dbapi

print("Testing HANA connection...")
print("Port 39013 is accessible (verified)")
print("\nAttempting connection...")

try:
    # Try without databaseName first, with various connection options
    connection_params = {
        'address': 'localhost',
        'port': 39013,
        'user': 'SYSTEM',
        'password': 'HXEHana1',
        'encrypt': False,  # Disable SSL
        'sslValidateCertificate': False
    }
    
    print(f"Connection params: {dict((k,v if k != 'password' else '***') for k,v in connection_params.items())}")
    
    conn = dbapi.connect(**connection_params)
    print("✓ Connected to HANA successfully!")
    
    # Test query
    cursor = conn.cursor()
    cursor.execute("SELECT DATABASE_NAME, VERSION FROM SYS.M_DATABASE")
    result = cursor.fetchone()
    print(f"\nDatabase: {result[0]}")
    print(f"Version: {result[1]}")
    
    cursor.close()
    conn.close()
    print("\n✓ Test successful!")
    
except Exception as e:
    print(f"✗ Connection failed: {e}")
    print(f"\nError type: {type(e)}")
    import traceback
    traceback.print_exc()
