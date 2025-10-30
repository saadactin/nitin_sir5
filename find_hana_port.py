"""Try all possible HANA ports"""
from hdbcli import dbapi

ports = [39013, 39015, 39017, 39040, 39041, 30013, 30015]

for port in ports:
    print(f"\nTrying port {port}...")
    try:
        conn = dbapi.connect(
            address='localhost',
            port=port,
            user='SYSTEM',
            password='HXEHana1',
            encrypt=False,
            sslValidateCertificate=False
        )
        print(f"  ✓ SUCCESS on port {port}!")
        
        cursor = conn.cursor()
        cursor.execute("SELECT DATABASE_NAME, VERSION FROM SYS.M_DATABASE")
        result = cursor.fetchone()
        print(f"  Database: {result[0]}")
        print(f"  Version: {result[1]}")
        cursor.close()
        conn.close()
        
        print(f"\n{'='*60}")
        print(f"✓✓✓ WORKING PORT FOUND: {port} ✓✓✓")
        print(f"{'='*60}")
        break
        
    except Exception as e:
        error = str(e)[:80]
        print(f"  ✗ Failed: {error}")
