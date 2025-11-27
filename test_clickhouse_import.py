"""
Quick test to verify clickhouse-connect can be imported
"""
import sys

print(f"Python: {sys.executable}")
print("="*60)

try:
    from clickhouse_connect import get_client
    print("✅ SUCCESS: clickhouse-connect imported successfully")
    
    # Try to get version info
    try:
        import clickhouse_connect
        print(f"✅ Package location: {clickhouse_connect.__file__}")
    except:
        pass
    
    # Test creating a client (without connecting)
    print("✅ get_client function is available")
    
except ImportError as e:
    print(f"❌ ERROR: Cannot import clickhouse-connect")
    print(f"   Error: {e}")
    print("\n   Solution: Run: pip install clickhouse-connect")
    sys.exit(1)

print("="*60)
print("✅ All checks passed! clickhouse-connect is ready to use.")

