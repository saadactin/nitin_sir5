"""
Check which Python environment Flask app is using
Run this to see what Python Flask will use
"""
import sys
import os

print("="*60)
print("FLASK ENVIRONMENT CHECK")
print("="*60)
print(f"Python Executable: {sys.executable}")
print(f"Python Version: {sys.version}")
print(f"Python Path: {sys.path[:3]}")
print("="*60)

# Check for clickhouse-connect
print("\nChecking for clickhouse-connect...")
try:
    import clickhouse_connect
    print(f"✅ clickhouse-connect is installed")
    print(f"   Location: {clickhouse_connect.__file__}")
    try:
        from clickhouse_connect import get_client
        print(f"   ✅ get_client function available")
    except Exception as e:
        print(f"   ❌ Error importing get_client: {e}")
except ImportError as e:
    print(f"❌ clickhouse-connect is NOT installed")
    print(f"   Error: {e}")
    print(f"\n   SOLUTION:")
    print(f"   Run: {sys.executable} -m pip install clickhouse-connect==0.8.0")
    print(f"   Or: pip install clickhouse-connect==0.8.0")

print("\n" + "="*60)
print("To fix: Install clickhouse-connect in the Python shown above")
print("="*60)

