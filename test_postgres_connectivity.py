#!/usr/bin/env python3
"""
Test PostgreSQL connectivity using environment variables and YAML fallback.
Reproduces the "No PostgreSQL databases found" issue with detailed diagnostics.
"""

import os
import sys
import traceback
import psycopg2
import yaml
from pathlib import Path

def test_postgres_env_connectivity():
    """Test PostgreSQL connection using environment variables"""
    print("=" * 70)
    print("POSTGRESQL CONNECTIVITY TEST WITH ENVIRONMENT VARIABLES")
    print("=" * 70)
    
    # Check if .env variables are available
    env_vars = {
        'PG_HOST': os.environ.get('PG_HOST'),
        'PG_PORT': os.environ.get('PG_PORT'),
        'PG_DATABASE': os.environ.get('PG_DATABASE'),
        'PG_USERNAME': os.environ.get('PG_USERNAME'),
        'PG_PASSWORD': os.environ.get('PG_PASSWORD')
    }
    
    print("\n1. ENVIRONMENT VARIABLES CHECK:")
    for key, value in env_vars.items():
        if value:
            # Mask password for display
            display_value = value if key != 'PG_PASSWORD' else '*' * len(value)
            print(f"   ✅ {key} = {display_value}")
        else:
            print(f"   ❌ {key} = (not set)")
    
    missing_env = [k for k, v in env_vars.items() if not v]
    if missing_env:
        print(f"\n❌ Missing environment variables: {', '.join(missing_env)}")
        print("   To fix: Set these variables in your .env file or environment")
        return False, "Missing environment variables"
    
    print("\n2. ATTEMPTING CONNECTION WITH ENVIRONMENT VARIABLES:")
    try:
        conn = psycopg2.connect(
            host=env_vars['PG_HOST'],
            port=int(env_vars['PG_PORT']),
            database=env_vars['PG_DATABASE'],
            user=env_vars['PG_USERNAME'],
            password=env_vars['PG_PASSWORD']
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT version(), current_database(), current_user")
        version, database, user = cursor.fetchone()
        
        print(f"   ✅ SUCCESS! Connected to PostgreSQL")
        print(f"   📊 Database: {database}")
        print(f"   👤 User: {user}")
        print(f"   🔧 Version: {version[:50]}...")
        
        # Test listing databases
        cursor.execute("SELECT datname FROM pg_database WHERE NOT datistemplate ORDER BY datname")
        databases = [row[0] for row in cursor.fetchall()]
        print(f"   🗄️  Available databases: {', '.join(databases[:5])}")
        if len(databases) > 5:
            print(f"      ... and {len(databases) - 5} more")
        
        cursor.close()
        conn.close()
        return True, None
        
    except psycopg2.OperationalError as e:
        error_msg = str(e)
        print(f"   ❌ CONNECTION FAILED: {error_msg}")
        
        # Provide specific guidance based on error type
        if "could not connect to server" in error_msg.lower():
            print("   💡 Troubleshooting: Check if PostgreSQL server is running and accessible")
            print("      - Verify PG_HOST and PG_PORT are correct")
            print("      - Check firewall settings")
        elif "authentication failed" in error_msg.lower():
            print("   💡 Troubleshooting: Authentication issue")
            print("      - Verify PG_USERNAME and PG_PASSWORD are correct")
            print("      - Check pg_hba.conf for allowed authentication methods")
        elif "database" in error_msg.lower() and "does not exist" in error_msg.lower():
            print("   💡 Troubleshooting: Database doesn't exist")
            print("      - Verify PG_DATABASE name is correct")
            print("      - Create the database if it doesn't exist")
        
        return False, error_msg
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        print(f"   ❌ UNEXPECTED ERROR: {error_msg}")
        print(f"   📋 Full traceback:")
        traceback.print_exc()
        return False, error_msg

def test_postgres_yaml_connectivity():
    """Test PostgreSQL connection using YAML configuration"""
    print("\n" + "=" * 70)
    print("POSTGRESQL CONNECTIVITY TEST WITH YAML CONFIGURATION")
    print("=" * 70)
    
    config_path = Path(__file__).parent / 'config' / 'db_connections.yaml'
    
    print(f"\n1. YAML CONFIGURATION CHECK:")
    print(f"   📁 Config file: {config_path}")
    
    if not config_path.exists():
        print(f"   ❌ Config file not found!")
        return False, "YAML config file not found"
    
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
    except Exception as e:
        print(f"   ❌ Failed to load YAML: {e}")
        return False, f"YAML load error: {e}"
    
    pg_config = config.get('postgresql', {})
    if not pg_config:
        print(f"   ❌ No 'postgresql' section found in YAML")
        return False, "No postgresql section in YAML"
    
    print(f"   ✅ YAML loaded successfully")
    for key in ['host', 'port', 'database', 'username', 'password']:
        value = pg_config.get(key)
        if value:
            display_value = value if key != 'password' else '*' * len(str(value))
            print(f"   📋 {key} = {display_value}")
        else:
            print(f"   ❌ {key} = (missing)")
    
    missing_yaml = [k for k in ['host', 'port', 'database', 'username', 'password'] if not pg_config.get(k)]
    if missing_yaml:
        print(f"\n❌ Missing YAML keys: {', '.join(missing_yaml)}")
        return False, "Missing YAML configuration"
    
    print("\n2. ATTEMPTING CONNECTION WITH YAML CONFIGURATION:")
    try:
        conn = psycopg2.connect(
            host=pg_config['host'],
            port=int(pg_config['port']),
            database=pg_config['database'],
            user=pg_config['username'],
            password=pg_config['password']
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT version(), current_database(), current_user")
        version, database, user = cursor.fetchone()
        
        print(f"   ✅ SUCCESS! Connected to PostgreSQL via YAML")
        print(f"   📊 Database: {database}")
        print(f"   👤 User: {user}")
        print(f"   🔧 Version: {version[:50]}...")
        
        cursor.close()
        conn.close()
        return True, None
        
    except Exception as e:
        error_msg = str(e)
        print(f"   ❌ CONNECTION FAILED: {error_msg}")
        return False, error_msg

def test_db_utils_integration():
    """Test if db_utils functions work correctly"""
    print("\n" + "=" * 70)
    print("DB_UTILS INTEGRATION TEST")
    print("=" * 70)
    
    try:
        # Import db_utils and test its PostgreSQL functions
        sys.path.append(os.path.dirname(__file__))
        from db_utils import get_pg_connection, load_pg_config
        
        print("\n1. TESTING load_pg_config():")
        try:
            pg_config = load_pg_config()
            print(f"   ✅ Config loaded successfully")
            print(f"   📋 Config keys: {list(pg_config.keys())}")
        except Exception as e:
            print(f"   ❌ Failed to load config: {e}")
            return False, f"load_pg_config failed: {e}"
        
        print("\n2. TESTING get_pg_connection():")
        try:
            conn = get_pg_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            print(f"   ✅ Connection successful, test query returned: {result[0]}")
            cursor.close()
            conn.close()
            return True, None
        except Exception as e:
            print(f"   ❌ get_pg_connection failed: {e}")
            return False, f"get_pg_connection failed: {e}"
            
    except ImportError as e:
        print(f"   ❌ Failed to import db_utils: {e}")
        return False, f"Import error: {e}"
    except Exception as e:
        print(f"   ❌ Unexpected error: {e}")
        traceback.print_exc()
        return False, f"Unexpected error: {e}"

def main():
    """Run all PostgreSQL connectivity tests"""
    print("🐘 POSTGRESQL CONNECTIVITY DIAGNOSTIC TOOL")
    print("This tool will help diagnose the 'No PostgreSQL databases found' issue")
    print()
    
    results = []
    
    # Test 1: Environment variables
    env_success, env_error = test_postgres_env_connectivity()
    results.append(("Environment Variables", env_success, env_error))
    
    # Test 2: YAML configuration
    yaml_success, yaml_error = test_postgres_yaml_connectivity()
    results.append(("YAML Configuration", yaml_success, yaml_error))
    
    # Test 3: db_utils integration
    utils_success, utils_error = test_db_utils_integration()
    results.append(("db_utils Integration", utils_success, utils_error))
    
    # Summary
    print("\n" + "=" * 70)
    print("CONNECTIVITY TEST SUMMARY")
    print("=" * 70)
    
    for test_name, success, error in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{test_name:25} {status}")
        if error:
            print(f"                          Error: {error}")
    
    total_passed = sum(1 for _, success, _ in results if success)
    print(f"\nTests passed: {total_passed}/{len(results)}")
    
    if total_passed == 0:
        print("\n🚨 ALL TESTS FAILED")
        print("   The 'No PostgreSQL databases found' issue is due to connection problems.")
        print("   Please check your PostgreSQL server, credentials, and network connectivity.")
    elif total_passed < len(results):
        print("\n⚠️  SOME TESTS FAILED")
        print("   There may be configuration issues. Check the failed tests above.")
    else:
        print("\n🎉 ALL TESTS PASSED")
        print("   PostgreSQL connectivity is working correctly!")
        print("   If you're still seeing 'No PostgreSQL databases found', the issue")
        print("   may be in the application logic or specific to certain operations.")

if __name__ == "__main__":
    main()