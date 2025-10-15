#!/usr/bin/env python3
"""
Setup Verification Script
This script verifies that the project is properly configured and can run.
"""

import os
import sys
import subprocess

def check_environment():
    """Check if the environment is properly set up"""
    print("🔧 ENVIRONMENT SETUP VERIFICATION")
    print("=" * 60)
    
    # Check Python version
    python_version = sys.version_info
    print(f"✅ Python Version: {python_version.major}.{python_version.minor}.{python_version.micro}")
    
    if python_version < (3, 8):
        print("❌ ERROR: Python 3.8+ required")
        return False
    
    # Check if virtual environment is activated
    if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        print("✅ Virtual Environment: Activated")
    else:
        print("⚠️  Virtual Environment: Not detected (recommended to use venv)")
    
    return True

def check_dependencies():
    """Check if all required dependencies are installed"""
    print("\n📦 DEPENDENCY CHECK")
    print("=" * 60)
    
    required_packages = [
        'Flask', 'pandas', 'psycopg2', 'pyodbc', 
        'bcrypt', 'yaml', 'schedule', 'sqlalchemy', 'cryptography'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            if package == 'yaml':
                import yaml
            elif package == 'psycopg2':
                import psycopg2
            elif package == 'sqlalchemy':
                import sqlalchemy
            else:
                __import__(package.lower())
            print(f"✅ {package}: Installed")
        except ImportError:
            print(f"❌ {package}: Missing")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n❌ Missing packages: {', '.join(missing_packages)}")
        print("💡 Run: python -m pip install -r requirements.txt")
        return False
    
    return True

def check_configuration():
    """Check if configuration files are properly set up"""
    print("\n⚙️  CONFIGURATION CHECK")
    print("=" * 60)
    
    # Check .env file
    if os.path.exists('.env'):
        print("✅ .env file: Found")
        
        # Read .env and check key variables
        with open('.env', 'r') as f:
            env_content = f.read()
        
        required_vars = ['PG_HOST', 'PG_DATABASE', 'PG_USERNAME', 'PG_PASSWORD']
        missing_vars = []
        
        for var in required_vars:
            if f"{var}=" in env_content and not f"{var}=" == env_content.split(f"{var}=")[1].split('\n')[0].strip():
                print(f"✅ {var}: Configured")
            else:
                print(f"⚠️  {var}: Not configured or empty")
                missing_vars.append(var)
        
        if missing_vars:
            print(f"\n⚠️  Please configure: {', '.join(missing_vars)} in .env file")
    else:
        print("❌ .env file: Not found")
        print("💡 Copy .env.example to .env and configure your settings")
        return False
    
    # Check YAML config
    if os.path.exists('config/db_connections.yaml'):
        print("✅ YAML config: Found")
    else:
        print("❌ YAML config: Missing config/db_connections.yaml")
        return False
    
    return True

def test_database_connectivity():
    """Test database connectivity"""
    print("\n🗄️  DATABASE CONNECTIVITY TEST")
    print("=" * 60)
    
    try:
        # Test PostgreSQL connectivity
        print("Testing PostgreSQL connection...")
        from db_utils import load_pg_config, get_pg_connection
        
        config = load_pg_config()
        if config:
            print("✅ PostgreSQL config: Loaded successfully")
            
            try:
                conn = get_pg_connection()
                cursor = conn.cursor()
                cursor.execute("SELECT version()")
                version = cursor.fetchone()[0]
                print(f"✅ PostgreSQL connection: Success")
                print(f"   Version: {version[:50]}...")
                cursor.close()
                conn.close()
                return True
            except Exception as e:
                print(f"❌ PostgreSQL connection: Failed - {e}")
                return False
        else:
            print("❌ PostgreSQL config: Failed to load")
            return False
            
    except Exception as e:
        print(f"❌ Database test failed: {e}")
        return False

def test_app_startup():
    """Test if the app can start without errors"""
    print("\n🚀 APPLICATION STARTUP TEST")
    print("=" * 60)
    
    try:
        # Test app import
        print("Testing app import...")
        import app
        print("✅ App import: Success")
        
        # Test config loading
        print("Testing configuration loading...")
        from app import load_pg_databases
        databases = load_pg_databases()
        print(f"✅ Database discovery: Found {len(databases)} databases")
        
        return True
        
    except Exception as e:
        print(f"❌ App startup test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all verification checks"""
    print("🧪 PROJECT SETUP VERIFICATION")
    print("This script verifies that your project is ready to run")
    print()
    
    checks = [
        ("Environment", check_environment),
        ("Dependencies", check_dependencies),
        ("Configuration", check_configuration),
        ("Database Connectivity", test_database_connectivity),
        ("Application Startup", test_app_startup)
    ]
    
    results = []
    
    for check_name, check_func in checks:
        try:
            result = check_func()
            results.append((check_name, result))
        except Exception as e:
            print(f"❌ {check_name} check failed with exception: {e}")
            results.append((check_name, False))
    
    # Summary
    print("\n" + "=" * 60)
    print("VERIFICATION SUMMARY")
    print("=" * 60)
    
    passed = 0
    for check_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{check_name:20} {status}")
        if result:
            passed += 1
    
    print(f"\nChecks passed: {passed}/{len(results)}")
    
    if passed == len(results):
        print("\n🎉 ALL CHECKS PASSED!")
        print("Your project is ready to run. Start with: python app.py")
    elif passed >= len(results) - 1:
        print("\n✅ MOSTLY READY")
        print("Minor issues detected. The app should still run.")
        print("Fix the issues above for optimal experience.")
    else:
        print("\n❌ SETUP INCOMPLETE")
        print("Please fix the issues above before running the application.")
        
    return passed == len(results)

if __name__ == "__main__":
    main()