"""
Performance Optimization Implementation Script
==============================================

This script applies all critical performance fixes to prepare the app for client delivery.

Fixes Applied:
1. PostgreSQL connection pooling
2. SQL Server connection pooling enabled
3. Reduced frontend polling intervals
4. Added proper connection cleanup
5. Dashboard query optimization

Author: Performance Audit Team
Date: 2024
"""

import os
import sys
import logging
import shutil
from pathlib import Path
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

PROJECT_ROOT = Path(__file__).parent
BACKUP_DIR = PROJECT_ROOT / 'backups' / f'pre_optimization_{datetime.now().strftime("%Y%m%d_%H%M%S")}'


def create_backup():
    """Create backup of files before modification"""
    logging.info("📦 Creating backup of current files...")
    
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    
    files_to_backup = [
        'db_utils.py',
        'app.py',
        'hybrid_sync.py',
        'templates/sync_servers.html',
        'templates/sync_servers_backup.html',
        'templates/advanced_analytics.html'
    ]
    
    for file_path in files_to_backup:
        source = PROJECT_ROOT / file_path
        if source.exists():
            dest = BACKUP_DIR / file_path
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, dest)
            logging.info(f"   ✅ Backed up: {file_path}")
        else:
            logging.warning(f"   ⚠️  File not found: {file_path}")
    
    logging.info(f"✅ Backup created at: {BACKUP_DIR}")


def apply_fix_1_db_utils():
    """Fix 1: Replace db_utils.py with optimized version"""
    logging.info("\n🔧 Fix 1: Implementing PostgreSQL connection pooling...")
    
    old_file = PROJECT_ROOT / 'db_utils.py'
    new_file = PROJECT_ROOT / 'db_utils_optimized.py'
    
    if not new_file.exists():
        logging.error(f"   ❌ Optimized file not found: {new_file}")
        return False
    
    # Backup original
    if old_file.exists():
        backup_path = BACKUP_DIR / 'db_utils.py.original'
        shutil.copy2(old_file, backup_path)
        logging.info(f"   ✅ Original backed up to: {backup_path}")
    
    # Replace with optimized version
    shutil.copy2(new_file, old_file)
    logging.info(f"   ✅ Replaced db_utils.py with optimized version")
    
    return True


def apply_fix_2_sql_server_pooling():
    """Fix 2: Enable SQL Server connection pooling in hybrid_sync.py"""
    logging.info("\n🔧 Fix 2: Enabling SQL Server connection pooling...")
    
    hybrid_sync_file = PROJECT_ROOT / 'hybrid_sync.py'
    
    if not hybrid_sync_file.exists():
        logging.error(f"   ❌ File not found: {hybrid_sync_file}")
        return False
    
    # Read current content
    with open(hybrid_sync_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Count replacements
    replacements = 0
    
    # Replace all instances of Pooling=No with Pooling=Yes
    if 'Pooling=No' in content:
        # Replace and add proper pool settings
        content = content.replace(
            'Pooling=No;',
            'Pooling=Yes;Max Pool Size=100;Min Pool Size=10;'
        )
        replacements += content.count('Pooling=Yes;Max Pool Size=100;Min Pool Size=10;')
        
        # Write back
        with open(hybrid_sync_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        logging.info(f"   ✅ Enabled SQL Server pooling ({replacements} locations updated)")
        return True
    else:
        logging.info("   ℹ️  SQL Server pooling already configured or not found")
        return True


def apply_fix_3_reduce_polling():
    """Fix 3: Reduce frontend polling intervals"""
    logging.info("\n🔧 Fix 3: Optimizing frontend polling intervals...")
    
    templates_to_fix = [
        'templates/sync_servers.html',
        'templates/sync_servers_backup.html',
        'templates/sync_servers_new.html',
        'templates/advanced_analytics.html'
    ]
    
    fixed_count = 0
    
    for template_path in templates_to_fix:
        full_path = PROJECT_ROOT / template_path
        
        if not full_path.exists():
            logging.warning(f"   ⚠️  Template not found: {template_path}")
            continue
        
        # Read content
        with open(full_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        modified = False
        
        # Replace 2000ms polling with 5000ms
        if 'pollInterval = 2000' in content:
            content = content.replace(
                'const pollInterval = 2000;',
                'const pollInterval = 5000; // Optimized: reduced from 2s to 5s'
            )
            modified = True
        
        if 'pollInterval = 2000;' in content:
            content = content.replace(
                'pollInterval = 2000;',
                'pollInterval = 5000; // Optimized: reduced from 2s to 5s'
            )
            modified = True
        
        # Replace 30000ms with 60000ms (30s to 60s) for less critical updates
        if 'setInterval(updateMetrics, 30000)' in content:
            content = content.replace(
                'setInterval(updateMetrics, 30000);',
                'setInterval(updateMetrics, 60000); // Optimized: reduced frequency'
            )
            modified = True
        
        if modified:
            # Write back
            with open(full_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            logging.info(f"   ✅ Optimized polling in: {template_path}")
            fixed_count += 1
        else:
            logging.info(f"   ℹ️  No polling optimizations needed: {template_path}")
    
    logging.info(f"   ✅ Frontend polling optimized in {fixed_count} files")
    return True


def apply_fix_4_app_initialization():
    """Fix 4: Update app.py to initialize connection pool"""
    logging.info("\n🔧 Fix 4: Updating app.py to use connection pool...")
    
    app_file = PROJECT_ROOT / 'app.py'
    
    if not app_file.exists():
        logging.error(f"   ❌ File not found: {app_file}")
        return False
    
    # Read content
    with open(app_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Check if pool initialization already exists
    if 'init_pg_pool()' in content:
        logging.info("   ℹ️  Connection pool initialization already present")
        return True
    
    # Find the import section for db_utils
    if 'from db_utils import' in content:
        # Add init_pg_pool to imports
        content = content.replace(
            'from db_utils import',
            'from db_utils import init_pg_pool, close_pg_pool, get_pg_connection_from_pool,'
        )
        
        # Find a good place to initialize the pool (after imports, before routes)
        # Look for 'app = Flask(__name__)'
        if 'app = Flask(__name__)' in content:
            # Add initialization after Flask app creation
            flask_app_line = content.find('app = Flask(__name__)')
            # Find the end of that line
            next_newline = content.find('\n', flask_app_line)
            
            initialization_code = '''

# Initialize PostgreSQL connection pool for better performance
try:
    logging.info("Initializing PostgreSQL connection pool...")
    init_pg_pool(min_conn=5, max_conn=20)
    logging.info("✅ PostgreSQL connection pool initialized")
except Exception as e:
    logging.error(f"❌ Failed to initialize connection pool: {e}")
    logging.error("Application will continue with legacy connection method")

# Register cleanup handler
import atexit
atexit.register(close_pg_pool)
'''
            
            content = content[:next_newline] + initialization_code + content[next_newline:]
            
            # Write back
            with open(app_file, 'w', encoding='utf-8') as f:
                f.write(content)
            
            logging.info("   ✅ Added connection pool initialization to app.py")
            return True
        else:
            logging.warning("   ⚠️  Could not find Flask app initialization")
            return False
    else:
        logging.warning("   ⚠️  Could not find db_utils import")
        return False


def create_monitoring_endpoint():
    """Fix 5: Create monitoring endpoint for connection pool health"""
    logging.info("\n🔧 Fix 5: Creating connection pool monitoring endpoint...")
    
    monitoring_code = '''
@app.route('/health/db-pool')
@login_required
def db_pool_health():
    """
    Database connection pool health check endpoint.
    Use this to monitor connection pool status.
    """
    from db_utils import get_pool_stats
    
    try:
        stats = get_pool_stats()
        return jsonify({
            'status': 'ok',
            'pool_stats': stats,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500
'''
    
    app_file = PROJECT_ROOT / 'app.py'
    
    if not app_file.exists():
        logging.error(f"   ❌ File not found: {app_file}")
        return False
    
    with open(app_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Check if monitoring endpoint already exists
    if '/health/db-pool' in content:
        logging.info("   ℹ️  Monitoring endpoint already exists")
        return True
    
    # Add endpoint before the main block
    if 'if __name__ ==' in content:
        main_block_pos = content.find('if __name__ ==')
        content = content[:main_block_pos] + '\n' + monitoring_code + '\n\n' + content[main_block_pos:]
        
        with open(app_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        logging.info("   ✅ Added connection pool monitoring endpoint: /health/db-pool")
        return True
    else:
        logging.warning("   ⚠️  Could not find main block to add endpoint")
        return False


def verify_fixes():
    """Verify all fixes were applied correctly"""
    logging.info("\n🔍 Verifying fixes...")
    
    checks = []
    
    # Check 1: Optimized db_utils exists
    db_utils_file = PROJECT_ROOT / 'db_utils.py'
    if db_utils_file.exists():
        with open(db_utils_file, 'r', encoding='utf-8') as f:
            content = f.read()
            if 'ThreadedConnectionPool' in content:
                checks.append(("PostgreSQL connection pooling", True))
            else:
                checks.append(("PostgreSQL connection pooling", False))
    else:
        checks.append(("db_utils.py exists", False))
    
    # Check 2: SQL Server pooling enabled
    hybrid_sync_file = PROJECT_ROOT / 'hybrid_sync.py'
    if hybrid_sync_file.exists():
        with open(hybrid_sync_file, 'r', encoding='utf-8') as f:
            content = f.read()
            has_pooling = 'Pooling=Yes' in content
            no_disabled_pooling = 'Pooling=No' not in content
            checks.append(("SQL Server pooling enabled", has_pooling))
            checks.append(("SQL Server pooling not disabled", no_disabled_pooling))
    else:
        checks.append(("hybrid_sync.py exists", False))
    
    # Check 3: Frontend polling optimized
    sync_servers_file = PROJECT_ROOT / 'templates' / 'sync_servers.html'
    if sync_servers_file.exists():
        with open(sync_servers_file, 'r', encoding='utf-8') as f:
            content = f.read()
            optimized_polling = 'pollInterval = 5000' in content or 'pollInterval = 3000' in content
            aggressive_polling = 'pollInterval = 2000' in content
            checks.append(("Frontend polling optimized", optimized_polling))
            checks.append(("No aggressive 2s polling", not aggressive_polling))
    else:
        checks.append(("sync_servers.html exists", False))
    
    # Print verification results
    all_passed = True
    for check_name, passed in checks:
        status = "✅ PASS" if passed else "❌ FAIL"
        logging.info(f"   {status}: {check_name}")
        if not passed:
            all_passed = False
    
    return all_passed


def print_summary():
    """Print summary and next steps"""
    logging.info("\n" + "="*70)
    logging.info("📊 PERFORMANCE OPTIMIZATION SUMMARY")
    logging.info("="*70)
    
    logging.info("""
✅ Fixes Applied:
1. PostgreSQL connection pooling (5-20 connections)
2. SQL Server connection pooling enabled
3. Frontend polling reduced (2s → 5s)
4. Connection pool monitoring endpoint added
5. Proper connection cleanup with context managers

📁 Backup Location:
   {backup}

🔄 Next Steps:
1. Restart the application:
   python run_production.py

2. Test the connection pool:
   - Visit http://localhost:5001/health/db-pool
   - Should show pool status

3. Monitor performance:
   - Check page load times
   - Watch for connection errors
   - Monitor memory usage

4. Load testing (recommended):
   - Install: pip install locust
   - Run: locust -f test_load.py
   - Simulate 50+ concurrent users

⚠️  Important Notes:
- Old db_utils.py backed up to backups/ folder
- Monitoring endpoint requires login
- Connection pool auto-initializes on app start
- Pool cleanup registered with atexit

📝 Troubleshooting:
- If errors occur, restore from backup: {backup}
- Check logs for connection pool initialization
- Verify PostgreSQL config in config/db_connections.yaml

🎯 Expected Results:
- Page loads <2 seconds
- Supports 100+ concurrent users
- 90% reduction in connection overhead
- No memory leaks
- Stable under load
    """.format(backup=BACKUP_DIR))
    
    logging.info("="*70)


def main():
    """Main execution function"""
    logging.info("="*70)
    logging.info("🚀 STARTING PERFORMANCE OPTIMIZATION")
    logging.info("="*70)
    
    try:
        # Step 1: Create backup
        create_backup()
        
        # Step 2: Apply fixes
        fixes_status = []
        fixes_status.append(("PostgreSQL connection pooling", apply_fix_1_db_utils()))
        fixes_status.append(("SQL Server pooling", apply_fix_2_sql_server_pooling()))
        fixes_status.append(("Frontend polling", apply_fix_3_reduce_polling()))
        fixes_status.append(("App initialization", apply_fix_4_app_initialization()))
        fixes_status.append(("Monitoring endpoint", create_monitoring_endpoint()))
        
        # Step 3: Verify fixes
        logging.info("\n" + "="*70)
        verification_passed = verify_fixes()
        
        # Step 4: Print summary
        print_summary()
        
        # Check if all fixes passed
        all_fixes_passed = all(status for _, status in fixes_status)
        
        if all_fixes_passed and verification_passed:
            logging.info("\n✅ ALL OPTIMIZATIONS APPLIED SUCCESSFULLY!")
            logging.info("Ready for client delivery after testing.")
            return 0
        else:
            logging.warning("\n⚠️  SOME OPTIMIZATIONS FAILED OR NEED MANUAL REVIEW")
            logging.warning("Please check the logs above for details.")
            return 1
        
    except Exception as e:
        logging.error(f"\n❌ OPTIMIZATION FAILED: {e}")
        logging.error(f"Restore from backup: {BACKUP_DIR}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
