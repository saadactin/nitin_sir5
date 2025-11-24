# File Analysis Report - Project Cleanup

## 🔍 Analysis Summary

**Total Python Files**: 117  
**Linter Errors Found**: 1 (minor warning - expected)  
**Files Analyzed**: All files in project

## ✅ Core Application Files (KEEP - Essential)

These files are actively used by the application:

### Main Application
- `app.py` - Main Flask application
- `auth.py` - Authentication system
- `db_utils.py` - Database utilities
- `scheduler_utils.py` - Scheduling system
- `sync_logger.py` - Sync logging
- `sync_manager.py` - Sync management
- `sync_summary.py` - Sync summary/reporting
- `connection_sync.py` - Connection synchronization
- `manage_server.py` - Server management
- `hybrid_sync.py` - SQL Server sync logic
- `hana_sync.py` - HANA sync logic
- `api_sync.py` - API sync logic
- `api_data_detector.py` - API data detection
- `api_polling.py` - API polling
- `api_upsert.py` - API upsert operations
- `universal_api_sync.py` - Universal API sync
- `zoho_oauth_manager.py` - Zoho OAuth management
- `oauth_token_manager.py` - OAuth token management
- `alerts.py` - Alert system
- `analytics.py` - Analytics
- `analytics_advanced.py` - Advanced analytics
- `metrics.py` - Metrics collection
- `dashboard.py` - Dashboard functions
- `seeschedule.py` - Schedule viewing
- `table_filters.py` - Table filtering
- `security.py` - Security utilities
- `monitoring.py` - Monitoring system
- `daily_summary_scheduler.py` - Daily summary scheduler

### Routes (KEEP - All used)
- `routes/main.py`
- `routes/dashboard.py`
- `routes/analytics.py`
- `routes/api.py`
- `routes/schedules.py`
- `routes/servers.py`
- `routes/sources.py`
- `routes/sync.py`
- `routes/upload.py`
- `routes/explore.py`
- `routes/debug.py`

### Utilities (KEEP)
- `utils/email_service.py`
- `utils/__init__.py`

### New Migration System (KEEP - Recently created)
- `unified_migration_manager.py` - Unified migration system
- `migration_validator.py` - Migration validation
- `migrate_hana_to_clickhouse.py` - HANA migration
- `test_hana_migration.py` - HANA migration testing
- `test_hana_incremental_sync.py` - Incremental sync testing

## ⚠️ Files That May Be Redundant (REVIEW)

### Duplicate/Similar Functionality
1. **`validate_hana_migration.py`** vs **`migration_validator.py`**
   - `validate_hana_migration.py` - Older validation script
   - `migration_validator.py` - New comprehensive validator
   - **Recommendation**: Keep `migration_validator.py`, review if `validate_hana_migration.py` has unique features

2. **`import_hana_export_to_clickhouse.py`** vs **`migrate_hana_to_clickhouse.py`**
   - `import_hana_export_to_clickhouse.py` - Older import script
   - `migrate_hana_to_clickhouse.py` - New unified migration
   - **Recommendation**: Keep `migrate_hana_to_clickhouse.py`, check if old one has unique features

3. **`final_working_sync.py`** - Appears to be a test/development file
   - **Recommendation**: Review - may be obsolete

### Test/Verification Scripts (May be redundant)
4. **`check_*.py`** files - Multiple check scripts:
   - `check_api_sync.py`
   - `check_csv_import_status.py`
   - `check_dashboard_data.py`
   - `check_hana_import_status.py`
   - `check_homepage_html.py`
   - `check_migration_data.py`
   - `check_pg_dbs_simple.py`
   - `check_specific_table.py`
   - `check_sync_status.py`
   - `check_synced_data.py`
   - `check_table_structure.py`
   - **Recommendation**: Review - some may be obsolete, keep useful ones

5. **`verify_*.py`** files - Verification scripts:
   - `verify_auto_emails.py`
   - `verify_data_migration.py`
   - `verify_multi_api_ready.py`
   - `verify_schedule_display.py`
   - `verify_setup.py`
   - `verify_test1_data.py`
   - `verify_unlimited_sync.py`
   - `verify_zoho_clickhouse_data.py`
   - **Recommendation**: Review - some may be obsolete

### Fix/Setup Scripts (May be one-time use)
6. **`fix_*.py`** files - Fix scripts:
   - `fix_all_sync_issues.py`
   - `fix_api_configs.sql` (SQL file)
   - `fix_api_sources.py`
   - `fix_clickhouse_env.py`
   - `fix_crm22_url.py`
   - `fix_data_sources_table.py`
   - `fix_hana_port_env.py`
   - `fix_hana_port_to_39017.py`
   - `fix_password_nullable.py`
   - `fix_sqlalchemy.py`
   - `fix_table_structure.py`
   - **Recommendation**: Review - may be one-time fixes, archive if not needed

7. **`setup_*.py`** files:
   - `setup_hana_env.py`
   - `setup_hana_sample_db.py`
   - `setup_and_run.ps1`
   - **Recommendation**: Keep if used for initial setup

### Development/Test Files
8. **`mock_*.py`** files:
   - `mock_incremental_api.py`
   - `mock_sse_server.py`
   - `mock-sse-server.js`
   - **Recommendation**: Review - may be test files

9. **`*_test*.py`** files:
   - `verify_test1_data.py`
   - **Recommendation**: Review

### Utility Scripts (Review)
10. **Inspection/Debug scripts**:
    - `inspect_clickhouse_data.py`
    - `inspect_saadtest_db.py`
    - `debug_tables.py`
    - `show_clickhouse_data.py`
    - `show_table_data.py`
    - `list_clickhouse_dbs.py`
    - **Recommendation**: Keep if useful for debugging

11. **Quick scripts**:
    - `quick_check.py`
    - `quick_restart.py`
    - **Recommendation**: Review

12. **Other utility scripts**:
    - `add_api_source_manually.py`
    - `apply_api_fixes.py`
    - `apply_performance_fixes.py`
    - `create_database.py`
    - `drop_all_api_tables.py`
    - `import_check.py`
    - `import_csv_to_clickhouse.py`
    - `simulate_upload_badfile.py`
    - `watch_sync.py`
    - **Recommendation**: Review each

### HANA-specific scripts (Review)
13. **HANA connection/testing**:
    - `find_hana_port.py`
    - `HANA_CONNECTION_ISSUE.py`
    - `try_port_39017.py`
    - `wait_for_hana.py`
    - **Recommendation**: Review - may be one-time troubleshooting

### OAuth/Server scripts
14. **OAuth/Server**:
    - `oauth_server_port_4000.py`
    - `flask_auto_start_polling.py`
    - `monitor_flask_server.py`
    - **Recommendation**: Review

### Database scripts
15. **Database utilities**:
    - `db_utils_optimized.py` - Check if this replaces `db_utils.py`
    - `load_postgres.py` - Used by hybrid_sync
    - `optimize_production_db.py` - Review
    - `create_database.py` - Review

### Migration scripts
16. **Migration**:
    - `migrate_zoho_oauth_schema.py` - May be one-time migration
    - **Recommendation**: Review

### Documentation/Config (KEEP)
- `PROJECT_IMPROVEMENTS.md`
- `QUICK_START_GUIDE.md`
- `HANA_SCHEDULING_GUIDE.md`
- `HANA_MIGRATION_TEST_README.md`
- `IMPROVEMENTS_SUMMARY.md`
- `hana_clickhouse_config_example.env`
- `requirements.txt`
- `requirements-docker.txt`
- `docker-compose.yml`
- `package.json`
- `package-lock.json`

### Batch/PowerShell Scripts (Review)
- `run_hana_import.bat`
- `start_hana_docker.bat`
- `START_UNLIMITED_SYNC.bat`
- `start-monitor.bat`
- `start_project.bat`
- `run-production.ps1`
- `start-monitor.ps1`
- `setup_and_run.ps1`
- `run-sync.ps1`
- `run-dev.ps1`
- `tail-logs.ps1`

### Log Files (Can be deleted - regenerated)
- `*.log` files (9 files)
- **Recommendation**: Can be deleted, will be regenerated

### Other Files
- `homepage_output.html` - Review
- `passwords.json` - **SECURITY**: Review - may contain sensitive data
- `API_VISUAL_GUIDE.py` - Review
- `routes.py` - Check if used (may be old routes file)

## 🗑️ Files Recommended for Deletion (After Review)

### High Confidence - Likely Obsolete
1. `final_working_sync.py` - Appears to be test/development file
2. `validate_hana_migration.py` - Replaced by `migration_validator.py`
3. `import_hana_export_to_clickhouse.py` - Replaced by `migrate_hana_to_clickhouse.py`
4. `routes.py` - Check if still used (routes are in routes/ folder)

### Medium Confidence - Review First
5. One-time fix scripts (if fixes already applied):
   - `fix_hana_port_to_39017.py`
   - `fix_hana_port_env.py`
   - `fix_clickhouse_env.py`
   - `fix_crm22_url.py`
   - `fix_password_nullable.py`
   - `fix_sqlalchemy.py`
   - `fix_table_structure.py`
   - `fix_data_sources_table.py`
   - `fix_all_sync_issues.py`
   - `apply_api_fixes.py`
   - `apply_performance_fixes.py`

6. Test/verification scripts (if not actively used):
   - Multiple `check_*.py` files
   - Multiple `verify_*.py` files

7. Development/test files:
   - `mock_incremental_api.py`
   - `mock_sse_server.py`
   - `simulate_upload_badfile.py`

### Log Files (Safe to Delete)
8. All `*.log` files (will be regenerated):
   - `app.log`
   - `csv_import.log`
   - `email_delivery.log`
   - `hana_import.log`
   - `hybrid_sync.log`
   - `load_postgres.log`
   - `production.log`
   - `run_sync_worker.log`
   - `sync_operations.log`

## ⚠️ Files to Review Before Deletion

Before deleting, check if these are referenced:
- `db_utils_optimized.py` - Check if this is used instead of `db_utils.py`
- `import_csv_to_clickhouse.py` - Check if still used
- `create_database.py` - Check if used for setup
- `drop_all_api_tables.py` - Check if used for cleanup
- `watch_sync.py` - Check if used for monitoring
- `monitor_flask_server.py` - Check if used
- `quick_restart.py` - Check if used
- `quick_check.py` - Check if used

## 🔧 Errors Found

### Linter Errors
1. **`migrate_hana_to_clickhouse.py:19`** - Import "clickhouse_connect" could not be resolved
   - **Status**: Expected warning (runtime dependency)
   - **Action**: No fix needed - this is a runtime dependency

## 📋 Action Plan

1. **Review files marked for deletion** - Check if they're referenced anywhere
2. **Test application** - Ensure nothing breaks after deletion
3. **Archive instead of delete** - Move to `archive/` folder first
4. **Delete log files** - Safe to delete (regenerated)
5. **Review passwords.json** - Check for sensitive data

## 🎯 Next Steps

1. I'll check which files are actually imported/referenced
2. Create a final deletion list
3. Ask for your approval before deleting
4. Fix any errors found

