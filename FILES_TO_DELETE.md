# Files Recommended for Deletion

## ✅ SAFE TO DELETE (Not Referenced Anywhere)

### 1. Obsolete/Replaced Files
- `final_working_sync.py` - Test/development file, not imported
- `validate_hana_migration.py` - Replaced by `migration_validator.py`
- `routes.py` - Old routes file, routes are in `routes/` folder
- `db_utils_optimized.py` - Alternative version, not used (db_utils.py is used)

### 2. Log Files (Will be regenerated)
- `app.log`
- `csv_import.log`
- `email_delivery.log`
- `hana_import.log`
- `hybrid_sync.log`
- `load_postgres.log`
- `production.log`
- `run_sync_worker.log`
- `sync_operations.log`

### 3. One-Time Fix Scripts (If fixes already applied)
- `fix_hana_port_to_39017.py` - One-time port fix
- `fix_hana_port_env.py` - One-time env fix
- `fix_clickhouse_env.py` - One-time env fix
- `fix_crm22_url.py` - One-time URL fix
- `fix_password_nullable.py` - One-time schema fix
- `fix_sqlalchemy.py` - One-time fix
- `fix_table_structure.py` - One-time fix
- `fix_data_sources_table.py` - One-time fix
- `fix_all_sync_issues.py` - One-time fix
- `apply_api_fixes.py` - One-time fix
- `apply_performance_fixes.py` - One-time fix

### 4. Test/Development Files
- `mock_incremental_api.py` - Mock/test file
- `mock_sse_server.py` - Mock/test file
- `mock-sse-server.js` - Mock/test file
- `simulate_upload_badfile.py` - Test file
- `API_VISUAL_GUIDE.py` - Visual guide (may be documentation)

### 5. HANA Troubleshooting Scripts (One-time use)
- `find_hana_port.py` - Troubleshooting script
- `HANA_CONNECTION_ISSUE.py` - Troubleshooting script
- `try_port_39017.py` - Troubleshooting script
- `wait_for_hana.py` - Troubleshooting script

### 6. Old Import Script (Replaced)
- `import_hana_export_to_clickhouse.py` - Replaced by `migrate_hana_to_clickhouse.py`
  - **Note**: Used by `run_hana_import.bat` - update bat file or delete both

## ⚠️ REVIEW BEFORE DELETING

### Files That May Still Be Used
- `import_hana_export_to_clickhouse.py` - Used by `run_hana_import.bat`
  - **Action**: Update bat file to use `migrate_hana_to_clickhouse.py` or delete both
- `import_csv_to_clickhouse.py` - Check if used for CSV imports
- `create_database.py` - Check if used for setup
- `drop_all_api_tables.py` - Check if used for cleanup
- `watch_sync.py` - Check if used for monitoring
- `monitor_flask_server.py` - Check if used
- `quick_restart.py` - Check if used
- `quick_check.py` - Check if used
- `homepage_output.html` - Check if used
- `passwords.json` - **SECURITY**: Review for sensitive data before deleting

### Check Scripts (May be useful for debugging)
- `check_*.py` files - Review if useful for debugging
- `verify_*.py` files - Review if useful for verification
- `inspect_*.py` files - Review if useful for inspection
- `show_*.py` files - Review if useful for viewing data

### Batch/PowerShell Scripts
- Review if these are used in your workflow
- Some may be useful for automation

## 📋 Summary

**Total files recommended for deletion**: ~35-40 files

**Categories**:
- Obsolete/Replaced: 4 files
- Log files: 9 files
- One-time fixes: 11 files
- Test/Development: 4 files
- Troubleshooting: 4 files
- Old import script: 1 file

**Files to review**: ~15-20 files

## 🎯 Next Steps

1. Review the list above
2. Confirm which files to delete
3. I'll delete them and update any references
4. Fix any errors found

