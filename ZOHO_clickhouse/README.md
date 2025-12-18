# Zoho CRM to ClickHouse Integration

This folder contains all files related to the Zoho CRM to ClickHouse integration.

## Files Description

### Core Integration Files

1. **zoho_crm_sync.py**
   - Main module for syncing Zoho CRM data to ClickHouse
   - Handles fetching records from Zoho CRM modules
   - Manages data transformation and storage in ClickHouse
   - Functions: `get_access_token()`, `get_available_modules()`, `sync_zoho_modules()`, `fetch_all_records()`, `save_to_clickhouse()`

2. **zoho_oauth_manager.py**
   - Manages Zoho OAuth token refresh and validation
   - Handles automatic token refresh before expiration
   - Caches tokens in memory for performance
   - Class: `ZohoOAuthManager`

3. **templates/zoho_crm_integration.html**
   - Flask template for the Zoho CRM integration UI
   - Provides form for entering Zoho credentials
   - Displays available modules and allows selection
   - Handles sync initiation and progress display

### Test and Verification Files

4. **test_zoho_complete_flow.py**
   - Complete end-to-end test of the Zoho integration
   - Tests token retrieval, module listing, and ClickHouse connection
   - Verifies the virtual environment setup

5. **test_zoho_integration.py**
   - Comprehensive test suite for Zoho CRM integration
   - Tests access token, module listing, ClickHouse connection, and data sync
   - Includes verification of synced data

6. **verify_zoho_data.py**
   - Quick verification script to check Zoho data in ClickHouse
   - Lists all Zoho tables and their record counts
   - Shows sample data from synced tables

7. **verify_zoho_clickhouse_data.py**
   - Alternative verification script using clickhouse_driver
   - Checks specific Zoho tables in ClickHouse
   - Displays table schemas and sample data

### Database Migration

8. **migrate_zoho_oauth_schema.py**
   - Database migration script for PostgreSQL
   - Adds OAuth-related columns to `data_sources` table
   - Columns: `oauth_refresh_token`, `oauth_client_id`, `oauth_client_secret`, `oauth_access_token`, `oauth_token_expiry`, `oauth_api_domain`

### Documentation

9. **ZOHO_UI_TROUBLESHOOTING.md**
   - Troubleshooting guide for Zoho CRM integration UI
   - Common issues and solutions
   - Debug steps and manual testing instructions

## Usage

### Setting Up Zoho Integration

1. Ensure you have Zoho OAuth credentials:
   - Client ID
   - Client Secret
   - Refresh Token
   - API Domain (region-specific)

2. Configure ClickHouse connection:
   - Host
   - Username
   - Password
   - Database name

3. Access the integration page in the Flask app:
   - Navigate to `/zoho-crm-integration`
   - Fill in credentials
   - Load available modules
   - Select modules to sync
   - Start sync

### Running Tests

```bash
# Test complete flow
python test_zoho_complete_flow.py

# Test integration
python test_zoho_integration.py

# Verify data
python verify_zoho_data.py
```

### Database Migration

If you need to add OAuth columns to the database:

```bash
python migrate_zoho_oauth_schema.py
```

## Dependencies

- `clickhouse-connect` or `clickhouse-driver` - ClickHouse client
- `requests` - HTTP requests to Zoho API
- `psycopg2` - PostgreSQL adapter (for database migration)
- Flask - Web framework (for UI template)

## Integration with Main Application

These files are integrated into the main Flask application (`app.py`) through:

- Route: `/zoho-crm-integration` - Renders the integration UI
- API Routes:
  - `/api/zoho/check-environment` - Checks environment setup
  - `/api/zoho/list-modules` - Lists available Zoho modules
  - `/api/zoho/test-connection` - Tests Zoho and ClickHouse connections
  - `/api/zoho/sync` - Initiates the sync process

## Notes

- All Zoho tables in ClickHouse are prefixed with `zoho_` (e.g., `zoho_leads`, `zoho_contacts`)
- Tables are created dynamically based on the fields in each Zoho module
- Data is stored as `Nullable(String)` in ClickHouse for flexibility
- Each record includes an `id` field and a `load_time` timestamp

