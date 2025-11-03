# Zoho OAuth Integration Guide

## Overview

This document describes the comprehensive Zoho OAuth 2.0 integration that has been added to the data synchronization platform. The integration includes automatic token refresh every hour, seamless API data syncing, and proper schema management in ClickHouse.

## Features

✅ **Automatic Token Refresh**: Access tokens are automatically refreshed every hour (3600 seconds) before expiry  
✅ **Secure Token Storage**: OAuth credentials stored in database with proper encryption  
✅ **UI Integration**: User-friendly form for adding Zoho API sources  
✅ **ClickHouse Sync**: Automatic schema detection and data syncing to ClickHouse  
✅ **Domain Auto-Detection**: API domain is automatically detected from token response  
✅ **Test Coverage**: Comprehensive test cases for OAuth token management  

## Components

### 1. OAuth Token Manager (`zoho_oauth_manager.py`)

Handles all Zoho OAuth token operations:

- **`refresh_token()`**: Refreshes access token using refresh token
- **`get_valid_token()`**: Gets valid token, refreshing if necessary
- **Token caching**: In-memory cache with database persistence
- **Automatic expiry handling**: Refreshes tokens 5 minutes before expiry

### 2. Database Schema

New columns added to `data_sources` table:

- `oauth_refresh_token`: Zoho refresh token (never expires)
- `oauth_client_id`: Zoho client ID
- `oauth_client_secret`: Zoho client secret
- `oauth_access_token`: Current access token
- `oauth_token_expiry`: Token expiry timestamp
- `oauth_api_domain`: API domain from token response (e.g., `https://www.zohoapis.in`)

### 3. UI Form (`templates/add_api_source.html`)

Enhanced API source form with:

- **Zoho OAuth 2.0** authentication option
- Fields for refresh_token, client_id, client_secret
- Automatic token refresh information
- Form validation

### 4. API Sync Integration (`api_sync.py`)

Updated to support Zoho OAuth:

- Automatic token refresh before API calls
- Domain-aware URL construction
- Token persistence in database
- Error handling and retry logic

### 5. Backend Routes (`app.py`)

Routes updated to handle Zoho OAuth:

- `/add-source/api`: Accepts Zoho OAuth parameters
- `/sync_api_source/<id>`: Syncs with automatic token management
- Token initialization on source creation

## Usage

### Adding a Zoho API Source

1. Navigate to "Add Source" → "REST API"
2. Select authentication type: **Zoho OAuth 2.0 (Auto-Refresh)**
3. Enter your Zoho credentials:
   - **Refresh Token**: `1000.2cbaa36345c6d04b699b0cb6740c21ef.149922195c479d83c84826653ff84ff4`
   - **Client ID**: `1000.0L3LLVLEKE9ELW7CE0I0KJ3K4FKBBT`
   - **Client Secret**: `d99c479d4c0db451c653d8c380bf6a4c557a73528c`
4. Enter API endpoint URL:
   - Example: `https://www.zohoapis.in/crm/v8/Leads?fields=Full_Name,Company,Email`
5. Select target ClickHouse database
6. Click "Add & Start Sync"

### API Endpoints Supported

The system supports all Zoho CRM API endpoints:

- **Leads**: `https://www.zohoapis.in/crm/v8/Leads`
- **Contacts**: `https://www.zohoapis.in/crm/v8/Contacts`
- **Accounts**: `https://www.zohoapis.in/crm/v8/Accounts`
- **Deals**: `https://www.zohoapis.in/crm/v8/Deals`
- **Module Metadata**: `https://www.zohoapis.in/crm/v8/settings/modules/Leads`

### Automatic Token Refresh

Tokens are automatically refreshed:
- **Every hour** (3600 seconds) as per Zoho's token expiration
- **5 minutes before expiry** to ensure seamless operation
- **On-demand** when token is expired or missing

## How It Works

### Token Refresh Flow

```
1. User adds Zoho API source with credentials
   ↓
2. System obtains initial access token
   ↓
3. Token stored in database with expiry time
   ↓
4. On each API call:
   - Check if token is still valid (with 5min buffer)
   - If expired/expiring soon → refresh token
   - Update database with new token
   - Use token for API request
```

### API Request Flow

```
1. API sync initiated
   ↓
2. Check token validity
   ↓
3. Refresh if needed → Get new token from Zoho
   ↓
4. Construct Authorization header: "Bearer <token>"
   ↓
5. Make API request with proper domain
   ↓
6. Parse response and sync to ClickHouse
   ↓
7. Auto-create table with proper schema
```

## Configuration

### Token Refresh URL

The system uses Zoho India's token endpoint:
```
https://accounts.zoho.in/oauth/v2/token
```

### Request Format

```http
POST https://accounts.zoho.in/oauth/v2/token
Content-Type: application/x-www-form-urlencoded

refresh_token=1000.xxx
client_id=1000.yyy
client_secret=zzz
grant_type=refresh_token
```

### Response Format

```json
{
  "access_token": "1000.xxx",
  "api_domain": "https://www.zohoapis.in",
  "expires_in": 3600,
  "token_type": "Bearer",
  "scope": "ZohoCRM.modules.ALL"
}
```

## Testing

Run test cases:

```bash
python test_zoho_oauth.py
```

Test cases include:
- Token refresh success/failure
- Valid token validation
- Expired token handling
- Integration tests (with real credentials)

## Database Migration

The database schema was updated with:

```bash
python migrate_zoho_oauth_schema.py
```

This adds the OAuth columns to the `data_sources` table.

## Security Considerations

1. **Client Secret**: Stored securely in database
2. **Refresh Token**: Never expires, handle with care
3. **Access Token**: Automatically refreshed, short-lived (1 hour)
4. **Token Storage**: Encrypted in database
5. **API Domain**: Auto-detected, prevents domain mismatch errors

## Troubleshooting

### Token Refresh Fails

- Verify refresh_token, client_id, and client_secret are correct
- Check network connectivity to `accounts.zoho.in`
- Review application logs for detailed error messages

### API Calls Fail with 401

- Token may have expired → System should auto-refresh
- Check if refresh token is still valid
- Verify API domain matches token response

### Domain Mismatch

- System automatically uses domain from token response
- If using `.com` but token is for `.in`, URL is auto-corrected
- Check `oauth_api_domain` column in database

## Example: Syncing Leads

1. Add Zoho API source with:
   - URL: `https://www.zohoapis.in/crm/v8/Leads?fields=Full_Name,Company,Email,Phone`
   - Auth: Zoho OAuth 2.0
   - Target: ClickHouse database

2. System will:
   - Get access token automatically
   - Make authenticated API request
   - Parse JSON response
   - Create `crm_<source_name>` table in ClickHouse
   - Insert all lead records

3. Table schema auto-created from first record:
   - Column names from API response
   - Proper ClickHouse data types
   - Metadata columns (`_sync_timestamp`, `_source_api`)

## Future Enhancements

- Support for multiple Zoho data centers (US, EU, India)
- Token refresh metrics and monitoring
- Bulk API operations
- Incremental sync support
- Webhook support for real-time updates

## Support

For issues or questions:
1. Check application logs: `app.log`
2. Review test cases: `test_zoho_oauth.py`
3. Check database: `data_sources` table OAuth columns

---

**Last Updated**: 2024  
**Version**: 1.0  
**Status**: Production Ready ✅

