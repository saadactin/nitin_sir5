# Zoho API Source Configuration Guide

## Complete Field-by-Field Instructions

Use this guide to fill out the "Add API Source" form for Zoho CRM integration.

---

## Form Fields

### 1. **Source Name** *
**Value**: `zoho_crm_leads` (or any descriptive name)
**Example**: 
- `zoho_crm_leads`
- `Zoho CRM Leads`
- `my_zoho_sync`

**Purpose**: A friendly name to identify this source in the dashboard.

---

### 2. **API Endpoint URL** *
**Important**: Use the actual data endpoint, NOT the metadata endpoint.

**❌ WRONG (Metadata endpoint - returns schema, not data):**
```
https://www.zohoapis.in/crm/v8/settings/modules/Leads
```

**✅ CORRECT (Data endpoints - returns actual records):**

**For Leads:**
```
https://www.zohoapis.in/crm/v8/Leads?fields=Full_Name,Company,Email,Phone
```

**For Contacts:**
```
https://www.zohoapis.in/crm/v8/Contacts?fields=Full_Name,Email,Phone
```

**For Accounts:**
```
https://www.zohoapis.in/crm/v8/Accounts?fields=Account_Name,Website,Phone
```

**For Deals:**
```
https://www.zohoapis.in/crm/v8/Deals?fields=Deal_Name,Amount,Stage
```

**Fields Parameter**: 
- Specify which fields you want using `?fields=Field1,Field2,Field3`
- Common fields: `Full_Name`, `Company`, `Email`, `Phone`, `id`
- Leave empty for all fields: `https://www.zohoapis.in/crm/v8/Leads` (might return error if fields required)

---

### 3. **Authentication Type** *
**Value**: Select `Zoho OAuth 2.0 (Auto-Refresh)`

This will show the Zoho OAuth configuration section.

---

### 4. **Refresh Token** *
**Value**: `1000.2cbaa36345c6d04b699b0cb6740c21ef.149922195c479d83c84826653ff84ff4`

**Where to get it**: 
- From your initial OAuth authorization flow
- Never expires (unless revoked in Zoho)
- Keep it secure!

---

### 5. **Client ID** *
**Value**: `1000.0L3LLVLEKE9ELW7CE0I0KJ3K4FKBBT`

**Where to get it**: 
- From your Zoho Developer Console
- Found when creating a Zoho API client

---

### 6. **Client Secret** *
**Value**: `d99c479d4c0db451c653d8c380bf6a4c557a73528c`

**Where to get it**: 
- From your Zoho Developer Console
- Keep it confidential!

---

### 7. **Custom Headers (Optional)**
**Value**: Leave empty OR use:
```json
{"Content-Type": "application/json", "Accept": "application/json"}
```

**Note**: Usually not needed for Zoho APIs. The system automatically adds the Authorization header.

---

### 8. **HTTP Method** *
**Value**: Select `GET`

**Note**: Zoho CRM APIs use GET for fetching data.

---

### 9. **Data Path (Optional)**
**Value**: Leave empty (recommended)

**Why**: The system auto-detects the data path. Zoho APIs return data in this format:
```json
{
  "data": [
    {"Full_Name": "...", "Email": "..."},
    ...
  ]
}
```
The system will automatically find the `data` array.

**Manual Override**: If needed, enter `data`

---

### 10. **Target Database Type** *
**Value**: Select `ClickHouse`

**Why**: Zoho API data is best stored in ClickHouse for analytics.

---

### 11. **Target Database Name** *
**Value**: `zoho`

**Note**: 
- Database will be created if it doesn't exist
- You can use any name like `zoho_crm`, `zoho_data`, etc.

---

### 12. **Continuous Sync Mode (Optional)**
**Options**:

#### **Enable Polling** (Check this if you want continuous sync)
- **When to use**: If you want to automatically check for new data periodically
- **Poll Interval**: Set to `300` (5 minutes) or `3600` (1 hour)
- **ID Column**: `id` (default)

#### **Server-Sent Events (SSE)**
- **When to use**: Only if Zoho provides an SSE stream (very rare)
- **Leave unchecked** for regular REST APIs

**Recommended**: 
- For regular syncing: ✅ **Enable Polling** with interval `3600` (1 hour)
- For one-time sync: Leave unchecked

---

## Complete Example Configuration

Here's a complete example for syncing Leads:

```
Source Name: zoho_crm_leads
API Endpoint URL: https://www.zohoapis.in/crm/v8/Leads?fields=Full_Name,Company,Email,Phone
Authentication Type: Zoho OAuth 2.0 (Auto-Refresh)
Refresh Token: 1000.2cbaa36345c6d04b699b0cb6740c21ef.149922195c479d83c84826653ff84ff4
Client ID: 1000.0L3LLVLEKE9ELW7CE0I0KJ3K4FKBBT
Client Secret: d99c479d4c0db451c653d8c380bf6a4c557a73528c
Custom Headers: (leave empty)
HTTP Method: GET
Data Path: (leave empty for auto-detect)
Target Database Type: ClickHouse
Target Database Name: zoho
Enable Polling: ✅ (checked, interval: 3600)
```

---

## Important Notes

### ⚠️ Rate Limiting
If you see "too many requests" error:
- **Wait 5-10 minutes** before trying again
- Zoho limits token refresh requests
- This is normal after multiple test runs

### ✅ Best Practices
1. **Use correct API URL**: Use data endpoints (Leads, Contacts), not metadata endpoints (settings/modules)
2. **Specify fields**: Use `?fields=` parameter to get specific columns
3. **Leave Data Path empty**: Let the system auto-detect
4. **Enable Polling**: For continuous syncing, enable polling with 1-hour interval

### 🔄 Token Refresh
- System automatically refreshes tokens every hour
- No manual intervention needed
- Tokens are stored securely in database

---

## Common Issues & Solutions

### Issue: "Failed to obtain Zoho access token"
**Cause**: Rate limiting or incorrect credentials
**Solution**: 
1. Wait 5-10 minutes (rate limit cooldown)
2. Verify credentials are correct
3. Check Zoho Developer Console for any restrictions

### Issue: "HTTP 401 Unauthorized"
**Cause**: Token expired or wrong domain
**Solution**:
1. System should auto-refresh, but wait a moment
2. Ensure API URL uses correct domain (`.in`, `.com`, or `.eu`)
3. Check that token was successfully obtained

### Issue: "No data found"
**Cause**: Wrong API endpoint or no records
**Solution**:
1. Use data endpoints (Leads, Contacts), not metadata
2. Ensure your Zoho CRM has data
3. Check API URL includes `?fields=...` if required

---

## Testing Steps

1. Fill in all fields as shown above
2. Click **"Test Connection"** (wait if rate limited)
3. Wait for success message
4. Click **"Add & Start Sync"**
5. Check dashboard - source should show as "Online"
6. Click **"Sync Server"** to sync data
7. Verify data in ClickHouse database

---

**Last Updated**: 2025-11-03  
**Status**: Production Ready ✅

