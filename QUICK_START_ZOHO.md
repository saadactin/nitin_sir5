# Quick Start: Zoho API to ClickHouse Sync

## Step-by-Step Configuration

### Fill These Exact Values:

| Field | Value |
|-------|-------|
| **Source Name** | `zoho_leads` |
| **API Endpoint URL** | `https://www.zohoapis.in/crm/v8/Leads?fields=Full_Name,Company,Email,Phone` |
| **Authentication Type** | `Zoho OAuth 2.0 (Auto-Refresh)` |
| **Refresh Token** | `1000.2cbaa36345c6d04b699b0cb6740c21ef.149922195c479d83c84826653ff84ff4` |
| **Client ID** | `1000.0L3LLVLEKE9ELW7CE0I0KJ3K4FKBBT` |
| **Client Secret** | `d99c479d4c0db451c653d8c380bf6a4c557a73528c` |
| **Custom Headers** | *(leave empty)* |
| **HTTP Method** | `GET` |
| **Data Path** | *(leave empty - auto-detect)* |
| **Target Database Type** | `ClickHouse` |
| **Target Database Name** | `zoho` |
| **Enable Polling** | *(optional - check if you want hourly sync)* |
| **Poll Interval** | `3600` (if polling enabled) |

---

## ⚠️ IMPORTANT: Rate Limiting

**You're seeing rate limit errors because too many token requests were made.**

### Solution:
1. **Wait 5-10 minutes** before trying "Test Connection" again
2. The system will automatically handle rate limits with retries

### While Waiting:
- You can proceed to add the source directly (it will work once rate limit expires)
- The system will retry token refresh automatically

---

## Alternative API URLs (if you want different data):

### Contacts:
```
https://www.zohoapis.in/crm/v8/Contacts?fields=Full_Name,Email,Phone
```

### Accounts:
```
https://www.zohoapis.in/crm/v8/Accounts?fields=Account_Name,Website,Phone
```

### Deals:
```
https://www.zohoapis.in/crm/v8/Deals?fields=Deal_Name,Amount,Stage
```

### All Leads (without field filter):
```
https://www.zohoapis.in/crm/v8/Leads
```
*(Note: Might require fields parameter - if you get an error, add `?fields=*`)*

---

## After Adding Source:

1. Click **"Add & Start Sync"**
2. Check dashboard - source should appear
3. Click **"Sync Server"** button on the source card
4. Data will sync to `zoho` database in ClickHouse
5. Table will be created automatically as `crm_zoho_leads` (or similar)

---

## Verify Data:

Run this command to verify data was synced:
```bash
python verify_zoho_clickhouse_data.py
```

Or query ClickHouse directly:
```sql
SELECT COUNT(*) FROM zoho.crm_zoho_leads;
SELECT * FROM zoho.crm_zoho_leads LIMIT 5;
```

---

**Current Status**: ⏳ Wait 5-10 minutes for rate limit to expire, then try again!

