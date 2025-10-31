# 🚀 Quick Start: API to ClickHouse Sync

## Add Any REST API in 3 Easy Steps

### Step 1: Go to Add API Source
Navigate to: **http://127.0.0.1:5000/add_api_source**

### Step 2: Fill in Basic Info

**Required Fields:**
- **Source Name**: A friendly name (e.g., "CRM Data")
- **API URL**: Your API endpoint (e.g., `http://localhost:4000/api/data`)
- **Target Database**: Select your ClickHouse database

**Optional Fields:**
- **Authentication**: Choose Bearer Token, API Key, Basic Auth, or None
- **Data Path**: **Leave empty** - auto-detection will find your data! 🤖

**For Continuous Sync:**
- ✅ Check **"Enable Polling"** if your API adds new data over time
- Set **Poll Interval** (default: 5 seconds)
- Set **ID Column** for deduplication (default: "id")

### Step 3: Click "Add Source"

That's it! The system will:
1. 🔍 Auto-detect where your data is in the JSON response
2. 📊 Extract and flatten the records
3. 🏗️ Create the ClickHouse table automatically
4. 💾 Insert your data with proper column structure
5. 🔄 Keep syncing if polling is enabled

---

## Examples

### Example 1: Simple API (One-Time Sync)

```
Source Name: User Data
API URL: https://jsonplaceholder.typicode.com/users
Target Database: test12
Data Path: (leave empty)
Polling: Unchecked
```

**Result:** Syncs 10 users once, creates table with all user fields flattened.

---

### Example 2: Continuous Polling API

```
Source Name: CRM Leads
API URL: http://localhost:4000/api/data
Target Database: test12
Data Path: (leave empty)
Polling: ✅ Checked
Poll Interval: 5 seconds
ID Column: id
```

**Result:** 
- Syncs initial data
- Checks every 5 seconds for new records
- Only inserts NEW records (deduplication by ID)
- Runs forever in the background

---

### Example 3: API with Authentication

```
Source Name: Private API
API URL: https://api.example.com/data
Authentication: Bearer Token
Token: your-secret-token-here
Target Database: test12
Data Path: (leave empty)
```

**Result:** Authenticates and syncs your private data securely.

---

## Supported JSON Structures

The auto-detection works with **ANY** of these structures:

✅ **Standard wrapped data**
```json
{
  "data": [
    {"id": 1, "name": "Record 1"},
    {"id": 2, "name": "Record 2"}
  ]
}
```

✅ **Different key names**
```json
{
  "results": [...],
  "items": [...],
  "records": [...]
}
```

✅ **Root-level arrays**
```json
[
  {"id": 1, "name": "Record 1"},
  {"id": 2, "name": "Record 2"}
]
```

✅ **Nested structures**
```json
{
  "response": {
    "data": {
      "items": [...]
    }
  }
}
```

✅ **With metadata**
```json
{
  "data": [...],
  "pagination": {...},
  "info": {...}
}
```
*The system ignores metadata and extracts only the data records.*

---

## What Gets Created in ClickHouse?

For each API source, you get:

### Table Structure
- **All JSON fields** become individual columns
- **Nested objects** are flattened (e.g., `address.city` → `address_city`)
- **Arrays** are converted to JSON strings (for complex structures)
- **Metadata columns** added automatically:
  - `_source_api`: The API URL
  - `_sync_timestamp`: When the record was synced

### Example Transformation

**API Response:**
```json
{
  "data": [
    {
      "id": "123",
      "name": "John Doe",
      "email": "john@example.com",
      "created_at": "2025-10-30T10:00:00Z"
    }
  ]
}
```

**ClickHouse Table:**
```
id              | name      | email              | created_at           | _source_api                    | _sync_timestamp
123             | John Doe  | john@example.com   | 2025-10-30 10:00:00  | http://localhost:4000/api/data | 2025-10-30 15:00:00
```

---

## Manual Sync

After adding a source, you can manually trigger syncs:

1. Go to the homepage
2. Find your API source in the list
3. Click **"Sync Server"** button
4. Data syncs in the background

---

## View Your Data

### In ClickHouse Web UI
1. Go to: `http://localhost:8123/play`
2. Login with your ClickHouse credentials
3. Query: `SELECT * FROM your_database.your_table LIMIT 100`

### In Your Application
```sql
SELECT * FROM test12.crm_leads ORDER BY _sync_timestamp DESC
```

---

## Troubleshooting

### ❌ "No records found"
**Cause:** API returned empty response or error  
**Fix:** Check API URL is correct and accessible

### ❌ "Data stored as single JSON string"
**Cause:** Old bug (now fixed with auto-detection)  
**Fix:** Delete and re-add the source, or click "Sync Server" again

### ❌ "Authentication failed"
**Cause:** Invalid token or credentials  
**Fix:** Verify your authentication details

### ❌ "Table already exists"
**Cause:** Table from previous sync  
**Fix:** Either use the existing table or drop it manually:
```sql
DROP TABLE IF EXISTS your_database.your_table
```

---

## Tips & Best Practices

### ✅ DO
- Leave Data Path empty for auto-detection
- Use polling for APIs that add new data over time
- Choose meaningful source names
- Test with a small dataset first

### ❌ DON'T
- Don't manually specify Data Path unless auto-detection fails
- Don't use polling for static APIs (wastes resources)
- Don't sync very large APIs without testing first
- Don't forget to set authentication if required

---

## Need Help?

Check the logs for detailed information:
```bash
# View application logs
tail -f app.log

# Check for sync errors
grep ERROR app.log
```

Look for these messages:
- `📊 Using data path: 'data' (found 100 records)` - Auto-detection working
- `✅ Inserted 100 new records` - Successful sync
- `⚠️ No new records found` - All data already synced

---

## Summary

Adding an API source is now **incredibly simple**:

1. Enter API URL
2. Select target database
3. Click Add

The system handles everything else automatically! 🎉

**No more manual configuration. No more errors. Just plug and play!**

