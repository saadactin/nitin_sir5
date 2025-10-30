# 🚀 Quick Start Guide: API to ClickHouse Migration

## ⚡ 60-Second Setup

### 1. Add Your First API Source

**Navigate to:** `http://localhost:8123` → Add Source → Add API Source

**Fill in:**
```
✏️ Source Name: crm
🌐 API URL: https://jsonplaceholder.typicode.com/users
🔐 Authentication: None (Public API)
🎯 Target Type: ClickHouse
💾 Target Database: test11
```

**Click:** `Add & Start Sync` ▶️

### 2. Watch the Magic! ✨

```
[Background Process]
1. Connecting to API... ✓
2. Fetching data... ✓ (10 records)
3. Analyzing schema... ✓ (16 columns)
4. Creating table test11.crm... ✓
5. Inserting records... ✓ (10/10)
6. Done! 🎉

Time taken: ~1.5 seconds
```

### 3. Verify the Data

**Option A: Web Interface**
- Go to homepage
- Click "Databases" button on the CRM card
- Browse the `crm` table

**Option B: ClickHouse Query**
```sql
SELECT * FROM test11.crm LIMIT 10;
```

**Option C: Python Script**
```bash
python show_clickhouse_data.py
```

### 4. Re-Sync Anytime

**On Homepage:**
- Find your API source card
- Click `Sync Server` button
- ✅ Fresh data pulled!

---

## 📋 Supported API Types

### ✅ Simple REST APIs
```json
GET https://api.example.com/users
→ Returns: [{"id": 1, "name": "John"}, ...]
```

### ✅ APIs with Authentication
```
Bearer Token: ✓
API Key: ✓
Basic Auth: ✓
```

### ✅ Nested JSON
```json
{
  "user": {
    "profile": {
      "name": "John",
      "email": "john@example.com"
    }
  }
}
→ Flattened to: user_profile_name, user_profile_email
```

### ✅ Complex Structures
```json
{
  "address": {
    "geo": {"lat": "40.7", "lng": "-74.0"}
  }
}
→ Stored as: address_geo = '{"lat": "40.7", "lng": "-74.0"}'
```

---

## 🎯 Real-World Examples

### Example 1: Public API (No Auth)
```
API: https://jsonplaceholder.typicode.com/users
Source Name: users
Auth: None
Result: test11.users table with 10 records
```

### Example 2: Protected API (Bearer Token)
```
API: https://api.github.com/users/octocat/repos
Source Name: github_repos
Auth Type: Bearer Token
Token: ghp_xxxxxxxxxxxx
Result: test11.github_repos table
```

### Example 3: Internal API (Basic Auth)
```
API: https://internal.company.com/api/customers
Source Name: customers
Auth Type: Basic Auth
Username: admin
Password: ********
Result: test11.customers table
```

---

## 📊 What Gets Created

### ClickHouse Table Structure
```sql
CREATE TABLE {database}.{table_name} (
    -- Your API fields (auto-detected)
    id Int64,
    name String,
    email String,
    address_city String,
    company_name String,
    ...
    
    -- Metadata (auto-added)
    _sync_timestamp DateTime64(3),
    _source_api String
) ENGINE = MergeTree()
ORDER BY tuple();
```

### Naming Convention
```
Source Name → Table Name
----------------------------
"CRM API"    → crm_api
"Sales Data" → sales_data
"User Info"  → user_info
```

---

## 🔧 Troubleshooting

### Issue: "API connection failed"
**Fix:** Check if API is accessible in browser

### Issue: "ClickHouse connection failed"
**Fix:** Verify ClickHouse is running
```bash
SELECT version();  -- Should return version number
```

### Issue: "Table already exists"
**Fix 1:** Use different source name
**Fix 2:** Drop existing table
```sql
DROP TABLE IF EXISTS test11.crm;
```

### Issue: "No records synced"
**Check:**
1. API returns JSON array: `[{...}, {...}]`
2. Or single object: `{...}`
3. Or nested: `{"data": [{...}]}`
   - Use Data Path field: `data`

---

## 🎓 Advanced Usage

### Data Path for Nested Responses

**API Response:**
```json
{
  "status": "success",
  "data": {
    "users": [
      {"id": 1, "name": "John"},
      {"id": 2, "name": "Jane"}
    ]
  }
}
```

**Data Path:** `data.users`

### Custom Headers

**Example:**
```json
{
  "Accept": "application/json",
  "User-Agent": "MyApp/1.0"
}
```

### HTTP Methods

- **GET:** Most common (default)
- **POST:** For APIs requiring POST requests

---

## ✅ Success Indicators

After adding an API source, you should see:

1. ✅ Flash message: "REST API source '{name}' added and sync started"
2. ✅ Source appears on homepage with "Online" status
3. ✅ Table created in ClickHouse
4. ✅ Records appear in table

---

## 📞 Need Help?

### Run Diagnostics
```bash
python test_api_to_clickhouse.py
```

### Check Logs
```bash
tail -f app.log
```

### Verify Setup
```python
# Test ClickHouse connection
from db_utils import load_clickhouse_config
from clickhouse_driver import Client

ch_conf = load_clickhouse_config()
client = Client(
    host=ch_conf['host'],
    port=ch_conf['port'],
    user=ch_conf['user'],
    password=ch_conf['password']
)

# Test query
print(client.execute('SELECT version()'))
```

---

## 🎉 That's It!

You're now ready to migrate data from any REST API to ClickHouse!

**Key Points:**
- 🚀 No coding required
- ✨ Auto-creates tables
- 🔄 One-click re-sync
- 📊 Handles complex JSON
- ⚡ Blazing fast

**Happy Data Migration!** 🎊

