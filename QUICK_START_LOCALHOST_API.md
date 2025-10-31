# Quick Start: Sync Your Localhost API to ClickHouse

## 🎯 Your API
```
URL: http://localhost:4000/api/data
Adds 5 new rows every 5 seconds
```

## 🚀 Setup (5 Steps)

### Step 1: Start Flask App
```bash
python app.py
```

### Step 2: Navigate to Add Source
- Open browser: `http://localhost:5000` (or your Flask port)
- Click: **"Add New Source"** → **"API Data Source"**

### Step 3: Fill Form
```
┌─────────────────────────────────────────────┐
│ Source Name:      my_api                    │
│ API URL:          http://localhost:4000/api/data
│ Request Method:   GET                        │
│ Authentication:   None                       │
│ Data Path:        data          ← Important! │
│                                              │
│ Target Type:      ClickHouse                 │
│ Target Database:  test11                     │
│                                              │
│ ☑️ Enable Polling  ← CHECK THIS!            │
│    Poll Interval: 5 seconds                  │
│    ID Column:     id                         │
│                                              │
│ ❌ SSE Stream      ← LEAVE UNCHECKED         │
└─────────────────────────────────────────────┘
```

### Step 4: Test & Add
1. Click **"Test Connection"** (should show success ✅)
2. Click **"Add & Start Sync"**

### Step 5: Done! ✅
- Polling starts automatically
- Initial sync: All existing records
- Every 5 seconds: Syncs 5 new records

---

## 🔍 Verify It's Working

### Check Flask Logs
```
INFO:api_polling:📡 Poll #1: Fetching data from API...
INFO:api_polling:📊 Received 1245 total records from API
INFO:api_polling:✨ Found 1245 NEW records to sync
INFO:api_polling:✅ Inserted 1245 new records | Total synced: 1245
INFO:api_polling:⏳ Waiting 5.0s until next poll...

INFO:api_polling:📡 Poll #2: Fetching data from API...
INFO:api_polling:📊 Received 1250 total records from API
INFO:api_polling:✨ Found 5 NEW records to sync
INFO:api_polling:✅ Inserted 5 new records | Total synced: 1250
INFO:api_polling:⏳ Waiting 5.0s until next poll...
```

### Query ClickHouse
```sql
-- Total records
SELECT COUNT(*) FROM test11.my_api;

-- Sample data
SELECT id, Last_Name, Email FROM test11.my_api LIMIT 10;

-- Records by minute
SELECT 
    toStartOfMinute(_sync_timestamp) as minute,
    COUNT(*) as new_records
FROM test11.my_api
GROUP BY minute
ORDER BY minute DESC;
```

---

## ✅ What You Get

### ClickHouse Table
- **Database**: `test11`
- **Table**: `my_api`
- **Columns**: 7 total
  - `Converted_Date_Time`
  - `Email`
  - `Last_Name`
  - `id`
  - `Converted__s`
  - `_source_api` (auto-added)
  - `_sync_timestamp` (auto-added)

### Continuous Sync
- **Every 5 seconds**: Checks API for new records
- **Deduplication**: By `id` field
- **Only new records** are inserted
- **Runs forever** until stopped

---

## 🛑 How to Stop

### Option 1: Restart Flask
```bash
# Press Ctrl+C to stop Flask
# Restart when needed
python app.py
```

### Option 2: Delete Source (UI)
1. Go to homepage
2. Find "my_api" in the list
3. Click "Delete"

---

## ⚡ That's It!

Your localhost API is now continuously syncing to ClickHouse with:
- ✅ Automatic deduplication
- ✅ Only new records synced
- ✅ Every 5 seconds
- ✅ 100% data accuracy

**And all your other REST APIs (JSONPlaceholder, CoinGecko, etc.) continue to work as before!** 🎉

