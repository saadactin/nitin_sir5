# API Polling Setup Guide

## 🎯 Overview

Your localhost API at `http://localhost:4000/api/data` that adds 5 new rows every 5 seconds is now supported with **Polling Mode**.

## ✅ What is Polling Mode?

**Polling Mode** continuously checks your API at regular intervals (e.g., every 5 seconds) and automatically syncs **only the NEW records** to ClickHouse.

### How it works:
1. First poll: Fetches all existing records (e.g., 1245 records)
2. Creates ClickHouse table with proper schema
3. Inserts all records
4. Waits 5 seconds
5. Second poll: Fetches API again, finds 5 new records
6. Inserts only the 5 new records (deduplicates by ID)
7. Repeats forever until stopped

---

## 🚀 Setup via Web UI

### Step 1: Navigate to Add API Source
1. Go to your Flask app
2. Click "Add New Source" → "API Data Source"

### Step 2: Fill in API Details
```
Source Name:      my_api_data
API URL:          http://localhost:4000/api/data
Request Method:   GET
Authentication:   None
Data Path:        data
```

**Important**: Set "Data Path" to `data` because your API returns:
```json
{
  "data": [array of records],
  "info": {metadata}
}
```

### Step 3: Select Target
```
Target Type:      ClickHouse
Target Database:  test11  (or any database you want)
```

### Step 4: Enable Polling Mode ✅
```
☑️ Enable Polling (Check API every N seconds)
   Poll Interval:  5 seconds
   ID Column:      id
```

**Leave unchecked**:
- ❌ Server-Sent Events (SSE) Stream

### Step 5: Test & Add
1. Click "Test Connection" to verify API is reachable
2. Click "Add & Start Sync"
3. Done! Polling starts automatically in the background

---

## 📊 Expected Behavior

### Initial Sync
```
Poll #1:
- Fetched 1245 total records from API
- Found 1245 NEW records
- Created table: test11.my_api_data
- Inserted 1245 records ✅
- Waiting 5s until next poll...
```

### Subsequent Polls
```
Poll #2 (after 5 seconds):
- Fetched 1250 total records from API
- Found 5 NEW records (deduplication by 'id' column)
- Inserted 5 new records ✅
- Total synced: 1250
- Waiting 5s until next poll...

Poll #3 (after 10 seconds):
- Fetched 1255 total records from API
- Found 5 NEW records
- Inserted 5 new records ✅
- Total synced: 1255
- Waiting 5s until next poll...

... (continues forever)
```

---

## 🔧 Configuration Options

### Poll Interval
- **Default**: 5 seconds
- **Min**: 1 second
- **Max**: 3600 seconds (1 hour)
- **Recommendation**: Match your API's data update frequency

### ID Column
- **Default**: `id`
- **Purpose**: Used to identify unique records for deduplication
- **Must be unique** for each record
- For your API: Use `id` (the field like "3652397000009118497")

### Data Path
- **For your API**: Set to `data`
- Extracts the array from `response['data']`
- Leave empty if API returns array directly

---

## 📋 ClickHouse Table Schema

Your API data will create a table like this:

```sql
CREATE TABLE test11.my_api_data (
    Converted_Date_Time Nullable(String),
    Email Nullable(String),
    Last_Name Nullable(String),
    id Nullable(String),
    Converted__s UInt8,
    _source_api Nullable(String),
    _sync_timestamp DateTime64(3)
) ENGINE = MergeTree()
ORDER BY tuple();
```

### Columns:
- `Converted_Date_Time` - Date from API
- `Email` - User email (nullable)
- `Last_Name` - User last name
- `id` - Unique identifier
- `Converted__s` - Boolean field
- `_source_api` - Source API URL (auto-added)
- `_sync_timestamp` - When record was synced (auto-added)

---

## ✅ Verification

### Check Sync Status
Watch the Flask logs to see polling in action:
```
INFO:api_polling:📡 Poll #1: Fetching data from API...
INFO:api_polling:📊 Received 1245 total records from API
INFO:api_polling:✨ Found 1245 NEW records to sync
INFO:api_polling:✅ Created table test11.my_api_data with 7 columns
INFO:api_polling:✅ Inserted 1245 new records | Total synced: 1245
INFO:api_polling:⏳ Waiting 5.0s until next poll...
```

### Query ClickHouse
```sql
-- Check total records
SELECT COUNT(*) FROM test11.my_api_data;

-- See sample data
SELECT id, Last_Name, Email, Converted_Date_Time 
FROM test11.my_api_data 
LIMIT 10;

-- Check sync timestamps
SELECT 
    COUNT(*) as records,
    toStartOfMinute(_sync_timestamp) as minute
FROM test11.my_api_data
GROUP BY minute
ORDER BY minute DESC
LIMIT 10;
```

---

## 🆚 Comparison: Polling vs SSE vs REST

| Feature | REST (One-Time) | Polling | SSE |
|---------|----------------|---------|-----|
| **Use Case** | Static data | Data grows over time | Real-time streams |
| **Example** | JSONPlaceholder users | Your localhost API | Stock tickers (rare) |
| **Sync Frequency** | Once | Every N seconds | Continuous |
| **Connection** | Short-lived | Repeated short requests | Long-lived connection |
| **Content-Type** | application/json | application/json | text/event-stream |
| **CPU Usage** | Low | Medium | Low |
| **Best For** | Full data dumps | Incremental updates | Event-driven data |

### Which Should You Use?

**Your API (`http://localhost:4000/api/data`)**:
- ✅ **Use Polling Mode**
- Why? It's a regular JSON API that adds records over time
- Content-Type: `application/json`
- Returns: `{"data": [records], "info": {...}}`

**JSONPlaceholder (`/users`)**:
- ✅ **Use REST (One-Time)**
- Why? Static data that doesn't change
- Just need to sync once

**True SSE Stream**:
- ✅ **Use SSE Mode**
- Why? Server pushes events as they happen
- Content-Type: `text/event-stream`
- Format: `data: {...}\n\n`

---

## 🛑 Stopping Polling

Polling runs as a background daemon thread. To stop it:

### Method 1: Restart Flask App
```bash
# Stop the Flask app (Ctrl+C)
# Start it again
python app.py
```

### Method 2: Delete Source (UI)
1. Go to homepage
2. Find your API source
3. Click "Delete" button
4. Confirm deletion

### Method 3: Mark Inactive (Database)
```sql
-- In PostgreSQL
UPDATE data_sources 
SET is_active = FALSE 
WHERE source_name = 'my_api_data';
```

---

## 📈 Performance Tips

### Optimize Poll Interval
- **Too Fast** (< 1s): Wastes resources
- **Just Right** (5-10s): Balanced
- **Too Slow** (> 60s): Delayed data

### Monitor Resource Usage
- Check CPU usage of Flask app
- Check ClickHouse insert performance
- Adjust poll_interval if needed

### Handle Large Datasets
- If API returns 10,000+ records, consider:
  - Increasing poll_interval
  - Using pagination (if API supports it)
  - Syncing during off-peak hours

---

## 🎉 Complete Example

Here's the complete configuration for your localhost API:

```yaml
Source Configuration:
  source_name: "crm_data"
  api_url: "http://localhost:4000/api/data"
  request_method: "GET"
  auth_type: "none"
  data_path: "data"
  
Target Configuration:
  target_type: "ClickHouse"
  target_database: "test11"
  target_table: "crm_data"
  
Polling Configuration:
  polling_mode: true
  poll_interval: 5
  id_column: "id"
  
Advanced:
  is_sse: false  # This is NOT an SSE stream
```

---

## 🆘 Troubleshooting

### Issue: "No new records found" every poll
**Cause**: ID column might be wrong or data isn't actually changing
**Solution**: 
1. Check your API - are records really being added?
2. Verify `id_column` matches your data (should be "id")
3. Check ClickHouse - `SELECT COUNT(*) FROM test11.my_api_data`

### Issue: Duplicate records in ClickHouse
**Cause**: ID column not set correctly
**Solution**: 
1. Drop the table: `DROP TABLE test11.my_api_data`
2. Restart sync with correct `id_column` setting

### Issue: Polling stopped
**Cause**: Flask app crashed or was restarted
**Solution**: 
1. Check Flask logs for errors
2. Restart Flask app
3. Polling will resume automatically

---

## ✅ Summary

1. **Your API**: `http://localhost:4000/api/data`
2. **Mode**: Polling ✅
3. **Interval**: 5 seconds
4. **Data Path**: `data`
5. **ID Column**: `id`
6. **Result**: New records automatically synced every 5 seconds!

All existing REST APIs (JSONPlaceholder, CoinGecko, etc.) continue to work as before with one-time sync. 🎉

