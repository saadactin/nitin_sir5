# 🚀 Quick Start: Add OAuth API in 2 Minutes

## ✅ Your System is Ready!

OAuth support is **fully implemented**. Just follow these steps:

---

## 📝 Simple 5-Step Process

### 1. Start Flask
```bash
python app.py
```

### 2. Open Browser
```
http://127.0.0.1:5001/add-source/api
```

### 3. Fill Form

| Field | Value | Example |
|-------|-------|---------|
| **Source Name** | Any name | `my_oauth_api` |
| **API URL** | Your DATA endpoint | `http://localhost:4010/data` |
| **Auth Type** | **OAuth 2.0 (Auto-Refresh)** | ← **Select from dropdown!** |
| **OAuth Token URL** | Your TOKEN endpoint | `http://localhost:4010/pass` |
| **OAuth Username** | Your username | `your_username` |
| **OAuth Password** | Your password | `your_password` |
| **Token Refresh** | Seconds until expiry | `3600` (1 hour) |
| **Target Database** | ClickHouse database | `test1` |
| **Enable Polling** | ✅ Check if continuous | ✅ |
| **Poll Interval** | Seconds between polls | `5` |

### 4. Click "Add Source"

### 5. Done!

The system now:
- ✅ Auto-generates tokens
- ✅ Auto-refreshes every hour
- ✅ Syncs data continuously
- ✅ You do NOTHING!

---

## 🔍 What Happens Behind the Scenes

```
Every 5 seconds:
├─ Check token validity
├─ If needed: POST /pass → get new token
├─ GET /data with token
├─ Parse JSON
├─ Insert into ClickHouse
└─ Repeat!

Every hour:
├─ POST /pass → refresh token
└─ Continue syncing with new token
```

---

## 📊 Check Your Data

```sql
-- Row count
SELECT count() FROM test1.my_oauth_api;

-- Latest records
SELECT * FROM test1.my_oauth_api 
ORDER BY _sync_timestamp DESC 
LIMIT 10;
```

---

## 🎯 For Multiple APIs with Same OAuth

If you have multiple endpoints using the same OAuth:

### API 1: /data
- OAuth Token URL: `http://localhost:4010/pass`
- Username: `admin`
- Password: `secret`
- API URL: `http://localhost:4010/data`

### API 2: /users  
- OAuth Token URL: `http://localhost:4010/pass` ← **SAME!**
- Username: `admin` ← **SAME!**
- Password: `secret` ← **SAME!**
- API URL: `http://localhost:4010/users` ← Different endpoint

**System reuses the same token manager!** Both use the same token! 🎯

---

## ✨ That's It!

**No scripts to run. No manual tokens. No complexity.**

Just fill the form and the system handles everything! 🚀

---

## 📚 More Details

For comprehensive documentation, see:
- `HOW_TO_ADD_OAUTH_API.md` - Detailed guide
- `OAUTH_FLOW_DIAGRAM.txt` - Visual flow
- `OAUTH_SETUP_GUIDE.md` - Technical details

