# 🔑 How to Add OAuth API - Step-by-Step Guide

## ✅ OAUTH IS ALREADY IMPLEMENTED AND READY!

The system now **automatically handles OAuth tokens** for you. Here's exactly how to use it:

---

## 📝 Step-by-Step Process

### Step 1: Make Sure Flask is Running

```bash
python app.py
```

You should see:
```
* Running on http://127.0.0.1:5001
```

---

### Step 2: Open the Web Interface

Open your browser and go to:
```
http://127.0.0.1:5001
```

---

### Step 3: Click "Add Data Source"

Click the **"Add Data Source"** button or go directly to:
```
http://127.0.0.1:5001/add-source/api
```

---

### Step 4: Fill in the Form

Here's EXACTLY what to enter for your OAuth API:

#### Basic Information:
- **Source Name**: `my_oauth_api` (or any name you want)
- **API URL**: `http://localhost:4010/data` ← **This is your DATA endpoint**
- **HTTP Method**: `GET`

#### Authentication Type:
- **Select**: `OAuth 2.0 (Auto-Refresh)` ← **IMPORTANT: Select this!**

When you select OAuth, new fields will appear:

#### OAuth Configuration (These fields appear when you select OAuth):
- **OAuth Token URL**: `http://localhost:4010/pass` ← **This is your TOKEN endpoint**
- **OAuth Username**: `your_username` ← **Your actual username**
- **OAuth Password**: `your_password` ← **Your actual password**
- **Token Refresh Interval**: `3600` ← **3600 seconds = 1 hour**

#### Target Configuration:
- **Target Database Type**: `ClickHouse`
- **Target Database Name**: `test1` (or your database)
- **Data Path**: Leave empty (auto-detection will find it)

#### Continuous Sync (Optional but Recommended):
- ✅ **Enable Polling**: Check this box
- **Poll Interval**: `5` seconds
- **ID Column**: `id` (or whatever unique field your API has)

---

### Step 5: Test Connection (Optional but Recommended)

Click the **"Test Connection"** button.

The system will:
1. POST to `http://localhost:4010/pass` with your username/password
2. Get the OAuth token
3. Use it to GET `http://localhost:4010/data`
4. Show you if it works!

---

### Step 6: Add Source

Click **"Add Source"** button.

---

## 🎉 What Happens Automatically

Once you add the source, the system will:

### Minute 0:
1. ✅ POST to `http://localhost:4010/pass`
   ```json
   Request: {"username": "your_username", "password": "your_password"}
   Response: {"token": "abc123..."}
   ```

2. ✅ Store token in memory
3. ✅ Start background thread for auto-refresh
4. ✅ GET `http://localhost:4010/data` with token
   ```
   Headers: {"Authorization": "Bearer abc123..."}
   ```

5. ✅ Detect JSON structure
6. ✅ Create ClickHouse table
7. ✅ Insert data

### Every 5 Seconds:
1. ✅ Check if token is still valid
2. ✅ GET `/data` with current token
3. ✅ Insert new/updated records

### Every Hour (Before Token Expires):
1. ✅ POST to `/pass` to get new token
2. ✅ Replace old token
3. ✅ Continue syncing with new token

**YOU DO NOTHING!** It's all automatic! 🚀

---

## 📊 Verify It's Working

### Check Flask Logs

You'll see output like:
```
🔐 OAuth authentication enabled
🔑 Token URL: http://localhost:4010/pass
👤 Username: your_username
⏰ Token refresh interval: 3600s
🔄 Requesting new OAuth token from http://localhost:4010/pass
✅ OAuth token refreshed successfully (expires in 3600s)
📡 Poll #1: Fetching data from API...
🔑 Using OAuth token
✅ Upserted 10 records (new + updated)
```

### Check ClickHouse

```sql
-- See your data
SELECT count() FROM test1.my_oauth_api;
SELECT * FROM test1.my_oauth_api LIMIT 10;
```

---

## 🖼️ Visual Guide

### Screenshot 1: Select Authentication Type
```
┌─────────────────────────────────────┐
│ Authentication Type *               │
│ ┌─────────────────────────────────┐ │
│ │ OAuth 2.0 (Auto-Refresh)     ▼ │ │ ← SELECT THIS!
│ └─────────────────────────────────┘ │
└─────────────────────────────────────┘
```

### Screenshot 2: OAuth Fields Appear
```
┌─────────────────────────────────────────────────┐
│ 🔑 OAuth Token URL *                            │
│ ┌─────────────────────────────────────────────┐ │
│ │ http://localhost:4010/pass                  │ │ ← Token endpoint
│ └─────────────────────────────────────────────┘ │
│                                                 │
│ 👤 OAuth Username *                             │
│ ┌─────────────────────────────────────────────┐ │
│ │ admin                                       │ │ ← Your username
│ └─────────────────────────────────────────────┘ │
│                                                 │
│ 🔒 OAuth Password *                             │
│ ┌─────────────────────────────────────────────┐ │
│ │ ••••••                                      │ │ ← Your password
│ └─────────────────────────────────────────────┘ │
│                                                 │
│ ⏰ Token Refresh Interval (seconds)             │
│ ┌─────────────────────────────────────────────┐ │
│ │ 3600                                        │ │ ← 1 hour
│ └─────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────┘
```

### Screenshot 3: API URL (Your Data Endpoint)
```
┌─────────────────────────────────────────────────┐
│ API URL *                                       │
│ ┌─────────────────────────────────────────────┐ │
│ │ http://localhost:4010/data                  │ │ ← Data endpoint
│ └─────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────┘
```

---

## 🔍 Real Example with Your API

Based on what you told me:

### Your OAuth Token Endpoint:
```
POST http://localhost:4010/pass
Body: {
  "username": "your_actual_username",
  "password": "your_actual_password"
}
Response: {
  "token": "eyJhbGciOiJIUzI1NiIs..."  // or "access_token" or "oauth_token"
}
```

### Your Data Endpoints (Multiple):
```
GET http://localhost:4010/data
GET http://localhost:4010/users
GET http://localhost:4010/orders
etc.
```

All require:
```
Headers: {
  "Authorization": "Bearer eyJhbGciOiJIUzI1NiIs..."
}
```

---

## 🎯 How to Add Each Endpoint

### Add First API (http://localhost:4010/data)

1. **Source Name**: `oauth_data`
2. **API URL**: `http://localhost:4010/data`
3. **Auth Type**: `OAuth 2.0 (Auto-Refresh)`
4. **OAuth Token URL**: `http://localhost:4010/pass`
5. **OAuth Username**: `your_username`
6. **OAuth Password**: `your_password`
7. **Token Refresh**: `3600`
8. **Target Database**: `test1`
9. ✅ **Enable Polling** (if data changes over time)
10. Click **"Add Source"**

### Add Second API (http://localhost:4010/users)

1. **Source Name**: `oauth_users`
2. **API URL**: `http://localhost:4010/users`
3. **Auth Type**: `OAuth 2.0 (Auto-Refresh)`
4. **OAuth Token URL**: `http://localhost:4010/pass` ← **SAME!**
5. **OAuth Username**: `your_username` ← **SAME!**
6. **OAuth Password**: `your_password` ← **SAME!**
7. **Token Refresh**: `3600`
8. **Target Database**: `test1`
9. Click **"Add Source"**

**Both will use the SAME token manager!** No duplicate token requests! 🎯

---

## 💡 Key Points

### The System Automatically:
1. ✅ **POSTs** to `/pass` with username/password → gets token
2. ✅ **Stores** token in memory (thread-safe)
3. ✅ **Uses** token in all `/data` requests
4. ✅ **Refreshes** token before it expires
5. ✅ **Never asks you** to generate tokens manually

### You Only Provide:
- Token endpoint URL (`/pass`)
- Username
- Password
- Data endpoint URL (`/data`)

### System Does Everything Else:
- Token generation ✅
- Token storage ✅
- Token refresh ✅
- Using token in requests ✅
- Data sync ✅

---

## 🧪 Quick Test with Built-in Test Server

Want to test before using your real API?

### Terminal 1:
```bash
python test_oauth_server.py
```

This starts a test OAuth server with:
- Token URL: `http://localhost:4010/pass`
- Credentials: `admin` / `secret`
- Data URL: `http://localhost:4010/data`

### Terminal 2 (Flask):
```bash
python app.py
```

### Browser:
1. Go to `http://127.0.0.1:5001/add-source/api`
2. Fill in:
   - Auth Type: `OAuth 2.0 (Auto-Refresh)`
   - OAuth Token URL: `http://localhost:4010/pass`
   - Username: `admin`
   - Password: `secret`
   - API URL: `http://localhost:4010/data`
   - Target Database: `test1`
   - ✅ Enable Polling
3. Click "Add Source"

Watch Flask logs to see OAuth in action!

---

## 📊 Summary

**You Asked For:**
> "I cannot everytime generate the OTP... make such a way that everytime i get the data request and save the data in the clickhouse databases it 1st post a request for the get request to work"

**I Built:**
✅ OAuth Token Manager that:
- POSTs to `/pass` automatically
- Gets token
- Uses it in `/data` requests
- Refreshes before expiry
- **You NEVER manually generate tokens!**

**Process:**
```
[Every API Request]
1. System checks: Is token valid?
2. If expired → POST /pass → Get new token
3. Use token in GET /data
4. Parse response
5. Sync to ClickHouse
```

**You just provide username/password ONCE in the web form.**
**System handles everything else automatically forever!** 🎉

---

## 🚀 RESTART FLASK AND START ADDING APIs!

```bash
python app.py
```

Then add your OAuth APIs via: `http://127.0.0.1:5001/add-source/api`

**NO MORE MANUAL TOKEN GENERATION!** ✨

