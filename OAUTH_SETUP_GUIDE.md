# OAuth 2.0 Auto-Refresh Setup Guide

## ✅ OAUTH INTEGRATION COMPLETE!

Your system now supports **automatic OAuth token management** with auto-refresh!

## 🔑 How It Works

1. **POST to Token URL** → Get OAuth token
2. **Auto-refresh** every N seconds (default: 1 hour)
3. **Use token** in API requests automatically
4. **Never worry** about expired tokens!

## 📋 Setup Instructions

### Method 1: Via Web Interface (Recommended)

1. **Go to**: `http://127.0.0.1:5001/add-source/api`

2. **Fill in Basic Info**:
   - Source Name: `My OAuth API`
   - API URL: `http://localhost:4010/data`
   - Request Method: `GET`

3. **Select Authentication Type**: `OAuth 2.0 (Auto-Refresh)`

4. **Fill OAuth Details**:
   - **OAuth Token URL**: `http://localhost:4010/pass`
   - **OAuth Username**: `your_username`
   - **OAuth Password**: `your_password`
   - **Token Refresh Interval**: `3600` (1 hour in seconds)

5. **Configure Target**:
   - Target Database Type: `ClickHouse`
   - Target Database: `test1`

6. **Enable Continuous Sync** (Optional):
   - ☑️ Enable Polling
   - Poll Interval: `5` seconds
   - ID Column: `id`

7. **Click**: "Test Connection" → "Add Source"

### Method 2: Programmatically

```python
from oauth_token_manager import get_oauth_token

# Get token (auto-managed)
token = get_oauth_token(
    token_url="http://localhost:4010/pass",
    username="your_username",
    password="your_password",
    refresh_interval=3600  # 1 hour
)

# Use token in API request
import requests
headers = {"Authorization": f"Bearer {token}"}
response = requests.get("http://localhost:4010/data", headers=headers)
```

## 🔄 Auto-Refresh Features

### Automatic Token Refresh
- ✅ **Background thread** refreshes token automatically
- ✅ **60 seconds before expiry** → new token requested
- ✅ **Thread-safe** for concurrent API calls
- ✅ **Reuses managers** for same token URL

### Token Manager Caching
- First API source creates the token manager
- Subsequent sources with **same token URL** reuse it
- **Single refresh thread** per unique token URL
- Memory efficient!

## 🧪 Testing

### Step 1: Create Test OAuth Server

```python
# test_oauth_server.py
from flask import Flask, request, jsonify
import uuid
from datetime import datetime

app = Flask(__name__)

@app.route('/pass', methods=['POST'])
def get_token():
    """OAuth token endpoint"""
    data = request.json
    username = data.get('username')
    password = data.get('password')
    
    # Validate credentials (example)
    if username == 'admin' and password == 'secret':
        token = str(uuid.uuid4())  # Generate random token
        return jsonify({
            'token': token,
            'expires_in': 3600
        })
    
    return jsonify({'error': 'Invalid credentials'}), 401

@app.route('/data', methods=['GET'])
def get_data():
    """Protected API endpoint"""
    auth_header = request.headers.get('Authorization')
    
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Unauthorized'}), 401
    
    # Return some data
    return jsonify({
        'data': [
            {'id': 1, 'name': 'Item 1', 'value': 100},
            {'id': 2, 'name': 'Item 2', 'value': 200},
            {'id': 3, 'name': 'Item 3', 'value': 300}
        ]
    })

if __name__ == '__main__':
    app.run(port=4010, debug=True)
```

### Step 2: Start Test Server

```bash
python test_oauth_server.py
```

### Step 3: Add OAuth API Source

Use the web interface as described above, or restart Flask:

```bash
python app.py
```

The auto-start feature will begin polling with OAuth!

## 📊 Verify It's Working

### Check Flask Logs

You should see:
```
🔐 OAuth authentication enabled
🔑 Token URL: http://localhost:4010/pass
👤 Username: admin
⏰ Token refresh interval: 3600s
🔄 Requesting new OAuth token from http://localhost:4010/pass
✅ OAuth token refreshed successfully (expires in 3600s)
📡 Poll #1: Fetching data from API...
🔑 Using OAuth token (expires soon: check manager)
```

### Check ClickHouse

```sql
SELECT count() FROM test1.my_oauth_api;
SELECT * FROM test1.my_oauth_api LIMIT 10;
```

## ⚙️ Configuration Options

| Parameter | Description | Default | Example |
|-----------|-------------|---------|---------|
| `oauth_token_url` | URL to POST for token | Required | `http://localhost:4010/pass` |
| `oauth_username` | Username for OAuth | Required | `admin` |
| `oauth_password` | Password for OAuth | Required | `secret` |
| `oauth_refresh_interval` | Token lifetime (seconds) | 3600 | 3600 = 1 hour |

## 🔍 Token Response Formats Supported

The system automatically detects tokens in these formats:

```json
// Format 1: "token" field
{"token": "abc123..."}

// Format 2: "access_token" field
{"access_token": "abc123..."}

// Format 3: "oauth_token" field
{"oauth_token": "abc123..."}

// Format 4: Any string field > 20 chars
{"custom_token_field": "abc123..."}
```

## 🛠️ Advanced Usage

### Multiple APIs with Same OAuth

```python
# Both use the same token manager automatically!
# Source 1
poll_api_to_clickhouse(
    api_url="http://localhost:4010/data",
    auth_type="oauth",
    oauth_token_url="http://localhost:4010/pass",
    oauth_username="admin",
    oauth_password="secret",
    ...
)

# Source 2 (reuses the token manager from Source 1)
poll_api_to_clickhouse(
    api_url="http://localhost:4010/users",
    auth_type="oauth",
    oauth_token_url="http://localhost:4010/pass",  # Same URL!
    oauth_username="admin",
    oauth_password="secret",
    ...
)
```

### Custom Token Refresh Intervals

Different token lifetimes:
- **1 minute**: `60`
- **5 minutes**: `300`
- **1 hour**: `3600` (default)
- **2 hours**: `7200`
- **1 day**: `86400`

### Manual Token Refresh

```python
from oauth_token_manager import get_token_manager

manager = get_token_manager(
    "http://localhost:4010/pass",
    "admin",
    "secret",
    3600
)

# Force refresh now
manager.refresh_token()

# Get current token
token = manager.get_token()
```

## 🎯 Real-World Example

### Scenario: CRM API with OAuth

```
POST http://localhost:8080/auth/token
Body: {"username": "crm_user", "password": "crm_pass"}
Response: {"access_token": "eyJhbGc...", "expires_in": 3600}

GET http://localhost:8080/api/customers
Headers: {"Authorization": "Bearer eyJhbGc..."}
Response: {"data": [...]}
```

### Configuration:

1. **OAuth Token URL**: `http://localhost:8080/auth/token`
2. **OAuth Username**: `crm_user`
3. **OAuth Password**: `crm_pass`
4. **Token Refresh**: `3600`
5. **API URL**: `http://localhost:8080/api/customers`
6. **Data Path**: `data`

## ✨ Benefits

1. **Zero Manual Work**: Tokens refresh automatically
2. **No Expired Tokens**: Refreshes 60 seconds before expiry
3. **Thread-Safe**: Multiple API calls use the same token safely
4. **Memory Efficient**: Token managers are reused
5. **Continuous Sync**: Works with polling mode
6. **Error Handling**: Retries on token failures

## 🎉 Summary

**OAuth integration is complete and fully automated!**

- ✅ Add OAuth APIs via web interface
- ✅ Tokens auto-refresh every N seconds
- ✅ Works with continuous polling
- ✅ Thread-safe and efficient
- ✅ Supports multiple token response formats
- ✅ No manual token management needed

**Just configure once and forget about tokens forever!** 🚀

