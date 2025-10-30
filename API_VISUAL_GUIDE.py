"""
Visual Guide: API Source Feature
Shows what the user sees at each step
"""

print("""
╔═══════════════════════════════════════════════════════════════════════════════╗
║                     API DATA SOURCE - VISUAL GUIDE                            ║
╚═══════════════════════════════════════════════════════════════════════════════╝

STEP 1: ADD SOURCE PAGE
═══════════════════════════════════════════════════════════════════════════════

┌───────────────────────────────────────────────────────────────────────────┐
│  [SQL Server]  [SAP HANA]  [API] ← Click this tab                         │
├───────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  Source Name *                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │ CRM API                                                              │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                                                            │
│  API URL/Endpoint *                                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │ http://localhost:3000/api/crm/stream                                │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                                                            │
│  Authentication Type *                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │ Bearer Token                                    ▼                    │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                                                            │
│  Bearer Token *                                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │ eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...                             │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                                                            │
│  Target Database Type *                                                    │
│  ◉ PostgreSQL     ○ ClickHouse                                            │
│                                                                            │
│  Target Database Name *                                                    │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │ crm_sync_db                                                          │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                                                            │
│                                                                            │
│  ┌──────────────────┐    ┌────────────────────┐                          │
│  │ Test Connection  │    │  Add API Source    │                          │
│  └──────────────────┘    └────────────────────┘                          │
└───────────────────────────────────────────────────────────────────────────┘


STEP 2: CLICK "TEST CONNECTION"
═══════════════════════════════════════════════════════════════════════════════

┌───────────────────────────────────────────────────────────────────────────┐
│  Testing connection...                                                     │
│  ┌────────────────────────────────────────────────────────────────────┐   │
│  │ ✓ API is reachable! Status: 200                                    │   │
│  │   Response time: 245ms                                             │   │
│  │   Content-Type: application/json                                   │   │
│  └────────────────────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────────────────┘


STEP 3: SUBMIT FORM
═══════════════════════════════════════════════════════════════════════════════

┌───────────────────────────────────────────────────────────────────────────┐
│  ✓ API source "CRM API" added successfully!                                │
│  Redirecting to homepage...                                                │
└───────────────────────────────────────────────────────────────────────────┘


STEP 4: HOMEPAGE - API SOURCE CARD
═══════════════════════════════════════════════════════════════════════════════

╔═══════════════════════════════════════════════════════════════════════════╗
║                         SQL SERVERS DASHBOARD                             ║
╠═══════════════════════════════════════════════════════════════════════════╣
║                                                                            ║
║  ┌──────────────────────────┐  ┌──────────────────────────┐              ║
║  │ server1                  │  │ CRM API             API  │ ← NEW CARD   ║
║  ├──────────────────────────┤  ├──────────────────────────┤              ║
║  │                          │  │                          │              ║
║  │ Host: localhost          │  │ Host: localhost:3000     │              ║
║  │ Status: ● Online         │  │ Status: ● Online         │              ║
║  │                          │  │                          │              ║
║  ├──────────────────────────┤  ├──────────────────────────┤              ║
║  │ [Databases] [Sync]       │  │ [Databases] [Sync]       │              ║
║  │ [Edit] [Delete]          │  │ [Edit] [Delete]          │              ║
║  └──────────────────────────┘  └──────────────────────────┘              ║
║                                                                            ║
╚═══════════════════════════════════════════════════════════════════════════╝


AUTHENTICATION TYPES
═══════════════════════════════════════════════════════════════════════════════

1. NONE (Public API)
┌───────────────────────────────────────────────────────────────────────────┐
│  Authentication Type: None                                                 │
│  (No additional fields shown)                                              │
│                                                                            │
│  Example: http://localhost:3000/api/public/data                           │
└───────────────────────────────────────────────────────────────────────────┘

2. BEARER TOKEN
┌───────────────────────────────────────────────────────────────────────────┐
│  Authentication Type: Bearer Token                                         │
│  Bearer Token: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...                     │
│                                                                            │
│  Sends: Authorization: Bearer <token>                                      │
└───────────────────────────────────────────────────────────────────────────┘

3. API KEY
┌───────────────────────────────────────────────────────────────────────────┐
│  Authentication Type: API Key                                              │
│  Header Name: X-API-Key                                                    │
│  Header Value: sk_live_1234567890abcdef                                    │
│                                                                            │
│  Sends: X-API-Key: sk_live_1234567890abcdef                                │
└───────────────────────────────────────────────────────────────────────────┘


STATUS INDICATORS
═══════════════════════════════════════════════════════════════════════════════

ONLINE:   ● Green dot + "Online"     (API responds with HTTP 200)
OFFLINE:  ● Red dot + "Offline"      (API timeout, error, or non-200 status)


DATABASE TABLE STRUCTURE
═══════════════════════════════════════════════════════════════════════════════

data_sources table:
┌────┬────────────┬────────────┬────────────────────┬──────────────────────────┐
│ id │ source_name│ source_type│ server_address     │ connection_details       │
├────┼────────────┼────────────┼────────────────────┼──────────────────────────┤
│ 8  │ CRM API    │ api        │ localhost:3000/... │ {"auth_type": "bearer",  │
│    │            │            │                    │  "bearer_token": "...",  │
│    │            │            │                    │  "http_method": "GET"}   │
└────┴────────────┴────────────┴────────────────────┴──────────────────────────┘


TEST SCENARIOS
═══════════════════════════════════════════════════════════════════════════════

✓ Public API (no auth)
  URL: https://jsonplaceholder.typicode.com/users
  Auth: None
  Result: ✓ 200 OK - 10 records fetched

✓ Bearer Token API
  URL: http://localhost:3000/api/crm/stream
  Auth: Bearer Token
  Result: ✓ 200 OK - Authenticated successfully

✓ API Key Authentication
  URL: https://api.example.com/data
  Auth: X-API-Key
  Result: ✓ 200 OK - Key validated

✗ Invalid URL
  URL: http://invalid-url:9999/api
  Auth: None
  Result: ✗ Connection timeout

✗ Wrong Token
  URL: http://localhost:3000/api/protected
  Auth: Bearer wrong_token
  Result: ✗ 401 Unauthorized


ERROR HANDLING
═══════════════════════════════════════════════════════════════════════════════

Timeout:          "Connection timeout after 10 seconds"
Invalid URL:      "Invalid URL format"
401 Unauthorized: "Authentication failed: Invalid credentials"
404 Not Found:    "API endpoint not found"
500 Server Error: "API server error: 500"
Network Error:    "Network error: Cannot reach API"


═══════════════════════════════════════════════════════════════════════════════
                           FEATURE IS READY TO USE!
═══════════════════════════════════════════════════════════════════════════════

Open your browser: http://127.0.0.1:5001
Login as: admin / admin123
Navigate to: Add Source → API Tab
""")
