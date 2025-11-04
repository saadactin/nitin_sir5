# ✅ HANA Docker Problem - SOLVED!

## Problem Found

**Protocol Error (1033)** when connecting to port 39013

## Root Cause

HANA Express uses **different ports** for different services:
- **Port 39013**: Different service (not SQL)
- **Port 39017**: ✅ **SQL connection port** (this works!)

## Solution

**Use port 39017 instead of 39013!**

### What Was Fixed

1. ✅ **Tested port 39017** - Connection works!
2. ✅ **Updated .env** - Changed `HANA_PORT=39013` to `HANA_PORT=39017`

## Port Mapping

**docker-compose.yml** maps both ports:
- Port 39013: Mapped ✅ (but not for SQL)
- Port 39017: Mapped ✅ (SQL port - this is what we need!)

## Current Configuration

**.env**:
```bash
HANA_HOST=localhost
HANA_PORT=39017  ✅ (Changed from 39013)
HANA_USERNAME=SYSTEM
HANA_PASSWORD=YourPassword123
```

**docker-compose.yml**:
- Port 39017 is mapped ✅
- Container is running and healthy ✅

## Test Results

✅ **Port 39017**: Connection successful!
- User: SYSTEM
- Timestamp: Retrieved successfully

## How to Use Now

### In Web UI:
1. Go to "Add HANA Source"
2. **Leave all fields empty** (uses .env with port 39017)
3. Click "Test Connection"
4. Should work! ✅

### Or Manually:
- Host: `localhost`
- Port: `39017` ✅ (not 39013!)
- Username: `SYSTEM`
- Password: `YourPassword123`

## Port Reference

| Port | Service | Use For |
|------|---------|---------|
| 39013 | ? | Not SQL |
| **39017** | **SQL** | **Python hdbcli connections** ✅ |
| 39041 | HTTP | HTTP interface |
| 8090 | XS Advanced | Web services |

## Summary

✅ **Container**: Running and healthy  
✅ **Port 39017**: Works for SQL connections  
✅ **.env**: Updated to port 39017  
✅ **Connection**: Working!  

**The problem was using port 39013 instead of 39017!**

---

**Now you can:**
1. ✅ Test connection in web UI
2. ✅ Run incremental sync test
3. ✅ Sync HANA data to ClickHouse

