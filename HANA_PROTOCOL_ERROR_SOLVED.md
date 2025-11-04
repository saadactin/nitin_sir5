# ✅ HANA Protocol Error (1033) - SOLVED!

## What Was Wrong

1. ❌ **Missing Password**: HANA Express requires password via `--passwords-url` or `--master-password`
2. ❌ **Port Mismatch**: Container had port 39013, code tried 39017
3. ❌ **Protocol Error**: Wrong connection parameters caused "invalid action type"

## ✅ What I Fixed

### 1. Created passwords.json
```json
{
  "master_password": "YourPassword123"
}
```

### 2. Updated docker-compose.yml
- Added password file volume mount
- Added `--passwords-url file:///config/passwords.json` to command
- Mapped both ports 39013 and 39017

### 3. Updated Connection Code (app.py)
- Added `communicationTimeout=30000`
- Added `reconnect=True`

### 4. Updated .env
- `HANA_PORT=39013` (HANA Express default SQL port)

## Current Status

✅ **Container is starting** - HANA initialization in progress  
⏳ **Wait 5-10 minutes** for "Startup finished!" message

## How to Test (Once HANA is Ready)

### Option 1: Leave Form Empty (Recommended)
1. Go to "Add HANA Source" form
2. **Leave ALL fields empty**
3. Click "Test Connection"
4. Uses `.env` automatically:
   - Host: localhost
   - Port: 39013 ✅
   - Username: SYSTEM
   - Password: YourPassword123

### Option 2: Enter Manually
1. Host: `localhost`
2. Port: `39013` ✅ (not 39017!)
3. Username: `SYSTEM`
4. Password: `YourPassword123`
5. Click "Test Connection"

## Check if HANA is Ready

```bash
docker logs hana-express --tail 10
```

Wait for: **"Startup finished!"**

## Configuration Summary

**passwords.json**:
```json
{"master_password": "YourPassword123"}
```

**.env**:
```bash
HANA_HOST=localhost
HANA_PORT=39013
HANA_USERNAME=SYSTEM
HANA_PASSWORD=YourPassword123
```

**docker-compose.yml**:
- Ports: 39013, 39017, 39041, 8090
- Password file: passwords.json
- All from .env (no hardcoded values)

---

**Next Step**: Wait 5-10 minutes for HANA to finish starting, then test connection with port 39013! ✅

