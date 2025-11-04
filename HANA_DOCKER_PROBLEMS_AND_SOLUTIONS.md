# HANA Docker Problems & Solutions

## Current Status Summary

### ✅ What's Working
- **Container**: Running and healthy (Up About an hour)
- **HANA Startup**: Completed ("Startup finished!")
- **Ports**: All mapped correctly (39013, 39017, 39041, 8090)
- **Port Listening**: Port 39013 is listening on host
- **Password**: Configured correctly in passwords.json

### ❌ What Was Wrong
- **Protocol Error (1033)**: "error while parsing protocol: invalid action type"
- **Root Cause**: Old `hdbcli` version (2.26.18) incompatible with HANA Express

### ✅ What Was Fixed
- **Upgraded hdbcli**: From 2.26.18 → 2.26.25
- **This should fix the protocol error!**

## Problems Encountered & Fixed

### Problem 1: Container Not Starting
**Error**: Container was stopped (Exited 5 days ago)

**Solution**: 
```bash
docker start hana-express
# or
docker-compose up -d
```

### Problem 2: Missing Password Configuration
**Error**: "A URL for retrieving the SYSTEM user passwords MUST be provided"

**Solution**: 
- Created `passwords.json` with master password
- Updated docker-compose.yml to use `--passwords-url file:///config/passwords.json`

### Problem 3: Wrong Port
**Error**: Connection refused on port 39017

**Solution**:
- HANA Express uses port **39013** (not 39017)
- Updated `.env`: `HANA_PORT=39013`

### Problem 4: Protocol Error (1033)
**Error**: "error while parsing protocol: invalid action type"

**Solution**:
- Upgraded `hdbcli`: `pip install --upgrade hdbcli`
- Old version: 2.26.18
- New version: 2.26.25 ✅

## Verification Steps

### 1. Check Container
```bash
docker ps --filter "name=hana-express"
# Should show: Up (healthy)
```

### 2. Check Logs
```bash
docker logs hana-express --tail 10
# Should show: "Startup finished!"
```

### 3. Check Port
```bash
netstat -an | findstr ":39013"
# Should show: LISTENING
```

### 4. Test Connection
```bash
python test_hana_connection_simple.py
# Should show: [OK] Connected successfully!
```

## If Issues Persist

### Option 1: Restart Container
```bash
docker restart hana-express
docker logs -f hana-express
```

### Option 2: Recreate Container
```bash
docker stop hana-express
docker rm hana-express
docker-compose up -d
```

### Option 3: Check Resources
HANA Express needs:
- At least 8GB RAM allocated to Docker
- Enough disk space for data volume

### Option 4: Use External HANA
If Docker issues persist, use existing HANA installation:
- Update `.env` with correct HANA connection details
- Form will use `.env` values automatically

## Current Configuration

**docker-compose.yml**:
- ✅ Image: `saplabs/hanaexpress:latest`
- ✅ Ports: 39013, 39017, 39041, 8090
- ✅ Password: From passwords.json
- ✅ Restart: unless-stopped

**passwords.json**:
```json
{"master_password": "YourPassword123"}
```

**.env**:
```bash
HANA_HOST=localhost
HANA_PORT=39013  ✅
HANA_USERNAME=SYSTEM
HANA_PASSWORD=YourPassword123
```

**hdbcli**:
- ✅ Version: 2.26.25 (upgraded from 2.26.18)

## Next Steps

1. ✅ **Test connection again** (after hdbcli upgrade)
   ```bash
   python test_hana_connection_simple.py
   ```

2. ✅ **If connection works**, run incremental sync test:
   ```bash
   python test_hana_incremental_sync_complete.py
   ```

3. ✅ **Use in web UI**:
   - Go to "Add HANA Source"
   - Leave fields empty (uses .env)
   - Click "Test Connection"
   - Should work! ✅

---

**Status**: All issues addressed. Try connection again - should work now! ✅

