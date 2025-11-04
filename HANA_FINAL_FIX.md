# ✅ HANA Connection - Final Fix

## Issue Fixed
HANA Express requires password via `--master-password` flag, not just environment variable.

## What I Did

### 1. Updated docker-compose.yml
Added `--master-password` to command:
```yaml
command: --agree-to-sap-license --master-password=${HANA_PASSWORD:-YourPassword123}
```

### 2. Recreated Container
- Stopped old container
- Removed it  
- Created new one with password flag

### 3. Updated Connection Code
- Added `communicationTimeout=30000`
- Added `reconnect=True`

### 4. Port Configuration
- Both 39013 and 39017 are mapped
- `.env` uses port 39013 (HANA Express default)

## Next Steps

### Wait for HANA to Initialize
HANA is starting fresh. Wait **5-10 minutes** for full initialization.

Check status:
```bash
docker logs -f hana-express
```

Look for: **"Startup finished!"**

### Test Connection
1. Go to "Add HANA Source" form
2. **Leave ALL fields empty** (will use .env)
3. Click "Test Connection"
4. Should work! ✅

## Configuration Summary

**.env** (already set):
```bash
HANA_HOST=localhost
HANA_PORT=39013
HANA_USERNAME=SYSTEM
HANA_PASSWORD=YourPassword123
```

**docker-compose.yml**:
- Ports: 39013, 39017, 39041, 8090
- Password: YourPassword123 (from .env)

**Connection**:
- Uses port 39013 from .env
- All parameters from .env (no hardcoded values)

---

**Status**: Container recreating with correct password configuration. Wait 5-10 minutes, then test!

