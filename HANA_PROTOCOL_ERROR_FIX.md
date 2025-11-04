# Fix HANA Protocol Error (1033)

## Error
```
(1033, 'error while parsing protocol: invalid action type')
```

## Root Cause
The HANA container was created with **port 39013** mapped, but the code was trying to connect to **port 39017**. This caused a protocol mismatch.

## Solution Applied

### 1. Updated docker-compose.yml
- Added both ports 39013 and 39017
- Using existing image: `saplabs/hanaexpress:latest`

### 2. Recreated Container
- Stopped old container
- Removed it
- Created new one with correct port mappings

### 3. Updated .env
- Set `HANA_PORT=39013` (HANA Express default)

### 4. Updated Connection Code
- Added `communicationTimeout` parameter
- Added `reconnect=True` parameter

## Next Steps

### Wait for HANA to Start
Container is recreating. Wait 2-3 minutes for:
```bash
docker logs hana-express
```
Look for: **"Startup finished!"**

### Test Connection
1. **Leave all form fields empty** (uses .env automatically)
2. Click "Test Connection"
3. Should work with port 39013! ✅

## Port Information

HANA Express Edition uses:
- **39013** - SQL port (default)
- **39017** - Alternative SQL port  
- **39041** - HTTP port
- **8090** - XS Advanced

The form will use **39013** from `.env`.

