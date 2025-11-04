# Fix HANA Port Issue

## Problem Found
Your HANA container is starting, but:
- ❌ Port **39017** is NOT mapped to host
- ✅ Port **39013** IS mapped to host

HANA Express uses **port 39013** for SQL connections, not 39017!

## Solution

### Option 1: Use Port 39013 (Quick Fix)
1. In "Add HANA Source" form
2. Change **Port** from `39017` to `39013`
3. Click "Test Connection"

### Option 2: Update .env and Use It
1. Update `.env`:
   ```bash
   HANA_PORT=39013
   ```

2. Leave form fields empty
3. Click "Test Connection" (will use port 39013 from .env)

### Option 3: Recreate Container with Port 39017
If you need port 39017, stop and recreate container:

```bash
docker stop hana-express
docker rm hana-express
docker-compose up -d
```

But HANA Express typically uses 39013, so Option 1 is recommended.

## Wait for HANA to Finish Starting

The container shows "health: starting" - wait 2-3 minutes for:
- HANA to fully initialize
- "Startup finished!" message

Check status:
```bash
docker logs hana-express --tail 10
```

## Quick Fix Steps

1. **Wait 2-3 minutes** for HANA to finish starting
2. Change **Port** from `39017` to `39013` in form
3. Click **"Test Connection"**
4. Should work! ✅

