# Fix HANA Connection Error

## Problem
You're getting: "Connection failed... actively refused it (localhost:30017)"

## Issues Identified

### Issue 1: Wrong Port
You entered **port 30017**, but HANA Express Docker uses **port 39017**.

### Issue 2: HANA Container May Not Be Running
The connection is being refused, which usually means:
- HANA container isn't running, OR
- HANA hasn't finished initializing yet

## Solution

### Step 1: Check If HANA Container Is Running
```bash
docker ps -a --filter "name=hana-express"
```

If container is not running:
```bash
docker-compose up -d
```

### Step 2: Use Correct Port
In the form, change:
- **Port: 30017** ❌
- **Port: 39017** ✅

### Step 3: Wait for HANA to Initialize
Check if HANA is ready:
```bash
docker logs -f hana-express
```

Wait until you see: **"Startup finished!"**

This can take **5-10 minutes** on first run.

### Step 4: Verify .env Settings
Make sure your `.env` has:
```bash
HANA_HOST=localhost
HANA_PORT=39017
HANA_USERNAME=SYSTEM
HANA_PASSWORD=YourPassword123
```

### Step 5: Test Connection Again
1. Go to "Add HANA Source" form
2. Enter:
   - **Host**: localhost (or leave empty to use .env)
   - **Port**: 39017 (or leave empty to use .env)
   - **Username**: SYSTEM (or leave empty to use .env)
   - **Password**: YourPassword123 (or leave empty to use .env)
3. Click "Test Connection"

## Quick Fix Commands

```bash
# 1. Start HANA if not running
docker-compose up -d

# 2. Check status
docker ps --filter "name=hana-express"

# 3. Check logs
docker logs hana-express --tail 20

# 4. Wait for "Startup finished!" message
```

## Why Port 39017?

- **HANA Express Edition** (Docker) uses port **39017**
- **Regular HANA** uses port **30015**
- Your Docker setup exposes **39017** (see docker-compose.yml)

## Alternative: Leave Fields Empty

If you've set `.env` correctly, you can **leave all fields empty** in the form:
- The form will use values from `.env` automatically
- Port will be taken from `HANA_PORT=39017` in .env

---

**Summary: Change port from 30017 to 39017, or leave empty to use .env!**

