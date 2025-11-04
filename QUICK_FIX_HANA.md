# ⚡ Quick Fix: HANA Connection Error

## Problem
```
Connection failed: actively refused it (localhost:30017)
```

## Two Issues

### ❌ Issue 1: Wrong Port
- **You entered**: 30017
- **Should be**: 39017

### ❌ Issue 2: Container Not Running
- HANA container is **stopped**
- Need to start it first

## ✅ Solution (3 Steps)

### Step 1: Start HANA Container
```bash
docker-compose up -d
```

### Step 2: Wait for HANA to Initialize
```bash
docker logs -f hana-express
```
Wait until you see: **"Startup finished!"** (takes ~5 minutes)

### Step 3: Use Correct Port in Form
1. Go back to "Add HANA Source" form
2. Change **Port** from `30017` to `39017`
3. Click "Test Connection"

## OR: Leave Fields Empty!

Since you have `.env` configured:
1. **Leave Host field empty** → uses `HANA_HOST=localhost` from .env
2. **Leave Port field empty** → uses `HANA_PORT=39017` from .env  
3. **Leave Username empty** → uses `HANA_USERNAME=SYSTEM` from .env
4. **Leave Password empty** → uses `HANA_PASSWORD=YourPassword123` from .env

Then click "Test Connection" - it will use `.env` values automatically!

## Current Status

✅ `.env` is configured correctly  
⚠️ HANA container needs to be started  
⚠️ Use port **39017** (not 30017)

---

**Quick Command:**
```bash
docker-compose up -d
```
Then wait 5 minutes and test again with port **39017**!

