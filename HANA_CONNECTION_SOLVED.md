# ✅ HANA Connection - SOLVED!

## What Was Wrong

1. ❌ HANA container was **stopped** (now started ✅)
2. ❌ Wrong port: Used **39017**, but HANA Express uses **39013** (now fixed ✅)
3. ⏳ HANA is still **starting up** (wait 2-3 more minutes)

## ✅ Solution Applied

### 1. Started HANA Container
```bash
docker start hana-express
```
✅ Container is now running

### 2. Updated .env to Use Correct Port
Changed `.env`:
```bash
HANA_PORT=39013  # Changed from 39017
```

### 3. Wait for HANA to Finish Starting
HANA is still initializing. Wait 2-3 minutes, then check:
```bash
docker logs hana-express --tail 10
```
Look for: **"Startup finished!"**

## How to Test Connection Now

### Method 1: Leave Form Fields Empty (Easiest!)
1. Go to "Add HANA Source" form
2. **Leave ALL fields empty**
3. Click "Test Connection"
4. Form will use `.env` values automatically:
   - Host: localhost
   - Port: 39013 ✅
   - Username: SYSTEM
   - Password: YourPassword123

### Method 2: Enter Port Manually
1. Go to "Add HANA Source" form
2. Enter:
   - Host: `localhost`
   - **Port: `39013`** ✅ (not 39017!)
   - Username: `SYSTEM`
   - Password: `YourPassword123`
3. Click "Test Connection"

## Check if HANA is Ready

```bash
docker logs hana-express --tail 5
```

When you see **"Startup finished!"**, HANA is ready.

## Summary

✅ HANA container is running  
✅ Port updated to 39013 in .env  
⏳ Wait 2-3 minutes for full startup  
✅ Then test connection with port 39013 or leave fields empty!

---

**Next Step:** Wait 2-3 minutes, then click "Test Connection" (leave fields empty to use .env)!

