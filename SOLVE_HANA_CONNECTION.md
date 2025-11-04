# Solve HANA Connection Error

## Error Message
```
Connection failed: target machine actively refused it (localhost:39017)
```

## Problem
- ✅ Port is now correct (39017)
- ❌ **HANA container is NOT running**
- Connection refused = nothing listening on port 39017

## Solution Steps

### Step 1: Check if HANA Container Exists
```bash
docker ps -a --filter "name=hana"
```

### Step 2: Check if HANA Image is Available
```bash
docker images | findstr "hana"
```

### Step 3: Start HANA Container
If container exists but is stopped:
```bash
docker start hana-express
```

If container doesn't exist, need to create it:
```bash
docker-compose up -d
```

### Step 4: Wait for HANA to Initialize
```bash
docker logs -f hana-express
```
Wait for: **"Startup finished!"** (takes 5-10 minutes)

### Step 5: Verify Container is Running
```bash
docker ps --filter "name=hana-express"
```
Should show status "Up" and ports mapped.

### Step 6: Test Connection Again
1. Go back to form
2. Click "Test Connection"
3. Should work now!

## Alternative: Use External HANA

If you have HANA running elsewhere (not Docker):

1. **Find your HANA port**:
   - Check your HANA installation
   - Common ports: 30015, 30013, 39017
   
2. **Update .env**:
   ```bash
   HANA_HOST=your_hana_host
   HANA_PORT=your_hana_port
   HANA_USERNAME=SYSTEM
   HANA_PASSWORD=your_password
   ```

3. **Leave form fields empty** to use .env values
4. **Test Connection**

## Quick Check Commands

```powershell
# Check if anything is listening on port 39017
netstat -an | findstr "39017"

# Check Docker containers
docker ps -a

# Start HANA if exists
docker start hana-express

# Check HANA logs
docker logs hana-express --tail 20
```

## If HANA Docker Image Not Available

HANA Express Docker requires:
1. Download from SAP website
2. Free SAP Developer account
3. Manual image load

OR use your existing HANA installation with correct host/port.

