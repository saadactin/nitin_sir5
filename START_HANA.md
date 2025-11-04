# How to Start HANA and Fix Connection

## Immediate Fix

### Change Port in Form
1. In "Add HANA Source" form
2. Change **Port** from `30017` to `39017`
3. Click "Test Connection"

## OR: Use .env Values (Easier!)

Since your `.env` is already configured with:
```bash
HANA_HOST=localhost
HANA_PORT=39017
HANA_USERNAME=SYSTEM
HANA_PASSWORD=YourPassword123
```

Just **leave all form fields empty** and click "Test Connection"!
- Form will automatically use `.env` values
- No need to type anything!

## If You Want to Start HANA Docker

### Note About HANA Express Docker
HANA Express Docker image may require:
- Manual download from SAP website
- SAP Developer account (free)
- Or use existing HANA installation

### Try Starting Container
```bash
docker-compose up -d
```

If image is not available, you'll need to:
1. Download from SAP website: https://developers.sap.com/tutorials/hxe-ua-install-using-docker.html
2. Or use your existing HANA installation at the port you specified

## Quick Summary

**EASIEST FIX:**
- Leave all form fields empty
- Form uses `.env` automatically (port 39017)
- Click "Test Connection"

**OR:**
- Change Port from `30017` to `39017` in form
- Click "Test Connection"

