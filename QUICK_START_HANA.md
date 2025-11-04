# 🚀 Quick Start: HANA Docker Setup

## One-Command Setup

### Windows (PowerShell):
```powershell
# 1. Update .env with HANA settings
# 2. Start HANA
.\start_hana_docker.bat

# 3. Wait for HANA to initialize (5-10 min)
# Check logs: docker logs -f hana-express

# 4. Create sample database
python setup_hana_sample_db.py
```

### Manual Steps:

#### Step 1: Update .env
Add to `.env`:
```bash
HANA_HOST=localhost
HANA_PORT=39017
HANA_USERNAME=SYSTEM
HANA_PASSWORD=YourPassword123
```

#### Step 2: Pull and Start HANA
```bash
docker-compose up -d
```

#### Step 3: Wait for Ready
```bash
docker logs -f hana-express
```
Wait until you see "Startup finished!"

#### Step 4: Create Sample Data
```bash
python setup_hana_sample_db.py
```

## What Gets Created

✅ **Database**: `HOSPITAL_DB`
✅ **Schema**: `HOSPITAL_SCHEMA`  
✅ **Table**: `HOSPITALS`
✅ **Data**: 3 hospital records

All uses `.env` variables - **no hardcoded values!** ✅

## Verify

```bash
python -c "from setup_hana_sample_db import setup_hana_sample_data; setup_hana_sample_data()"
```

## Use in Sync Tool

1. Go to "Add HANA Source"
2. Click "Test Connection" (uses .env values)
3. Select `HOSPITAL_DB` → `HOSPITAL_SCHEMA` → `HOSPITALS`
4. Sync to ClickHouse! 🎉

