# ✅ HANA Docker Setup - Complete!

## What Was Created

### 1. Docker Configuration
- ✅ `docker-compose.yml` - HANA Express Edition container
- ✅ Uses SAP HANA Express (free version)
- ✅ Port 39017 exposed for SQL connections
- ✅ Password from `.env` file (YourPassword123)

### 2. Setup Scripts
- ✅ `setup_hana_env.py` - Adds HANA settings to `.env`
- ✅ `setup_hana_sample_db.py` - Creates sample hospital database
- ✅ `start_hana_docker.bat` - Windows batch script to start HANA

### 3. Sample Database
When you run `setup_hana_sample_db.py`, it creates:
- **Database**: `HOSPITAL_DB`
- **Schema**: `HOSPITAL_SCHEMA`
- **Table**: `HOSPITALS`
- **Data**: 3 hospital records

## Quick Start

### Step 1: Setup .env (Already Done!)
```bash
python setup_hana_env.py
```
This adds to `.env`:
```bash
HANA_HOST=localhost
HANA_PORT=39017
HANA_USERNAME=SYSTEM
HANA_PASSWORD=YourPassword123
```

### Step 2: Start HANA Container
```bash
docker-compose up -d
```

**First time**: Downloads ~6GB image and initializes (5-10 minutes)

### Step 3: Wait for HANA to be Ready
```bash
docker logs -f hana-express
```
Wait until you see: `Startup finished!`

### Step 4: Create Sample Database
```bash
python setup_hana_sample_db.py
```

This will:
1. ✅ Connect to HANA using `.env` variables
2. ✅ Create `HOSPITAL_DB` database
3. ✅ Create `HOSPITAL_SCHEMA` schema
4. ✅ Create `HOSPITALS` table
5. ✅ Insert 3 hospital records

## Sample Data

The `HOSPITALS` table contains:

| ID | Name | City | State | Beds | Speciality |
|----|------|------|-------|------|------------|
| 1 | City General Hospital | New York | NY | 500 | General Medicine, Cardiology, Surgery |
| 2 | Sunset Medical Center | Los Angeles | CA | 350 | Emergency Care, Orthopedics, Pediatrics |
| 3 | Riverside Community Hospital | Chicago | IL | 275 | Oncology, Neurology, Maternity |

## Use in Your Sync Tool

1. **Go to "Add HANA Source"** in web UI
2. **Click "Test Connection"** - it will use `.env` values automatically
3. **Select Database**: `HOSPITAL_DB`
4. **Select Schema**: `HOSPITAL_SCHEMA`
5. **Select Table**: `HOSPITALS`
6. **Target**: ClickHouse database
7. **Sync!** 🎉

## Important Notes

✅ **All uses .env** - No hardcoded values
✅ **Password**: YourPassword123 (as specified)
✅ **Host**: localhost (Docker container)
✅ **Port**: 39017 (standard HANA Express port)
✅ **Username**: SYSTEM (default admin)

## Container Management

### Start HANA
```bash
docker-compose up -d
```

### Stop HANA
```bash
docker-compose down
```

### View Logs
```bash
docker logs -f hana-express
```

### Remove Container (keeps data)
```bash
docker-compose down
```

### Remove Container and Data
```bash
docker-compose down -v
```

## Troubleshooting

### Port Already in Use
If port 39017 is in use, change in `.env`:
```bash
HANA_PORT=39018
```
Then update `docker-compose.yml` port mapping.

### Connection Refused
Wait longer - HANA takes 5-10 minutes to initialize on first run.

### Wrong Password
The password is set to `YourPassword123` in `.env`. Make sure it matches.

## Files Created

1. `docker-compose.yml` - Docker configuration
2. `setup_hana_env.py` - .env setup script
3. `setup_hana_sample_db.py` - Database creation script
4. `start_hana_docker.bat` - Windows start script
5. `HANA_DOCKER_SETUP.md` - Detailed documentation
6. `QUICK_START_HANA.md` - Quick reference

## Summary

✅ Free HANA Express Edition Docker image configured
✅ All settings use `.env` file (no hardcoded values)
✅ Sample hospital database ready
✅ Password: YourPassword123
✅ Ready to sync to ClickHouse!

**Everything uses `.env` variables - no hardcoded values!** ✅

