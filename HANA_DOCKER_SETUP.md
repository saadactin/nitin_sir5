# HANA Docker Setup Guide

This guide will help you:
1. Pull and run SAP HANA Express Edition in Docker
2. Create a sample database with hospital data
3. Use it with your sync tool

## Prerequisites

- Docker Desktop installed and running
- Python 3.11+
- `hdbcli` Python package installed: `pip install hdbcli`
- `python-dotenv` installed: `pip install python-dotenv`

## Step 1: Update .env File

Add these HANA variables to your `.env` file:

```bash
# HANA Docker Configuration
HANA_HOST=localhost
HANA_PORT=39017
HANA_USERNAME=SYSTEM
HANA_PASSWORD=YourPassword123
```

## Step 2: Pull and Start HANA Container

Run this command to pull the HANA Express image and start the container:

```bash
docker-compose up -d
```

**Note**: The first time this runs, it will:
- Download the HANA Express image (several GB)
- Initialize the database (takes 5-10 minutes)
- Expose port 39017 for SQL connections

## Step 3: Wait for HANA to be Ready

Check the container logs:

```bash
docker logs -f hana-express
```

Wait until you see: `Startup finished!`

Or wait 5-10 minutes for initialization.

## Step 4: Create Sample Database

Run the setup script to create:
- Database: `HOSPITAL_DB`
- Schema: `HOSPITAL_SCHEMA`
- Table: `HOSPITALS` with 3 sample rows

```bash
python setup_hana_sample_db.py
```

This script will:
1. ✅ Wait for HANA to be ready
2. ✅ Connect using .env variables
3. ✅ Create database `HOSPITAL_DB`
4. ✅ Create schema `HOSPITAL_SCHEMA`
5. ✅ Create table `HOSPITALS` with hospital data structure
6. ✅ Insert 3 sample hospital records

## Step 5: Verify Setup

You can verify the data was created:

```bash
python -c "
from dotenv import load_dotenv
load_dotenv()
import os
import hdbcli.dbapi as hana_dbapi

config = {
    'host': os.environ.get('HANA_HOST'),
    'port': int(os.environ.get('HANA_PORT')),
    'username': os.environ.get('HANA_USERNAME'),
    'password': os.environ.get('HANA_PASSWORD')
}

conn = hana_dbapi.connect(**config)
cursor = conn.cursor()
cursor.execute('USE DATABASE HOSPITAL_DB')
cursor.execute('SELECT * FROM HOSPITAL_SCHEMA.HOSPITALS')
rows = cursor.fetchall()
print(f'Found {len(rows)} hospitals:')
for row in rows:
    print(f'  {row[1]} - {row[3]}, {row[4]}')
cursor.close()
conn.close()
"
```

## Using in Your Sync Tool

Once setup is complete, you can:

1. **Go to "Add HANA Source"** in your web UI
2. **Fill in connection details** (or leave empty to use .env):
   - Host: `localhost` (or leave empty)
   - Port: `39017` (or leave empty)
   - Username: `SYSTEM` (or leave empty)
   - Password: `YourPassword123` (or leave empty)
3. **Click "Test Connection"** - should populate database dropdown
4. **Select database**: `HOSPITAL_DB`
5. **Select schema**: `HOSPITAL_SCHEMA`
6. **Select tables**: `HOSPITALS`
7. **Sync to ClickHouse**!

## Container Management

### Stop HANA
```bash
docker-compose down
```

### Start HANA
```bash
docker-compose up -d
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
If port 39017 is already in use, change it in `.env`:
```bash
HANA_PORT=39018
```
Then update `docker-compose.yml` port mapping.

### Connection Refused
Wait a few more minutes for HANA to fully initialize. Check logs:
```bash
docker logs hana-express
```

### Wrong Password
If you need to reset password, stop container, remove volume, and restart:
```bash
docker-compose down -v
docker-compose up -d
```

## Sample Data Created

The `HOSPITALS` table contains:

1. **City General Hospital**
   - New York, NY
   - 500 beds
   - General Medicine, Cardiology, Surgery

2. **Sunset Medical Center**
   - Los Angeles, CA
   - 350 beds
   - Emergency Care, Orthopedics, Pediatrics

3. **Riverside Community Hospital**
   - Chicago, IL
   - 275 beds
   - Oncology, Neurology, Maternity

---

**All values come from .env - no hardcoded credentials!** ✅

