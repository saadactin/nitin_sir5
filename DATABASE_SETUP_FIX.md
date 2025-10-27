# Database Setup Fix

## ❌ Problem
You're getting this error:
```
FATAL: database "test1" does not exist
```

## ✅ Solution

### **Option 1: Create the Database (Recommended)**

Run this command:
```powershell
python create_database.py
```

This will:
- ✓ Create the "test1" database in PostgreSQL
- ✓ Create the "metrics_sync_tables" schema
- ✓ Set up everything automatically

Then start the app:
```powershell
python app.py
```

---

### **Option 2: Change Database Name in Config**

If you want to use a different database that already exists:

1. Edit `config/db_connections.yaml`
2. Change this line:
   ```yaml
   postgresql:
     database: test1  # ← Change this to your existing database name
   ```
3. Save the file
4. Run `python app.py`

---

## 🔍 What the Error Means

The app is trying to connect to a PostgreSQL database called "test1" (configured in `config/db_connections.yaml`), but that database doesn't exist on your PostgreSQL server.

---

## ✅ Quick Fix Commands

```powershell
# Step 1: Create the database
python create_database.py

# Step 2: Start the app
python app.py
```

---

## 📋 Manual Database Creation (Alternative)

If you prefer to create the database manually using PostgreSQL:

```sql
-- Connect to PostgreSQL (using psql or pgAdmin)
CREATE DATABASE test1;

-- Connect to test1 database
\c test1

-- Create schema
CREATE SCHEMA metrics_sync_tables;
```

Then run:
```powershell
python app.py
```

---

## 🎯 Summary

**The easiest solution:**
1. Run `python create_database.py` (one time only)
2. Run `python app.py` (starts your application)

Done! ✅
