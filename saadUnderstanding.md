# 🚀 Complete Project Deployment Guide - SQL Sync Dashboard

## 📋 Table of Contents
1. [Project Overview](#project-overview)
2. [Prerequisites & System Requirements](#prerequisites--system-requirements)
3. [Step-by-Step Installation](#step-by-step-installation)
4. [Configuration Files Setup](#configuration-files-setup)
5. [Database Setup](#database-setup)
6. [Starting the Application](#starting-the-application)
7. [Initial Setup & First Sync](#initial-setup--first-sync)
8. [Troubleshooting](#troubleshooting)
9. [Production Deployment](#production-deployment)

---

## 📦 Project Overview

**SQL Sync Dashboard** is a Flask-based web application that:
- Syncs data from multiple SQL Server instances to PostgreSQL
- Provides real-time monitoring and analytics dashboards
- Supports scheduled syncs (interval-based and daily)
- Includes role-based access control (Admin, Operator, Viewer)
- Sends email notifications for sync events
- Handles 100+ servers with optimized performance

---

## 🔧 Prerequisites & System Requirements

### Required Software

#### 1. **Python 3.8 or Higher**
- Download: https://www.python.org/downloads/
- During installation: ✅ Check "Add Python to PATH"
- Verify installation:
  ```powershell
  python --version
  # Should show: Python 3.8.x or higher
  ```

#### 2. **PostgreSQL 12 or Higher**
- Download: https://www.postgresql.org/download/
- During installation:
  - Set password for `postgres` user (remember this!)
  - Default port: 5432
  - ✅ Install "Stack Builder" for additional components
- Verify installation:
  ```powershell
  psql --version
  # Should show: psql (PostgreSQL) 12.x or higher
  ```

#### 3. **ODBC Driver 17 for SQL Server**
- Download: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
- **CRITICAL:** This is required for connecting to SQL Server
- Choose: "ODBC Driver 17 for SQL Server" (64-bit)
- Verify installation:
  ```powershell
  # Open: Control Panel → Administrative Tools → ODBC Data Sources (64-bit)
  # Check "Drivers" tab for "ODBC Driver 17 for SQL Server"
  ```

#### 4. **Git (Optional but Recommended)**
- Download: https://git-scm.com/download/win
- Used for version control and updates

---

## 📥 Step-by-Step Installation

### Step 1: Extract Project Files

```powershell
# Extract the ZIP file to your desired location
# Example: C:\Projects\sql-sync-dashboard
cd C:\Projects\sql-sync-dashboard
```

**Project Structure:**
```
sql-sync-dashboard/
├── app.py                          # Main Flask application
├── requirements.txt                # Python dependencies
├── config/
│   └── db_connections.yaml         # Database configuration (YOU EDIT THIS)
├── .env                           # Environment variables (CREATE THIS)
├── templates/                      # HTML templates
├── static/                         # CSS, JS, images
├── utils/
│   └── email_service.py           # Email notifications
└── ... (other Python files)
```

### Step 2: Create Python Virtual Environment

**Why?** Isolates project dependencies from system Python.

```powershell
# Navigate to project directory
cd C:\Projects\sql-sync-dashboard

# Create virtual environment
python -m venv venv

# Activate virtual environment
.\venv\Scripts\Activate.ps1

# You should see (venv) at the start of your prompt
# If you get an error about execution policy, run:
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Step 3: Install Python Dependencies

```powershell
# Make sure virtual environment is activated (you see "(venv)")
pip install --upgrade pip

# Install all required packages
pip install -r requirements.txt

# This installs:
# - Flask (web framework)
# - psycopg2 (PostgreSQL driver)
# - pyodbc (SQL Server driver)
# - pandas (data processing)
# - schedule (job scheduling)
# - bcrypt (password hashing)
# - PyYAML (YAML config files)
# - python-dotenv (environment variables)
```

**Troubleshooting pip install:**
- If `psycopg2` fails: Try `pip install psycopg2-binary`
- If `pyodbc` fails: Ensure ODBC Driver 17 is installed first

### Step 4: Verify ODBC Driver Installation

```powershell
# Test ODBC Driver availability
python -c "import pyodbc; print(pyodbc.drivers())"

# Should include: 'ODBC Driver 17 for SQL Server'
# If not found, reinstall ODBC Driver 17 from Microsoft
```

---

## ⚙️ Configuration Files Setup

### Configuration File 1: `.env` (Environment Variables)

**Location:** Create in project root directory

**Purpose:** Stores sensitive credentials and settings

```powershell
# Create .env file
notepad .env
```

**Required Content:**

```env
# ============================================
# POSTGRESQL DATABASE CONFIGURATION
# ============================================
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=test1
POSTGRES_USER=migration_user
POSTGRES_PASSWORD=StrongPassword123

# ============================================
# EMAIL NOTIFICATION SETTINGS
# ============================================
# Gmail SMTP Configuration
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
EMAIL_USER=your-email@gmail.com
EMAIL_PASSWORD=your-app-password

# Administrator email addresses (comma-separated)
ADMIN_EMAIL_ADDRESSES=admin1@company.com,admin2@company.com

# ============================================
# OPTIONAL SETTINGS
# ============================================
# Cache TTL for Sync Summary page (seconds)
SYNC_SUMMARY_CACHE_TTL=60

# Flask settings
FLASK_ENV=development
FLASK_DEBUG=True
SECRET_KEY=your-secret-key-here-change-this
```

**🔐 IMPORTANT NOTES:**

1. **PostgreSQL Credentials:**
   - `POSTGRES_DB`: Name of target database (will be created)
   - `POSTGRES_USER`: PostgreSQL user (will be created)
   - `POSTGRES_PASSWORD`: Choose a strong password

2. **Email Setup (Gmail):**
   - `EMAIL_USER`: Your Gmail address
   - `EMAIL_PASSWORD`: **NOT your Gmail password!**
   - You need an "App Password":
     1. Go to: https://myaccount.google.com/apppasswords
     2. Create app password for "Mail"
     3. Use that 16-character password here

3. **Security:**
   - **NEVER commit `.env` to Git!**
   - Already in `.gitignore`
   - Change `SECRET_KEY` to random string

---

### Configuration File 2: `config/db_connections.yaml`

**Location:** `config/db_connections.yaml`

**Purpose:** Defines SQL Server connections and PostgreSQL settings

```powershell
# Edit the configuration file
notepad config\db_connections.yaml
```

**Template:**

```yaml
# ============================================
# POSTGRESQL TARGET CONFIGURATION
# ============================================
postgresql:
  host: localhost              # PostgreSQL server address
  port: 5432                   # PostgreSQL port
  database: test1              # Must match .env POSTGRES_DB
  username: migration_user     # Must match .env POSTGRES_USER
  password: StrongPassword123  # Must match .env POSTGRES_PASSWORD

# ============================================
# SQL SERVER SOURCE CONNECTIONS
# ============================================
sqlservers:
  
  # SERVER 1 - Example Production Server
  prod_server1:
    server: 192.168.1.100              # SQL Server hostname or IP
    port: 1433                         # SQL Server port (default: 1433)
    username: sa                       # SQL Server username
    password: YourSQLPassword123       # SQL Server password
    target_postgres_db: test1          # Which PostgreSQL DB to sync to
    skip_databases:                    # Databases to exclude from sync
      - tempdb
      - model
      - msdb
    
  # SERVER 2 - Example Local Development Server
  dev_server:
    server: localhost\SQL2019           # Named instance format
    port: null                          # null for default port with named instance
    username: windows                   # Use 'windows' for Windows Authentication
    password: windows                   # Use 'windows' for Windows Authentication
    target_postgres_db: test1
    skip_databases:
      - AdventureWorks
      - SampleDB
  
  # SERVER 3 - Example Remote Server
  remote_server:
    server: sql.company.com
    port: 1433
    username: sync_user
    password: SecurePassword456
    target_postgres_db: test1
    skip_databases: []

# ============================================
# AUTHENTICATION METHODS
# ============================================
# SQL Server Authentication:
#   username: actual_username
#   password: actual_password
#
# Windows Authentication:
#   username: windows
#   password: windows
#
# Trusted Connection:
#   username: trusted
#   password: trusted
```

**🔧 Configuration Explained:**

#### PostgreSQL Section:
```yaml
postgresql:
  host: localhost              # Where is PostgreSQL? (localhost = same machine)
  port: 5432                   # PostgreSQL port
  database: test1              # Target database name
  username: migration_user     # Database user for syncing
  password: StrongPassword123  # User password
```

#### SQL Server Section:

**Each server needs:**
1. **Unique name** (e.g., `prod_server1`, `dev_server`)
2. **Connection details:**
   - `server`: Hostname/IP or named instance
   - `port`: 1433 (default) or custom
3. **Authentication:**
   - SQL Auth: Real username/password
   - Windows Auth: `windows`/`windows`
4. **Target PostgreSQL database**
5. **Databases to skip** (optional)

**Connection String Formats:**

| Format | Example | When to Use |
|--------|---------|-------------|
| Hostname | `sqlserver.company.com` | Remote server |
| IP Address | `192.168.1.100` | Known IP |
| localhost | `localhost` | Same machine |
| Named Instance | `localhost\SQL2019` | SQL Server named instance |
| Port | Use `port: 1433` | Non-default port |

---

## 🗄️ Database Setup

### Step 1: Create PostgreSQL User and Database

**Option A: Using psql Command Line**

```powershell
# Open psql as postgres superuser
psql -U postgres -h localhost

# In psql prompt, run these commands:
```

```sql
-- Create the migration user
CREATE USER migration_user WITH PASSWORD 'StrongPassword123';

-- Create the target database
CREATE DATABASE test1 OWNER migration_user;

-- Grant all privileges
GRANT ALL PRIVILEGES ON DATABASE test1 TO migration_user;

-- Connect to the database
\c test1

-- Grant schema privileges
GRANT ALL ON SCHEMA public TO migration_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO migration_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO migration_user;

-- Exit psql
\q
```

**Option B: Using pgAdmin (GUI)**

1. Open pgAdmin 4
2. Connect to PostgreSQL Server (localhost)
3. Right-click "Login/Group Roles" → Create → Login/Group Role
   - Name: `migration_user`
   - Definition → Password: `StrongPassword123`
   - Privileges → ✅ Can login
4. Right-click "Databases" → Create → Database
   - Database: `test1`
   - Owner: `migration_user`

### Step 2: Initialize Application Database Schema

The application will automatically create required tables on first run:
- `metrics_sync_tables.sync_history` - Sync execution logs
- `metrics_sync_tables.schedules` - Scheduled jobs
- `metrics_sync_tables.users` - Application users

**These are created automatically - no manual setup needed!**

### Step 3: Run Database Optimization (For Production)

```powershell
# After first sync, optimize for production scale
python optimize_production_db.py
```

This creates indexes for fast queries with 100+ servers.

---

## 🚀 Starting the Application

### First-Time Startup

```powershell
# 1. Make sure virtual environment is activated
.\venv\Scripts\Activate.ps1

# 2. Navigate to project directory
cd C:\Projects\sql-sync-dashboard

# 3. Start the Flask application
python app.py
```

**Expected Output:**

```
[SCHEDULER] Initializing scheduler system...
======================================================================
[SCHEDULER] RESTORING SCHEDULES FROM DATABASE
======================================================================
[SCHEDULER] No active schedules found in database
======================================================================

Warning: Could not load .env file: ...
 * Serving Flask app 'app'
 * Debug mode: off
WARNING: This is a development server. Use a production WSGI server instead.
 * Running on http://127.0.0.1:5001
Press CTRL+C to quit
```

**Application is now running!** 🎉

### Accessing the Application

**Open your web browser:**
```
http://127.0.0.1:5001
```

**You'll see the login page** (first user creation required)

---

## 👤 Initial Setup & First Sync

### Step 1: Create Admin User

**First-time access automatically redirects to user creation:**

1. Visit: http://127.0.0.1:5001
2. You'll be redirected to: http://127.0.0.1:5001/create-user
3. Fill in the form:
   - **Username:** admin
   - **Password:** YourSecurePassword123
   - **Email:** your-email@company.com
   - **Role:** Admin
4. Click "Create User"

**✅ Admin user created!** You'll receive a confirmation email.

### Step 2: Login

1. Use your admin credentials to login
2. You'll see the main dashboard

### Step 3: Configure SQL Servers (If Not Done in YAML)

**Option A: Already Configured in `db_connections.yaml`**
- Servers will appear on the dashboard automatically
- Skip to Step 4

**Option B: Add via Web Interface**
1. Click **"Add SQL Server"** button
2. Fill in connection details:
   - Server Name (friendly name)
   - Server Address (hostname/IP)
   - Port (1433)
   - Username
   - Password
   - Target PostgreSQL DB
3. Click **"Test Connection"** to verify
4. Click **"Save"**

### Step 4: Run Your First Sync

**Manual Sync (Recommended for First Time):**

1. On the dashboard, you'll see your configured SQL Servers
2. Each server has a **"Sync Now"** button
3. Click **"Sync Now"** on a server
4. Watch the sync progress in real-time
5. You'll see:
   - Databases being processed
   - Tables being synced
   - Row counts
   - Completion status

**Monitor Progress:**
```
=== MIGRATION STARTED for server: prod_server1 ===
[INFO] Found 5 databases
=== DATABASE START: SalesDB ===
[DB COMPLETE] INCR SalesDB: 12 tables

=== MIGRATION COMPLETE for server: prod_server1 ===
[COMPLETE] 5/5 databases synced
```

### Step 5: Schedule Automatic Syncs (Optional)

**Option A: Interval-Based Schedule**
1. Click **"Schedule"** in sidebar
2. Select server
3. Choose "Interval"
4. Set minutes (e.g., 60 for hourly)
5. Click "Create Schedule"

**Option B: Daily Schedule**
1. Click **"Schedule"** in sidebar
2. Select server
3. Choose "Daily"
4. Set time (e.g., 02:00 AM)
5. Click "Create Schedule"

**✅ Schedule is now active!** Will run automatically.

### Step 6: Verify Sync Results

**Check Advanced Analytics Dashboard:**
1. Click **"Advanced Analytics"** in sidebar
2. You should see:
   - Total Syncs Today: 1 (or more)
   - Success Rate: 100%
   - Server Status: Your servers listed
   - Recent Activity: Sync operations
   - Charts with data

**Check Sync Summary:**
1. Click **"Sync Summary"** in sidebar
2. Compare SQL Server vs PostgreSQL row counts
3. Verify data accuracy

---

## 🔍 Troubleshooting

### Issue 1: Cannot Connect to PostgreSQL

**Error:** `FATAL: database "test1" does not exist`

**Solution:**
```powershell
# Create the database
psql -U postgres -h localhost -c "CREATE DATABASE test1"

# Or run the setup script
python create_database.py
```

---

### Issue 2: Cannot Connect to SQL Server

**Error:** `'ODBC Driver 17 for SQL Server' not found`

**Solution:**
1. Download ODBC Driver 17: https://go.microsoft.com/fwlink/?linkid=2223300
2. Install (64-bit version)
3. Restart PowerShell
4. Verify:
   ```powershell
   python -c "import pyodbc; print(pyodbc.drivers())"
   ```

---

### Issue 3: Email Notifications Not Working

**Error:** Email not sending

**Solution:**
1. **Check Gmail Settings:**
   - Enable 2-Factor Authentication
   - Generate App Password: https://myaccount.google.com/apppasswords
   - Use App Password in `.env`, not your Gmail password

2. **Verify `.env` Configuration:**
   ```env
   SMTP_SERVER=smtp.gmail.com
   SMTP_PORT=587
   EMAIL_USER=your-email@gmail.com
   EMAIL_PASSWORD=your-16-char-app-password
   ```

3. **Test Email:**
   ```powershell
   python send_test_email.py
   ```

---

### Issue 4: Port 5001 Already in Use

**Error:** `Address already in use`

**Solution:**
1. **Change Port:** Edit `app.py`
   ```python
   if __name__ == "__main__":
       app.run(host='127.0.0.1', port=5002)  # Change to 5002
   ```

2. **Or Kill Existing Process:**
   ```powershell
   # Find process using port 5001
   netstat -ano | findstr :5001
   
   # Kill the process (replace PID)
   taskkill /F /PID <PID>
   ```

---

### Issue 5: Windows Authentication Not Working

**Error:** Login failed for user

**Solution:**
In `config/db_connections.yaml`:
```yaml
sqlservers:
  myserver:
    server: localhost\SQL2019
    port: null
    username: windows     # Must be exactly 'windows'
    password: windows     # Must be exactly 'windows'
    target_postgres_db: test1
```

---

### Issue 6: Tables Not Syncing

**Possible causes:**

1. **No Primary Key or Sync Column:**
   - SQL Server tables need a primary key, timestamp, or identity column
   - Check logs for: "No suitable sync column found"

2. **Excluded Schema:**
   - System schemas are excluded (sys, information_schema)
   - Check `table_filters.py` for exclusions

3. **Empty Tables:**
   - Empty tables sync successfully with 0 rows

---

### Issue 7: Slow Sync Performance

**Solutions:**

1. **Enable Incremental Sync:**
   - Tables with timestamp/rowversion columns sync faster
   - Only changed rows are transferred

2. **Optimize PostgreSQL:**
   ```powershell
   python optimize_production_db.py
   ```

3. **Reduce Sync Frequency:**
   - Don't sync every 5 minutes
   - Use hourly or daily schedules

4. **Exclude Large Tables:**
   ```yaml
   skip_databases:
     - LargeArchiveDB
     - LoggingDatabase
   ```

---

## 🌐 Production Deployment

### For Production Environment:

#### 1. Use Production WSGI Server (NOT Flask Dev Server)

**Install Gunicorn (Linux) or Waitress (Windows):**

```powershell
# For Windows
pip install waitress

# For Linux
pip install gunicorn
```

**Start Production Server:**

```powershell
# Windows (Waitress)
waitress-serve --host=0.0.0.0 --port=5001 app:app

# Linux (Gunicorn)
gunicorn -w 4 -b 0.0.0.0:5001 app:app
```

#### 2. Update `.env` for Production

```env
FLASK_ENV=production
FLASK_DEBUG=False
SECRET_KEY=generate-strong-random-key-here
```

#### 3. Set Up Systemd Service (Linux)

Create `/etc/systemd/system/sql-sync.service`:

```ini
[Unit]
Description=SQL Sync Dashboard
After=network.target postgresql.service

[Service]
Type=simple
User=www-data
WorkingDirectory=/opt/sql-sync-dashboard
Environment="PATH=/opt/sql-sync-dashboard/venv/bin"
ExecStart=/opt/sql-sync-dashboard/venv/bin/gunicorn -w 4 -b 0.0.0.0:5001 app:app
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**Start service:**
```bash
sudo systemctl start sql-sync
sudo systemctl enable sql-sync
```

#### 4. Configure Firewall

```powershell
# Windows Firewall
New-NetFirewallRule -DisplayName "SQL Sync Dashboard" -Direction Inbound -LocalPort 5001 -Protocol TCP -Action Allow

# Linux (ufw)
sudo ufw allow 5001/tcp
```

#### 5. Set Up Reverse Proxy (Optional)

**Nginx Configuration:**

```nginx
server {
    listen 80;
    server_name sync-dashboard.company.com;

    location / {
        proxy_pass http://127.0.0.1:5001;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
}
```

#### 6. Regular Maintenance

**Daily:**
- Monitor sync logs
- Check email notifications
- Verify scheduled jobs running

**Weekly:**
```sql
-- PostgreSQL maintenance
VACUUM ANALYZE metrics_sync_tables.sync_history;
```

**Monthly:**
```sql
-- Archive old sync history (older than 90 days)
DELETE FROM metrics_sync_tables.sync_history 
WHERE sync_time < NOW() - INTERVAL '90 days';
```

---

## 📊 Key Features Overview

### 1. Dashboard Features
- ✅ Real-time sync status for all servers
- ✅ One-click manual sync
- ✅ Connection status indicators
- ✅ Recent sync history

### 2. Advanced Analytics
- ✅ Total syncs today across all servers
- ✅ Success rate percentage
- ✅ Top 50 servers by sync count
- ✅ Performance charts (hourly trends)
- ✅ Server status table with 7-day success rates
- ✅ Recent 100 sync operations feed

### 3. Sync Summary
- ✅ Server-by-server comparison
- ✅ SQL Server vs PostgreSQL row counts
- ✅ Table-level detailed comparison
- ✅ Sync status indicators
- ✅ 60-second cache for fast loading

### 4. Scheduling
- ✅ Interval-based schedules (every X minutes)
- ✅ Daily schedules (specific time)
- ✅ Schedule persistence across restarts
- ✅ Edit/delete schedules
- ✅ View all active schedules

### 5. User Management
- ✅ Role-Based Access Control
  - **Admin:** Full access
  - **Operator:** Can sync, view, schedule
  - **Viewer:** Read-only access
- ✅ User creation with email notification
- ✅ Secure password hashing (bcrypt)

### 6. Email Notifications
- ✅ New user creation
- ✅ Scheduled sync success
- ✅ Scheduled sync failure
- ✅ Server down alerts
- ✅ Server restart notifications

### 7. Monitoring
- ✅ Watchdog for stuck syncs (marks as failed after 60 min)
- ✅ Health checks every 15 minutes
- ✅ Detailed logging with timestamps
- ✅ Sync duration tracking

---

## 🎯 Quick Start Checklist

Use this checklist when deploying to a new server:

- [ ] **Install Prerequisites:**
  - [ ] Python 3.8+
  - [ ] PostgreSQL 12+
  - [ ] ODBC Driver 17 for SQL Server

- [ ] **Extract Project Files:**
  - [ ] Unzip to desired location
  - [ ] Navigate to project directory

- [ ] **Setup Python Environment:**
  - [ ] Create virtual environment: `python -m venv venv`
  - [ ] Activate: `.\venv\Scripts\Activate.ps1`
  - [ ] Install dependencies: `pip install -r requirements.txt`

- [ ] **Configure Application:**
  - [ ] Create `.env` file with PostgreSQL credentials
  - [ ] Add email SMTP settings to `.env`
  - [ ] Edit `config/db_connections.yaml` with SQL Server connections

- [ ] **Setup Database:**
  - [ ] Create PostgreSQL user: `migration_user`
  - [ ] Create database: `test1`
  - [ ] Grant privileges

- [ ] **Start Application:**
  - [ ] Run: `python app.py`
  - [ ] Open: http://127.0.0.1:5001

- [ ] **Initial Configuration:**
  - [ ] Create first admin user
  - [ ] Login to dashboard
  - [ ] Test SQL Server connections
  - [ ] Run first manual sync
  - [ ] Verify data in PostgreSQL

- [ ] **Optional Setup:**
  - [ ] Create scheduled syncs
  - [ ] Run database optimization: `python optimize_production_db.py`
  - [ ] Test email notifications: `python send_test_email.py`

---

## 📞 Support & Documentation

### Configuration Files Reference
- `.env` - Environment variables (sensitive data)
- `config/db_connections.yaml` - SQL Server connections
- `requirements.txt` - Python dependencies

### Important Scripts
- `app.py` - Main application (start here)
- `optimize_production_db.py` - Database indexes for production
- `send_test_email.py` - Test email configuration
- `create_database.py` - Auto-create PostgreSQL database

### Documentation Files
- `README.md` - Project overview
- `PRODUCTION_READY.md` - 100+ servers optimization details
- `ANALYTICS_COMPLETE_FIX.md` - Analytics dashboard fixes
- `SCHEDULE_PERSISTENCE_GUIDE.md` - Schedule recovery guide

### Logs Location
- Console output shows real-time sync progress
- Check terminal for detailed logs
- Database `sync_history` table stores all sync records

---

## 🔒 Security Best Practices

1. **Strong Passwords:**
   - PostgreSQL: Use complex passwords
   - Admin user: Minimum 12 characters
   - SQL Server: Follow company security policy

2. **Environment Variables:**
   - Never commit `.env` to version control
   - Keep `.env` file secure (chmod 600 on Linux)

3. **Email Security:**
   - Use Gmail App Passwords (not regular password)
   - Enable 2FA on email account

4. **Network Security:**
   - Use firewall to restrict access to port 5001
   - Consider VPN for remote access
   - Use HTTPS in production (with reverse proxy)

5. **Database Security:**
   - Limit PostgreSQL user privileges
   - Use separate user for syncing (not postgres superuser)
   - Regular backups of PostgreSQL

---

## ✅ Deployment Complete!

Your SQL Sync Dashboard is now ready to sync data from multiple SQL Servers to PostgreSQL with monitoring, scheduling, and analytics!

**Access your dashboard at:** http://127.0.0.1:5001

**Default first user:** Create via web interface (auto-redirects on first access)

**Need help?** Check the troubleshooting section above.

---

**Last Updated:** October 27, 2025  
**Version:** 1.0 - Production Ready for 100+ Servers
