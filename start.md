# 🚀 Project Setup Guide

This guide will help you set up and run the Data Migration Project from scratch on a new machine.

---

## 📋 Prerequisites & Downloads Required

### 1. **Python 3.8 or Higher**
   - Download from: https://www.python.org/downloads/
   - **Important**: During installation, check "Add Python to PATH"
   - Verify installation: Open terminal/command prompt and run:
     ```bash
     python --version
     ```

### 2. **PostgreSQL Database** (Required)
   - Download from: https://www.postgresql.org/download/
   - Install PostgreSQL and note down:
     - Database name
     - Username
     - Password
     - Port (default: 5432)
   - **Alternative**: Use Docker to run PostgreSQL:
     ```bash
     docker run --name postgres -e POSTGRES_PASSWORD=yourpassword -e POSTGRES_DB=test1 -p 5432:5432 -d postgres
     ```

### 3. **ClickHouse Database** (Required for Data Storage)
   - Download from: https://clickhouse.com/docs/en/install
   - **Windows**: Use Docker (recommended):
     ```bash
     docker run -d --name clickhouse -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server
     ```
   - **Linux/Mac**: Follow official installation guide
   - Default credentials:
     - User: `default`
     - Password: (empty by default)
     - Port: `9000` (native), `8123` (HTTP)

### 4. **Optional Dependencies** (Only if needed)
   - **SQL Server Support**: Requires `pyodbc` and ODBC Driver for SQL Server
     - ODBC Driver: https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
   - **SAP HANA Support**: Requires `hdbcli` package
     - Will be installed via pip if needed

---

## 📦 Installation Steps

### Step 1: Extract the Project
1. Extract the ZIP file to your desired location (e.g., `C:\Projects\data-migration` or `/home/user/data-migration`)
2. Open terminal/command prompt in the project directory

### Step 2: Create Virtual Environment
```bash
# Windows
python -m venv myenv
myenv\Scripts\activate

# Linux/Mac
python3 -m venv myenv
source myenv/bin/activate
```

### Step 3: Install Python Dependencies
```bash
pip install -r requirements.txt
```

**Note**: If you only need API sync functionality (no SQL Server/HANA), you can skip installing `pyodbc` and `hdbcli`:
```bash
# Install only essential packages for API sync
pip install Flask==3.1.0 pandas==2.2.2 psycopg2-binary==2.9.10 bcrypt==4.2.0 PyYAML==6.0.2 schedule==1.2.2 SQLAlchemy==2.0.30 cryptography==42.0.2 python-dotenv==1.0.1 openpyxl==3.1.2 clickhouse-driver==0.2.3 clickhouse-connect==0.8.0 requests==2.31.0 Flask-SocketIO python-engineio python-socketio
```

---

## ⚙️ Configuration Files Setup

### 1. Create `.env` File

Create a `.env` file in the project root directory with the following variables:

```env
# ============================================
# PostgreSQL Configuration (REQUIRED)
# ============================================
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=test1
PG_USERNAME=migration_user
PG_PASSWORD=StrongPassword123

# Alternative PostgreSQL environment variables (if not using PG_* prefix)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=test1
POSTGRES_USER=migration_user
POSTGRES_PASSWORD=StrongPassword123

# ============================================
# ClickHouse Configuration (REQUIRED)
# ============================================
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DATABASE=default

# ============================================
# Flask Application Configuration
# ============================================
SECRET_KEY=your-secret-key-change-this-in-production-use-random-string
FLASK_ENV=development
FLASK_DEBUG=1
FLASK_PORT=5002

# ============================================
# Email Configuration (Optional - for notifications)
# ============================================
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your-email@gmail.com
SMTP_PASSWORD=your-app-password
ADMIN_EMAILS=admin1@example.com,admin2@example.com

# Alternative email environment variables
EMAIL_HOST=smtp.gmail.com
EMAIL_PORT=587
EMAIL_HOST_USER=your-email@gmail.com
EMAIL_HOST_PASSWORD=your-app-password

# ============================================
# SAP HANA Configuration (Optional - only if using HANA)
# ============================================
HANA_HOST=192.168.16.62
HANA_PORT=30015
HANA_USERNAME=your_hana_username
HANA_PASSWORD=your_hana_password
HANA_DATABASE=SYSTEMDB

# ============================================
# Admin User Configuration (Optional)
# ============================================
DEFAULT_ADMIN_PASSWORD=admin123
CREATE_DEFAULT_ADMIN=1

# ============================================
# Session Security (Optional)
# ============================================
SESSION_COOKIE_SECURE=0
SESSION_COOKIE_SAMESITE=Lax
PERMANENT_SESSION_LIFETIME=3600

# ============================================
# Connection Pool Settings (Optional)
# ============================================
PG_POOL_MIN_CONN=5
PG_POOL_MAX_CONN=20
SQL_POOL_SIZE=5
HANA_POOL_SIZE=3
CLICKHOUSE_POOL_SIZE=5
```

**Important Notes:**
- Replace all placeholder values with your actual credentials
- **SECRET_KEY**: Generate a strong random string (e.g., use `python -c "import secrets; print(secrets.token_hex(32))"`)
- **SMTP_PASSWORD**: For Gmail, use an "App Password" (not your regular password)
- Never commit `.env` file to version control

### 2. Configure `config/db_connections.yaml`

Edit the file `config/db_connections.yaml`:

```yaml
postgresql:
  database: test1                    # Your PostgreSQL database name
  host: localhost                    # PostgreSQL host
  password: StrongPassword123         # Your PostgreSQL password
  port: 5432                         # PostgreSQL port
  schema: CompanyDB                   # Schema name (optional)
  username: migration_user            # Your PostgreSQL username

sqlservers: {}                       # Leave empty if not using SQL Server
```

**Important Notes:**
- Update `database`, `host`, `password`, `port`, and `username` with your PostgreSQL credentials
- The `schema` field is optional
- If you're not using SQL Server, leave `sqlservers: {}` as empty

---

## 🏃 Running the Project

### Method 1: Using Python Directly
```bash
# Make sure virtual environment is activated
python app.py
```

The application will start on `http://localhost:5002` (or the port specified in `FLASK_PORT`)

### Method 2: Using Batch Script (Windows)
```bash
# Double-click or run:
start_project.bat
```

### Method 3: Using PowerShell Script (Windows)
```powershell
.\run-dev.ps1
```

### Method 4: Using Production Script
```bash
python run_production.py
```

---

## 🌐 Accessing the Application

1. Open your web browser
2. Navigate to: `http://localhost:5002`
3. You will be redirected to the login page
4. **Default Credentials:**
   - Username: `admin`
   - Password: `admin123` (or the value set in `DEFAULT_ADMIN_PASSWORD`)

---

## ✅ Verification Steps

### 1. Check Database Connections
After starting the application, verify:
- PostgreSQL connection is working
- ClickHouse connection is working (if configured)

### 2. Test API Sync
1. Login to the application
2. Go to "Add Source" → "Add API Source"
3. Enter an API URL (e.g., `https://jsonplaceholder.typicode.com/posts`)
4. Click "Test Connection"
5. If successful, click "Add & Start Sync"

### 3. Check Logs
- Application logs: Check terminal/console output
- Error logs: Check `app.log` file in project directory

---

## 🔧 Troubleshooting

### Issue: "ModuleNotFoundError: No module named 'pyodbc'"
**Solution**: This is only needed for SQL Server sync. If you're only using API sync, you can ignore this error or install it:
```bash
pip install pyodbc
```

### Issue: "ModuleNotFoundError: No module named 'hdbcli'"
**Solution**: This is only needed for SAP HANA sync. If you're only using API sync, you can ignore this error or install it:
```bash
pip install hdbcli
```

### Issue: "Cannot connect to PostgreSQL"
**Solution**:
1. Verify PostgreSQL is running: `pg_isready` (Linux/Mac) or check Services (Windows)
2. Verify credentials in `.env` and `config/db_connections.yaml`
3. Check firewall settings
4. Verify PostgreSQL is listening on the correct port

### Issue: "Cannot connect to ClickHouse"
**Solution**:
1. Verify ClickHouse is running
2. Check if port 9000 (or 8123) is accessible
3. Verify credentials in `.env`
4. Test connection: `clickhouse-client --host localhost --port 9000`

### Issue: "SECRET_KEY not set"
**Solution**: Add `SECRET_KEY` to your `.env` file:
```env
SECRET_KEY=your-random-secret-key-here
```

### Issue: "Email sending failed"
**Solution**: 
- Email configuration is optional
- If you don't need email notifications, you can skip SMTP configuration
- For Gmail, make sure to use an "App Password" instead of your regular password

---

## 📝 Quick Start Checklist

- [ ] Python 3.8+ installed
- [ ] PostgreSQL installed and running
- [ ] ClickHouse installed and running
- [ ] Virtual environment created and activated
- [ ] Dependencies installed (`pip install -r requirements.txt`)
- [ ] `.env` file created with correct values
- [ ] `config/db_connections.yaml` configured
- [ ] Application started (`python app.py`)
- [ ] Can access `http://localhost:5002`
- [ ] Can login with admin credentials
- [ ] Can add and test API source

---

## 🎯 Next Steps

1. **Add Your First API Source:**
   - Login to the application
   - Navigate to "Add Source" → "Add API Source"
   - Enter your API URL and configure authentication if needed
   - Test connection and start sync

2. **Configure Scheduling:**
   - Go to "Create Schedule" to set up automatic syncs
   - Choose interval (every X minutes) or daily schedule

3. **Monitor Sync Status:**
   - Check "Sync Dashboard" for real-time sync status
   - View "Sync History" for past syncs
   - Check "Log Analysis" for detailed logs

---

## 📞 Support

If you encounter any issues:
1. Check the troubleshooting section above
2. Review application logs in the terminal
3. Check `app.log` file for detailed error messages
4. Verify all configuration files are correctly set up

---

## 🔒 Security Notes

- **Never commit `.env` file** to version control
- Use strong passwords for all databases
- Change default admin password after first login
- Use `SECRET_KEY` with a strong random value in production
- Enable `SESSION_COOKIE_SECURE=1` and use HTTPS in production
- Set `FLASK_DEBUG=0` in production environment

---

**Last Updated**: 2025-11-25
**Project Version**: 1.0

