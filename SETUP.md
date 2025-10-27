# SQL Server to PostgreSQL Sync Application - Setup Guide

## Prerequisites
- Python 3.8+ installed
- PostgreSQL server running and accessible
- SQL Server instance (if syncing from SQL Server)

## Quick Setup for New Users

### 1. Clone/Extract the Project
```bash
# Extract or clone the project to your desired location
cd path/to/project/nitin_sir5
```

### 2. Create Virtual Environment
```bash
python -m venv myenv
myenv\Scripts\activate
```

### 3. Install Dependencies
```bash
python -m pip install -r requirements.txt
```

### 4. Configure Environment Variables
Copy `.env.example` to `.env` and edit the values:

```bash
# Copy the example file
copy .env .env.local  # or create your own .env file
```

**Required .env Configuration:**
```properties
# Application secret - MUST be set for production
SECRET_KEY=your-secret-key-here

# PostgreSQL connection (PRIMARY - app prefers these over YAML)
PG_HOST=localhost                    # Your PostgreSQL server
PG_PORT=5432                        # PostgreSQL port
PG_DATABASE=your_database_name      # Target database name
PG_USERNAME=your_pg_username        # PostgreSQL username
PG_PASSWORD=your_pg_password        # PostgreSQL password

# Flask settings
FLASK_DEBUG=0                       # Set to 1 for development
APP_HOST=127.0.0.1                 # Server bind address
APP_PORT=5000                       # Server port

# Admin user creation (optional)
CREATE_DEFAULT_ADMIN=1              # Set to 1 to create default admin
DEFAULT_ADMIN_PASSWORD=admin123     # Change this password!
```

### 5. Configure Database Connections (if needed)
The app uses **environment variables first**, then falls back to `config/db_connections.yaml`.

**Option A: Use Environment Variables Only (Recommended)**
- Just configure the `.env` file above
- No need to modify YAML files

**Option B: Use YAML Configuration**
Edit `config/db_connections.yaml`:
```yaml
postgresql:
  database: your_database_name
  host: localhost
  password: your_password
  port: 5432
  schema: CompanyDB
  username: your_username

sqlservers:
  your_server_name:
    server: your_sql_server_host
    username: your_sql_username
    password: your_sql_password
    target_postgres_db: your_target_db
    sync_mode: hybrid
```

### 6. Initialize PostgreSQL Database
Make sure your PostgreSQL database exists and is accessible:
```sql
-- Connect to PostgreSQL and create database if needed
CREATE DATABASE your_database_name;
```

### 7. Run the Application
```bash
python app.py
```

The application will:
- Start on http://127.0.0.1:5000 (or your configured HOST:PORT)
- Automatically create required database tables
- Create default admin user if configured

## Configuration Priority

The application follows this configuration priority:

1. **Environment Variables** (`.env` file) - **HIGHEST PRIORITY**
   - `PG_HOST`, `PG_PORT`, `PG_DATABASE`, `PG_USERNAME`, `PG_PASSWORD`
   
2. **YAML Configuration** (`config/db_connections.yaml`) - **FALLBACK**
   - Used only if environment variables are not set

## Troubleshooting

### Common Issues:

1. **"No PostgreSQL databases found"**
   - Check your `.env` file has correct PostgreSQL credentials
   - Verify PostgreSQL server is running and accessible
   - Test connection: `python test_postgres_connectivity.py`

2. **Import errors or missing modules**
   - Ensure virtual environment is activated
   - Run: `python -m pip install -r requirements.txt`

3. **SQL Server connection issues**
   - Verify ODBC drivers are installed
   - Check SQL Server is running and accessible
   - Update connection strings in YAML or environment variables

4. **Permission errors**
   - Ensure PostgreSQL user has CREATE/INSERT/UPDATE permissions
   - Verify SQL Server user has READ permissions on target databases

### Testing Connectivity:
```bash
# Test PostgreSQL connection
python test_postgres_connectivity.py

# Test overall connectivity
python test_pg_databases_fix.py
```

## Security Notes

- **Never commit `.env` files with real credentials**
- Change default passwords in production
- Use strong SECRET_KEY in production
- Restrict database user permissions to minimum required

## Development vs Production

**Development:**
```properties
FLASK_DEBUG=1
CREATE_DEFAULT_ADMIN=1
```

**Production:**
```properties
FLASK_DEBUG=0
CREATE_DEFAULT_ADMIN=0
SECRET_KEY=strong-random-secret-key
```

## 🚀 Setup Instructions
Step 1: Install dependencies
On Windows, follow these steps:
Install the SQL Server ODBC driver:
Open PowerShell and run:
```bash
winget install Microsoft.DataAccess.OdbcDriverForSqlServer
```
Install Python dependencies:
Run the following command in your project directory:
```bash
pip install -r requirements.txt
```
Step 2: Run the application
Start the Flask app by running:
```bash
python app.py
```
Your app should now be up and running!