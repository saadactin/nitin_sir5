# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Common Development Tasks

### Running the Application
```bash
python app.py
```
The Flask application will start in debug mode and be accessible at http://localhost:5000.

### Installing Dependencies
```bash
# Install ODBC driver for SQL Server (Windows)
winget install Microsoft.DataAccess.OdbcDriverForSqlServer

# Install Python dependencies
pip install -r requirements.txt
```

### Managing SQL Server Connections
```bash
# List configured servers
python manage_server.py --list

# Add a new SQL server
python manage_server.py --add SERVER_NAME HOST USERNAME PASSWORD

# Delete a server
python manage_server.py --delete SERVER_NAME
```

### Database Schema Management
- PostgreSQL schema is automatically initialized on first run via `db_utils.init_pg_schema()`
- Sync tracking tables are created automatically via `hybrid_sync.create_sync_tracking_table()`

## Architecture Overview

### Core Components

**Flask Web Application (`app.py`)**
- Main web interface with role-based authentication (admin, operator, viewer)
- Routes for server management, sync operations, scheduling, analytics, and monitoring
- Session-based authentication with bcrypt password hashing

**Hybrid Sync Engine (`hybrid_sync.py`)**
- Core synchronization logic between SQL Server and PostgreSQL
- Supports both full and incremental sync modes
- Handles schema discovery, data type mapping, and batch processing
- Tracks sync status at database and table levels

**Authentication System (`auth.py`)**
- Role-based access control with three roles: admin, operator, viewer
- Uses bcrypt for password hashing
- Creates default admin user (admin/admin123) on first run

**Scheduler System (`scheduler_utils.py`)**
- Background job scheduling using the `schedule` library
- Supports interval-based and daily scheduled syncs
- Persistent job storage in PostgreSQL
- Thread-based scheduler execution

**Analytics & Monitoring**
- `analytics.py` - Basic comparison and delta tracking
- `analytics_advanced.py` - Advanced features like sync history, alerts, schema change detection
- `metrics.py` - Comprehensive metrics collection for servers, databases, and tables
- `dashboard.py` - Sync history and status tracking

### Data Flow Architecture

1. **Source Discovery**: SQL Server databases and tables are discovered via pyodbc
2. **Schema Mapping**: PostgreSQL schemas are created following the pattern `{server}_{database}`
3. **Sync Execution**: Data is read via SQLAlchemy/pandas and written to PostgreSQL
4. **Progress Tracking**: Sync status is maintained in `sync_database_status` and `sync_table_status` tables
5. **Scheduling**: Background jobs trigger sync operations based on configured schedules

### Configuration Structure

**Database Connections (`config/db_connections.yaml`)**
- PostgreSQL connection parameters
- SQL Server configurations with sync modes and skip lists
- Supports multiple SQL Server instances

**Key Configuration Fields**
- `sync_mode`: "hybrid" (default)
- `check_new_databases`: Auto-discover new databases
- `skip_databases`: Databases to exclude from sync

### Key Modules and Their Purpose

**`load_postgres.py`**: PostgreSQL table creation and schema management
**`db_utils.py`**: PostgreSQL connection utilities and schema initialization
**`manage_server.py`**: Command-line server management utilities
**`seeschedule.py`**: Schedule viewing and management
**`monitoring.py`**: System monitoring capabilities

## Development Guidelines

### Working with Sync Operations
- Full sync is triggered for databases without sync history
- Incremental sync uses primary key tracking via `sync_table_status`
- System tables are automatically cleaned up during sync operations
- Batch size is configurable via `HYBRID_SYNC_BATCH_SIZE` environment variable (default: 10000)

### Authentication Context
- All routes require authentication except `/login`
- Role hierarchy: admin > operator > viewer
- Admins can create users and manage all operations
- Operators can perform syncs and scheduling
- Viewers have read-only access

### Database Schema Patterns
- PostgreSQL schemas follow: `{server_clean}_{db_name}`
- PostgreSQL tables follow: `{schema}_{table}`
- Sync tracking uses server name from configuration, not display name

### Testing Sync Operations
- Use the web interface at `/server/{server_name}` to select specific databases
- Monitor sync progress via `/dashboard` and `/metrics/{server_name}`
- Check sync history at `/sync-history/{server}/{db}`
- Use `/compare/{server}/{db}/{table}` for data validation

### Troubleshooting
- Sync logs are written to `hybrid_sync.log`
- PostgreSQL connection logs in `load_postgres.log`
- Failed syncs are tracked in `sync_database_status` with non-COMPLETED status
- Use `/alerts` route to view system-wide issues

### Environment Variables
- `HYBRID_SYNC_BATCH_SIZE`: Controls batch size for data processing (default: 10000)
- SMTP settings for email notifications: `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`, `SMTP_FROM`
- `SLACK_WEBHOOK_URL`: For Slack notifications

### Template Structure
The web interface uses Flask templates in the `templates/` directory with comprehensive views for:
- Server and database management
- Sync scheduling and monitoring  
- Analytics and reporting
- User authentication and role management