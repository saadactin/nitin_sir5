# Project Overview

## Enterprise Data Migration and Synchronization Platform

A comprehensive Flask-based web application designed for enterprise data migration, synchronization, and management across multiple database systems and data sources. This platform provides a unified interface for managing data flows between SQL Server, PostgreSQL, SAP HANA, ClickHouse, and REST APIs.

---

## Table of Contents

1. [Overview](#overview)
2. [Core Features](#core-features)
3. [Supported Data Sources](#supported-data-sources)
4. [Architecture](#architecture)
5. [Key Components](#key-components)
6. [Setup & Installation](#setup--installation)
7. [Configuration](#configuration)
8. [Usage Guide](#usage-guide)
9. [Security & Authentication](#security--authentication)
10. [Analytics & Monitoring](#analytics--monitoring)
11. [API Integration](#api-integration)
12. [Scheduling & Automation](#scheduling--automation)
13. [Testing Tools](#testing-tools)
14. [Deployment](#deployment)

---

## Overview

This platform is designed to solve enterprise data migration and synchronization challenges by providing:

- **Multi-Database Support**: Connect and sync data across SQL Server, PostgreSQL, SAP HANA, and ClickHouse
- **REST API Integration**: Import data from REST APIs with OAuth 2.0 support
- **Automated Scheduling**: Set up recurring sync operations with flexible scheduling
- **Real-Time Monitoring**: Track sync progress, errors, and system health
- **Role-Based Access Control**: Secure access with admin, operator, and viewer roles
- **Advanced Analytics**: Comprehensive reporting and data comparison tools
- **Web-Based Interface**: User-friendly dashboard for managing all operations

---

## Core Features

### 1. Database Synchronization

- **Full Sync**: Complete data migration from source to target databases
- **Incremental Sync**: Smart delta sync using primary keys and timestamps
- **Hybrid Mode**: Automatic switching between full and incremental syncs
- **Batch Processing**: Configurable batch sizes for optimal performance
- **Schema Mapping**: Automatic schema discovery and data type conversion
- **Progress Tracking**: Real-time sync status and progress monitoring

### 2. Multi-Source Data Management

- **SQL Server**: Full support for SQL Server databases with ODBC connectivity
- **PostgreSQL**: Native PostgreSQL support as both source and target
- **SAP HANA**: Complete HANA to ClickHouse migration pipeline
- **ClickHouse**: High-performance analytical database support
- **REST APIs**: RESTful API data ingestion with authentication support

### 3. Web Interface

- **Dashboard**: Centralized overview of sync operations and system status
- **Server Management**: Add, edit, and manage database connections
- **Source Management**: Unified interface for all data source types
- **Sync Control**: Start, stop, and monitor sync operations
- **Analytics Dashboard**: Visual representation of sync history and metrics
- **Schedule Management**: Create and manage automated sync schedules

### 4. Security & Access Control

- **Role-Based Access Control (RBAC)**: Three-tier permission system
  - **Admin**: Full system access and user management
  - **Operator**: Sync operations and scheduling
  - **Viewer**: Read-only access to reports and dashboards
- **Session Management**: Secure session handling with logout functionality
- **Password Encryption**: Bcrypt hashing for user passwords
- **Connection Security**: Encrypted database connections

### 5. Automation & Scheduling

- **Interval-Based Syncs**: Schedule syncs at regular intervals (minutes, hours)
- **Daily Syncs**: Set up daily sync operations at specific times
- **Persistent Schedules**: Schedules saved in database survive restarts
- **Background Processing**: Non-blocking sync operations
- **Multi-Threaded Execution**: Concurrent sync support

### 6. Monitoring & Alerts

- **Email Notifications**: Automated email alerts for:
  - Sync completion/failure
  - Server downtime
  - System errors
  - Schedule execution status
- **Sync History**: Complete audit trail of all sync operations
- **Metrics Collection**: Server, database, and table-level metrics
- **Log Analysis**: Comprehensive logging with log file management

### 7. Data Upload & Import

- **File Upload Support**: CSV, Excel, and text file import
- **Multi-Format Parsing**: Automatic format detection and parsing
- **Target Selection**: Choose PostgreSQL or ClickHouse as target
- **Progress Tracking**: Real-time upload progress indicators
- **Data Validation**: Input validation and error handling

---

## Supported Data Sources

### SQL Server
- Multiple SQL Server instances
- Database and schema discovery
- Full and incremental sync modes
- ODBC driver support
- Named instance support

### PostgreSQL
- Primary target database
- Schema creation and management
- Full CRUD operations
- Connection pooling
- Multiple database support

### SAP HANA
- HANA database connection via hdbcli
- Schema and table discovery
- Data type mapping to ClickHouse
- Batch data migration
- Incremental sync support

### ClickHouse
- Native ClickHouse driver support
- HTTP and native protocol support
- Database and table management
- High-performance analytical queries
- Bulk data insertion

### REST APIs
- RESTful API integration
- OAuth 2.0 authentication
- Bearer token support
- API endpoint configuration
- Automatic polling support
- Data transformation and mapping

---

## Architecture

### Technology Stack

**Backend:**
- Flask (Python web framework)
- SQLAlchemy (ORM)
- Pandas (Data processing)
- PyODBC (SQL Server connectivity)
- psycopg2 (PostgreSQL driver)
- clickhouse-driver (ClickHouse client)
- hdbcli (SAP HANA client)
- Requests (HTTP client)

**Frontend:**
- HTML5/CSS3/JavaScript
- Bootstrap (Responsive UI)
- jQuery (DOM manipulation)
- Server-Sent Events (Real-time updates)

**Database:**
- PostgreSQL (Primary metadata storage)
- ClickHouse (Analytical data storage)

### System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Flask Web Application                     │
│                      (app.py)                                │
├─────────────────────────────────────────────────────────────┤
│  Authentication  │  Sync Manager  │  Analytics  │  Scheduler │
│     (auth.py)    │ (sync_manager) │(analytics_*)│(scheduler) │
├─────────────────────────────────────────────────────────────┤
│           Database Connection Layer                          │
│  hybrid_sync.py  │  hana_sync.py  │  api_sync.py           │
├─────────────────────────────────────────────────────────────┤
│              Data Source Connectors                          │
│  SQL Server  │  PostgreSQL  │  HANA  │  ClickHouse  │  APIs │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **Connection**: Connect to source database/API
2. **Discovery**: Discover schemas, tables, or endpoints
3. **Schema Mapping**: Map source schema to target format
4. **Data Extraction**: Read data in batches from source
5. **Transformation**: Apply data type conversions
6. **Loading**: Write data to target database
7. **Tracking**: Update sync status and history
8. **Notification**: Send completion alerts

---

## Key Components

### Core Modules

#### `app.py` - Main Application
- Flask application entry point
- Route definitions and request handling
- Session management
- Error handling and logging
- Background task coordination

#### `hybrid_sync.py` - SQL Server Sync Engine
- SQL Server to PostgreSQL synchronization
- Full and incremental sync logic
- Schema discovery and mapping
- Batch processing and optimization
- Sync status tracking

#### `hana_sync.py` - HANA Migration Module
- SAP HANA to ClickHouse migration
- HANA schema discovery
- Data type mapping
- Batch data migration
- Incremental sync setup

#### `api_sync.py` - REST API Integration
- REST API data fetching
- OAuth 2.0 authentication
- Data transformation
- ClickHouse insertion
- Polling support

#### `sync_manager.py` - Sync Orchestration
- Background sync management
- Concurrent sync coordination
- Status tracking
- Error recovery
- Resource management

#### `auth.py` - Authentication System
- User authentication
- Role-based access control
- Password hashing (bcrypt)
- Session management
- User creation and management

#### `scheduler_utils.py` - Job Scheduling
- Interval-based scheduling
- Daily schedule management
- Persistent job storage
- Background thread execution
- Schedule CRUD operations

#### `analytics.py` - Basic Analytics
- Table row comparison
- Delta tracking
- Top changed tables
- Sync statistics

#### `analytics_advanced.py` - Advanced Analytics
- Database history tracking
- Table history analysis
- Failed sync detection
- Sync report generation
- Schema change parsing
- Alert collection

#### `dashboard.py` - Dashboard Data
- Sync history retrieval
- Last sync details
- Sync logging
- Server-specific sync data

#### `metrics.py` - Metrics Collection
- Server metrics
- Database metrics
- Table statistics
- Performance indicators

#### `db_utils.py` - Database Utilities
- PostgreSQL connection management
- Schema initialization
- Connection configuration
- Database operations

#### `manage_server.py` - Server Management
- Server configuration loading
- Server configuration saving
- Connection management

#### `connection_sync.py` - Config Synchronization
- YAML to database sync
- Database to YAML sync
- Connection updates
- Connection removal

### Utility Modules

- `alerts.py`: Log analysis and alerting
- `monitoring.py`: System monitoring
- `seeschedule.py`: Schedule viewing
- `sync_summary.py`: Sync comparison and summaries
- `utils/email_service.py`: Email notification service
- `security.py`: Security enhancements

---

## Setup & Installation

### Prerequisites

- Python 3.8 or higher
- PostgreSQL server (target database)
- ODBC Driver for SQL Server (if syncing from SQL Server)
- Network access to source databases/APIs
- Virtual environment (recommended)

### Installation Steps

1. **Clone/Extract Project**
   ```bash
   cd path/to/project/nitin_sir5
   ```

2. **Create Virtual Environment**
   ```bash
   python -m venv myenv
   myenv\Scripts\activate  # Windows
   # or
   source myenv/bin/activate  # Linux/Mac
   ```

3. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Install ODBC Driver (Windows)**
   ```bash
   winget install Microsoft.DataAccess.OdbcDriverForSqlServer
   ```

5. **Configure Environment Variables**
   Create a `.env` file in the project root:
   ```env
   # Application Configuration
   SECRET_KEY=your-secret-key-here
   FLASK_DEBUG=0
   APP_HOST=127.0.0.1
   APP_PORT=5000
   
   # PostgreSQL Configuration
   PG_HOST=localhost
   PG_PORT=5432
   PG_DATABASE=your_database_name
   PG_USERNAME=your_username
   PG_PASSWORD=your_password
   
   # ClickHouse Configuration (if using)
   CLICKHOUSE_HOST=localhost
   CLICKHOUSE_PORT=9000
   CLICKHOUSE_USER=default
   CLICKHOUSE_PASSWORD=
   
   # HANA Configuration (if using)
   HANA_HOST=192.168.16.62
   HANA_PORT=30015
   HANA_USERNAME=your_username
   HANA_PASSWORD=your_password
   
   # Email Configuration (for notifications)
   SMTP_HOST=smtp.gmail.com
   SMTP_PORT=587
   SMTP_USER=your_email@gmail.com
   SMTP_PASSWORD=your_app_password
   EMAIL_FROM=your_email@gmail.com
   EMAIL_TO=admin@company.com
   ```

6. **Initialize PostgreSQL Database**
   ```sql
   CREATE DATABASE your_database_name;
   GRANT ALL PRIVILEGES ON DATABASE your_database_name TO your_username;
   ```

7. **Run Application**
   ```bash
   python app.py
   ```

8. **Access Web Interface**
   - Open browser: `http://127.0.0.1:5000`
   - Default login: `admin` / `admin123` (change immediately)

---

## Configuration

### Configuration Priority

1. **Environment Variables** (`.env` file) - **Highest Priority**
2. **YAML Configuration** (`config/db_connections.yaml`) - **Fallback**
3. **Database Storage** (PostgreSQL) - **Runtime configuration**

### Environment Variables

**Application Settings:**
- `SECRET_KEY`: Flask secret key for sessions
- `FLASK_DEBUG`: Debug mode (0/1)
- `APP_HOST`: Server bind address
- `APP_PORT`: Server port

**PostgreSQL:**
- `PG_HOST`: PostgreSQL server address
- `PG_PORT`: PostgreSQL port (default: 5432)
- `PG_DATABASE`: Target database name
- `PG_USERNAME`: PostgreSQL username
- `PG_PASSWORD`: PostgreSQL password

**ClickHouse:**
- `CLICKHOUSE_HOST`: ClickHouse server address
- `CLICKHOUSE_PORT`: ClickHouse port (default: 9000)
- `CLICKHOUSE_USER`: ClickHouse username
- `CLICKHOUSE_PASSWORD`: ClickHouse password

**HANA:**
- `HANA_HOST`: HANA server IP address
- `HANA_PORT`: HANA port (default: 30015)
- `HANA_USERNAME`: HANA username
- `HANA_PASSWORD`: HANA password

**Email:**
- `SMTP_HOST`: SMTP server address
- `SMTP_PORT`: SMTP port
- `SMTP_USER`: SMTP username
- `SMTP_PASSWORD`: SMTP password/app password
- `EMAIL_FROM`: Sender email address
- `EMAIL_TO`: Recipient email address(es)

### YAML Configuration

`config/db_connections.yaml`:
```yaml
postgresql:
  database: your_database
  host: localhost
  port: 5432
  username: your_username
  password: your_password
  schema: CompanyDB

sqlservers: {}
```

### Sync Settings

**Batch Size:**
- Environment variable: `HYBRID_SYNC_BATCH_SIZE` (default: 5000-10000)
- Controls rows processed per batch

**Sync Mode:**
- `hybrid`: Automatic full/incremental switching
- `full`: Always perform full sync
- `incremental`: Only sync new/changed records

---

## Usage Guide

### Getting Started

1. **Login**
   - Access the web interface
   - Use default credentials: `admin` / `admin123`
   - Change password immediately after first login

2. **Add Data Source**
   - Navigate to "Add Source" menu
   - Choose source type:
     - SQL Server
     - SAP HANA
     - REST API
   - Enter connection details
   - Test connection
   - Save configuration

3. **Configure Target Database**
   - Ensure PostgreSQL or ClickHouse is configured in `.env`
   - Target is automatically selected based on source type

4. **Perform Sync**
   - Go to "Sync Servers" or "Sources" page
   - Select source to sync
   - Choose databases/tables (if applicable)
   - Click "Sync" button
   - Monitor progress in real-time

5. **View Results**
   - Check dashboard for sync status
   - View sync history
   - Review analytics and reports

### Common Operations

#### Adding SQL Server Source
1. Navigate to "Add Source" → "SQL Server"
2. Enter server name, host, username, password
3. Test connection
4. Select target database (PostgreSQL)
5. Save

#### Adding HANA Source
1. Navigate to "Add Source" → "SAP HANA"
2. Enter HANA host, port, username, password
3. Test connection
4. Select target (ClickHouse)
5. Save

#### Adding REST API Source
1. Navigate to "Add Source" → "REST API"
2. Enter API endpoint URL
3. Configure authentication (OAuth/Bearer token)
4. Test connection
5. Select target (ClickHouse)
6. Configure polling interval (optional)
7. Save

#### Scheduling Sync Operations
1. Navigate to "Schedule" page
2. Select server/source
3. Choose sync type:
   - Interval-based (every X minutes/hours)
   - Daily (specific time)
4. Configure schedule details
5. Save schedule

#### Uploading Files
1. Navigate to "Upload" page
2. Select file (CSV, Excel, TXT)
3. Choose target database (PostgreSQL or ClickHouse)
4. Select target database name
5. Upload and monitor progress

#### Viewing Analytics
1. Navigate to "Advanced Analytics" page
2. Select server/source
3. Choose analysis type:
   - Database history
   - Table history
   - Sync reports
   - Schema changes
4. Review results

---

## Security & Authentication

### Role-Based Access Control (RBAC)

**Admin Role:**
- Full system access
- User management
- Server/source configuration
- Sync operations
- Schedule management
- Analytics access

**Operator Role:**
- Sync operations
- Schedule management
- View sync history
- Limited configuration access

**Viewer Role:**
- Read-only access
- View dashboards
- View reports and analytics
- No modification capabilities

### Security Features

- **Password Hashing**: Bcrypt with salt
- **Session Management**: Secure session handling
- **SQL Injection Protection**: Parameterized queries
- **XSS Protection**: Input sanitization
- **CSRF Protection**: Flask-WTF integration (if enabled)
- **Connection Encryption**: SSL/TLS support for database connections

### User Management

- Create users via web interface
- Assign roles during user creation
- Password reset functionality
- User deletion and management
- Audit trail of user actions

---

## Analytics & Monitoring

### Dashboard Features

- **Sync History**: Last 10 sync operations
- **Status Overview**: Current sync states
- **Server Metrics**: Performance indicators
- **Database Metrics**: Table and row counts
- **Error Tracking**: Failed sync details

### Advanced Analytics

- **Database History**: Historical sync data per database
- **Table History**: Per-table sync tracking
- **Delta Tracking**: Changes between syncs
- **Top Changed Tables**: Most frequently updated tables
- **Failed Sync Detection**: Identification of problematic syncs
- **Schema Change Detection**: Automatic schema change alerts

### Metrics Collection

- Server-level metrics
- Database-level statistics
- Table row counts
- Sync duration tracking
- Error rate monitoring

### Logging

- Application logs: `app.log`
- Sync logs: `hybrid_sync.log`
- PostgreSQL logs: `load_postgres.log`
- Worker logs: `run_sync_worker.log`
- Email logs: `email_delivery.log`

---

## API Integration

### REST API Features

- **Endpoint Configuration**: Configure API URLs
- **Authentication Methods**:
  - OAuth 2.0 (with token refresh)
  - Bearer token
  - Basic authentication
  - API keys
- **Data Fetching**: Automatic data retrieval
- **Polling Support**: Scheduled API polling
- **Data Transformation**: Automatic JSON to table conversion
- **ClickHouse Integration**: Direct insertion to ClickHouse

### OAuth 2.0 Flow

1. Configure OAuth credentials
2. Obtain authorization code
3. Exchange code for access token
4. Use token for API requests
5. Automatic token refresh (if refresh token provided)

### API Source Management

- Add multiple API sources
- Configure different endpoints
- Set up polling intervals
- Monitor API sync status
- View API sync history

---

## Scheduling & Automation

### Schedule Types

**Interval-Based:**
- Every N minutes
- Every N hours
- Configurable intervals

**Daily:**
- Specific time of day
- Daily recurrence
- Timezone support

### Schedule Management

- Create schedules via web interface
- Edit existing schedules
- Delete schedules
- View schedule status
- Schedule persistence (survives restarts)

### Background Processing

- Non-blocking sync operations
- Concurrent sync support (up to 3 simultaneous)
- Automatic retry on failure
- Progress tracking
- Real-time status updates

---

## Testing Tools

### Test Scripts

- `test_hana_to_clickhouse_migration.py`: HANA migration testing
- `test_postgres_connectivity.py`: PostgreSQL connection test
- `test_hana_connection.py`: HANA connection test
- `verify_setup.py`: System setup verification
- Various API and sync test scripts

### Test Coverage

- Connection testing
- Sync operation testing
- Authentication testing
- API integration testing
- Performance testing

---

## Deployment

### Production Considerations

**Environment Setup:**
- Set `FLASK_DEBUG=0`
- Use strong `SECRET_KEY`
- Configure production database
- Set up SSL/TLS certificates
- Configure firewall rules

**Database:**
- Use production PostgreSQL instance
- Configure connection pooling
- Set up database backups
- Monitor database performance

**Security:**
- Change default admin password
- Use strong passwords for all accounts
- Enable HTTPS
- Configure firewall
- Regular security updates

**Monitoring:**
- Set up log rotation
- Configure email alerts
- Monitor disk space
- Track performance metrics

**Scalability:**
- Use WSGI server (Gunicorn, uWSGI)
- Configure reverse proxy (Nginx)
- Set up load balancing (if needed)
- Database connection pooling

### Running in Production

**Using Gunicorn:**
```bash
gunicorn -w 4 -b 0.0.0.0:5000 app:app
```

**Using uWSGI:**
```bash
uwsgi --http :5000 --wsgi-file app.py --callable app
```

**Using Nginx Reverse Proxy:**
```nginx
server {
    listen 80;
    server_name your-domain.com;
    
    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

### Docker Support

- Dockerfile available
- docker-compose configuration
- Database initialization scripts
- Volume mounting for data persistence

---

## File Structure

```
nitin_sir5/
├── app.py                      # Main Flask application
├── requirements.txt            # Python dependencies
├── .env                        # Environment variables (create this)
├── config/
│   └── db_connections.yaml     # Database configuration
├── templates/                  # HTML templates
├── static/                     # CSS, JS, images
├── init-db/                    # Database initialization scripts
├── data/                       # Data export directory
├── utils/                      # Utility modules
│   └── email_service.py        # Email notification service
├── hybrid_sync.py             # SQL Server sync engine
├── hana_sync.py               # HANA migration module
├── api_sync.py                # REST API integration
├── sync_manager.py            # Sync orchestration
├── auth.py                     # Authentication system
├── scheduler_utils.py          # Job scheduling
├── analytics.py                # Basic analytics
├── analytics_advanced.py       # Advanced analytics
├── dashboard.py                # Dashboard data
├── metrics.py                  # Metrics collection
├── db_utils.py                 # Database utilities
├── manage_server.py            # Server management
└── tests/                      # Test scripts
```

---

## Troubleshooting

### Common Issues

**Connection Failures:**
- Verify network connectivity
- Check firewall rules
- Verify credentials
- Test with connection test scripts

**Sync Failures:**
- Check source database accessibility
- Verify target database permissions
- Review sync logs
- Check disk space

**Performance Issues:**
- Adjust batch sizes
- Optimize database queries
- Check network latency
- Monitor system resources

**Email Notifications Not Working:**
- Verify SMTP configuration
- Check email credentials
- Review email logs
- Test with test email script

---

## Support & Maintenance

### Log Files

- `app.log`: Application logs
- `hybrid_sync.log`: Sync operation logs
- `load_postgres.log`: PostgreSQL operation logs
- `email_delivery.log`: Email delivery logs

### Maintenance Tasks

- Regular database backups
- Log file rotation
- Database optimization
- Security updates
- Performance monitoring

---

## License & Credits

This is an enterprise data migration and synchronization platform designed for production use. Ensure proper licensing for all dependencies and database systems used.

---

**Version:** 1.0  
**Last Updated:** 2024  
**Platform:** Flask (Python)  
**Database:** PostgreSQL, ClickHouse, SQL Server, SAP HANA

---

For detailed setup instructions, refer to configuration sections above. For specific feature documentation, explore the web interface help sections.

