# Docker Deployment Guide

This guide provides instructions for deploying the SQL Server to PostgreSQL Sync Application using Docker.

## Prerequisites

- Docker Engine 20.10 or later
- Docker Compose 2.0 or later
- Access to SQL Server instances you want to sync from
- At least 2GB of available RAM
- 5GB of available disk space

## Quick Start

### 1. Build and Run with Docker Compose

```bash
# Clone or download the project
# Navigate to the project directory
cd sql-pg-sync

# Copy environment template and customize
cp .env.example .env
# Edit .env file with your specific settings

# Start all services
docker-compose up -d

# Or start with pgAdmin for database management
docker-compose --profile tools up -d
```

### 2. Access the Application

- **Web Interface**: http://localhost:5000
- **Default Login**: admin / admin123
- **pgAdmin** (if enabled): http://localhost:8080

### 3. Configure SQL Server Connections

1. Login to the web interface
2. Navigate to "Add Server" or use the command line:

```bash
# Add SQL Server via CLI
docker exec sql-pg-sync-app python manage_server.py --add myserver sql-server-host sa password123
```

## Configuration

### Environment Variables

The application uses environment variables for configuration. Key variables:

```bash
# Database
POSTGRES_HOST=postgres          # PostgreSQL host
POSTGRES_DB=sync_db            # Database name
POSTGRES_USER=migration_user   # Username
POSTGRES_PASSWORD=password     # Password

# Application
HYBRID_SYNC_BATCH_SIZE=10000   # Sync batch size
FLASK_DEBUG=0                  # Debug mode (0 for production)

# Notifications (Optional)
SMTP_HOST=smtp.gmail.com       # Email SMTP host
SLACK_WEBHOOK_URL=https://...  # Slack webhook URL
```

### Persistent Data

The Docker setup includes persistent volumes:

- `postgres_data`: PostgreSQL database files
- `app_data`: Application data and exports
- `app_logs`: Application logs

## Building the Image

### Build Locally

```bash
# Build the application image
docker build -t sql-pg-sync:latest .

# Or build with specific tag
docker build -t sql-pg-sync:v1.0.0 .
```

### Build Arguments

The Dockerfile supports build arguments:

```bash
docker build \
  --build-arg PYTHON_VERSION=3.11 \
  --build-arg BATCH_SIZE=5000 \
  -t sql-pg-sync:custom .
```

## Deployment Scenarios

### Development

```bash
# Start with development settings
FLASK_DEBUG=1 docker-compose up
```

### Production

```bash
# Production deployment with specific settings
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

### Scaling

```bash
# Scale application instances (requires load balancer)
docker-compose up -d --scale app=3
```

## Management Commands

### Container Management

```bash
# View running containers
docker-compose ps

# View logs
docker-compose logs app
docker-compose logs postgres

# Follow logs
docker-compose logs -f app

# Restart services
docker-compose restart app

# Stop services
docker-compose stop

# Remove everything
docker-compose down -v
```

### Application Management

```bash
# Execute commands in the app container
docker exec sql-pg-sync-app python manage_server.py --list

# Access shell
docker exec -it sql-pg-sync-app bash

# View application logs
docker exec sql-pg-sync-app tail -f /app/hybrid_sync.log

# Initialize database manually
docker exec sql-pg-sync-app python -c "from db_utils import init_pg_schema; init_pg_schema()"
```

### Database Management

```bash
# Access PostgreSQL directly
docker exec -it sql-pg-sync-postgres psql -U migration_user -d sync_db

# Backup database
docker exec sql-pg-sync-postgres pg_dump -U migration_user sync_db > backup.sql

# Restore database
docker exec -i sql-pg-sync-postgres psql -U migration_user sync_db < backup.sql
```

## Health Checks

The containers include health checks:

```bash
# Check container health
docker-compose ps

# View health check details
docker inspect sql-pg-sync-app | grep -A 10 Health
```

## Troubleshooting

### Common Issues

**1. SQL Server Connection Issues**
```bash
# Check ODBC drivers
docker exec sql-pg-sync-app odbcinst -q -d -n "ODBC Driver 18 for SQL Server"

# Test SQL Server connectivity
docker exec sql-pg-sync-app python -c "
import pyodbc
conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER=your-server;UID=sa;PWD=password')
print('Connection successful')
"
```

**2. PostgreSQL Connection Issues**
```bash
# Check PostgreSQL status
docker exec sql-pg-sync-postgres pg_isready -U migration_user

# View PostgreSQL logs
docker-compose logs postgres
```

**3. Application Won't Start**
```bash
# Check application logs
docker-compose logs app

# Verify configuration
docker exec sql-pg-sync-app cat /app/config/db_connections.yaml
```

### Debugging

```bash
# Enable debug mode
FLASK_DEBUG=1 docker-compose up

# Run with verbose logging
LOG_LEVEL=DEBUG docker-compose up

# Execute debug commands
docker exec -it sql-pg-sync-app python -c "
from hybrid_sync import get_pg_engine
print('Testing PostgreSQL connection...')
engine = get_pg_engine()
with engine.connect() as conn:
    result = conn.execute('SELECT version()')
    print(result.fetchone())
"
```

## Security Considerations

1. **Change default passwords** in production
2. **Use environment variables** for sensitive data
3. **Enable SSL/TLS** for database connections
4. **Use secure networks** for container communication
5. **Regular updates** of base images and dependencies

## Monitoring

### Application Metrics

The web interface provides monitoring at:
- `/dashboard` - Sync status and history
- `/metrics/{server}` - Server-specific metrics
- `/alerts` - System alerts and issues

### Container Monitoring

```bash
# Monitor resource usage
docker stats sql-pg-sync-app sql-pg-sync-postgres

# View system information
docker system df
docker system info
```

## Updates

### Updating the Application

```bash
# Pull latest code
git pull

# Rebuild and restart
docker-compose build app
docker-compose up -d app

# Or rebuild everything
docker-compose build
docker-compose up -d
```

### Database Migrations

The application handles database schema initialization automatically. For manual migration:

```bash
docker exec sql-pg-sync-app python -c "
from db_utils import init_pg_schema
from hybrid_sync import create_sync_tracking_table, create_table_sync_tracking, get_pg_engine
init_pg_schema()
engine = get_pg_engine()
create_sync_tracking_table(engine)
create_table_sync_tracking(engine)
print('Database migration completed')
"
```