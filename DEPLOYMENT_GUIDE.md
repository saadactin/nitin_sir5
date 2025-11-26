# 🚀 Production Deployment Guide

This guide walks you through deploying the application to production.

## Prerequisites

- Docker and Docker Compose installed
- Domain name (optional, for HTTPS)
- SSL certificate (optional, for HTTPS)
- Production database credentials

## Quick Start with Docker

### 1. Clone and Prepare

```bash
git clone <your-repo>
cd nitin_sir5
```

### 2. Create Environment File

Create `.env` file with production values:

```env
# Database
POSTGRES_HOST=postgres
POSTGRES_DB=your_production_db
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_secure_password

# ClickHouse
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your_password
CLICKHOUSE_DATABASE=your_database

# Flask
SECRET_KEY=your-secret-key-here-change-this
FLASK_ENV=production

# Email (optional)
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your-email@gmail.com
SMTP_PASSWORD=your-app-password
ADMIN_EMAILS=admin@example.com

# Rate Limiting (optional - use Redis)
RATE_LIMIT_STORAGE_URI=redis://redis:6379
```

### 3. Build and Run

```bash
# Build and start all services
docker-compose up -d

# View logs
docker-compose logs -f app

# Check health
curl http://localhost:5002/health
```

## Deployment Options

### Option 1: Docker Compose (Recommended for Small/Medium Deployments)

**Pros**: Simple, includes all services  
**Cons**: Single server, no auto-scaling

```bash
docker-compose up -d
```

### Option 2: Docker with Gunicorn (Production)

**Pros**: Better performance, production WSGI server  
**Cons**: Need to manage databases separately

```bash
# Build image
docker build -t sql-sync-app .

# Run container
docker run -d \
  --name sql-sync-app \
  -p 5002:5002 \
  --env-file .env \
  sql-sync-app
```

### Option 3: Kubernetes (Enterprise)

**Pros**: Auto-scaling, high availability  
**Cons**: Complex setup

See `kubernetes/` directory for manifests (create if needed).

## Production Checklist

### Before Deployment

- [ ] All secrets moved to environment variables
- [ ] Database migrations run
- [ ] Health check endpoint tested
- [ ] Environment variables validated
- [ ] HTTPS configured (if exposed to internet)
- [ ] Backup strategy implemented
- [ ] Monitoring and alerting set up
- [ ] Log rotation configured
- [ ] Rate limiting tuned
- [ ] Connection pool sizes optimized

### Security

- [ ] `SECRET_KEY` is strong and unique
- [ ] Database passwords are strong
- [ ] HTTPS enforced (if public)
- [ ] CORS configured properly
- [ ] Security headers enabled
- [ ] Rate limiting enabled
- [ ] Input validation on all endpoints
- [ ] SQL injection prevention verified

### Performance

- [ ] Connection pools sized correctly
- [ ] Database indexes created
- [ ] Query optimization done
- [ ] Caching strategy implemented
- [ ] Static files served via CDN (if applicable)

### Monitoring

- [ ] Health checks configured
- [ ] Error tracking set up (Sentry)
- [ ] Metrics collection (Prometheus)
- [ ] Log aggregation (ELK/CloudWatch)
- [ ] Alerting rules configured

## Running with Gunicorn (Linux)

```bash
# Install Gunicorn
pip install gunicorn

# Run with config file
gunicorn -c gunicorn_config.py app:app

# Or with command line options
gunicorn --bind 0.0.0.0:5002 --workers 4 app:app
```

## Running with Waitress (Windows)

```bash
# Install Waitress
pip install waitress

# Run
waitress-serve --host=0.0.0.0 --port=5002 app:app
```

## Nginx Reverse Proxy (Recommended)

```nginx
server {
    listen 80;
    server_name your-domain.com;
    
    # Redirect to HTTPS
    return 301 https://$server_name$request_uri;
}

server {
    listen 443 ssl http2;
    server_name your-domain.com;
    
    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;
    
    location / {
        proxy_pass http://127.0.0.1:5002;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # WebSocket support (if needed)
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
    
    # Health check
    location /health {
        proxy_pass http://127.0.0.1:5002/health;
        access_log off;
    }
}
```

## Database Migrations

```bash
# Run migrations (if using Alembic)
alembic upgrade head

# Or manually run SQL scripts
psql -h localhost -U postgres -d your_db -f migrations/001_initial.sql
```

## Backup Strategy

### Automated Backups

```bash
# PostgreSQL backup script
#!/bin/bash
DATE=$(date +%Y%m%d_%H%M%S)
pg_dump -h localhost -U postgres your_db > backups/db_$DATE.sql

# Keep last 7 days
find backups/ -name "db_*.sql" -mtime +7 -delete
```

### Schedule with Cron

```cron
# Daily backup at 2 AM
0 2 * * * /path/to/backup_script.sh
```

## Monitoring

### Health Checks

```bash
# Basic health check
curl http://localhost:5002/health

# Readiness check
curl http://localhost:5002/ready

# Metrics (Prometheus format)
curl http://localhost:5002/metrics
```

### Log Monitoring

```bash
# View application logs
docker-compose logs -f app

# View specific log file
tail -f sync_operations.log
```

## Troubleshooting

### Application Won't Start

1. Check environment variables: `docker-compose config`
2. Check logs: `docker-compose logs app`
3. Verify database connectivity
4. Check port availability

### Database Connection Issues

1. Verify database is running: `docker-compose ps`
2. Check connection string in `.env`
3. Test connection: `psql -h localhost -U postgres -d your_db`
4. Check firewall rules

### Performance Issues

1. Check connection pool stats: `/metrics` endpoint
2. Review slow query logs
3. Monitor resource usage: `docker stats`
4. Adjust worker count in Gunicorn config

## Scaling

### Horizontal Scaling

```bash
# Scale app service
docker-compose up -d --scale app=3

# Use load balancer (Nginx/HAProxy)
```

### Vertical Scaling

1. Increase worker count in `gunicorn_config.py`
2. Increase connection pool sizes
3. Add more database connections
4. Increase server resources

## Rollback Procedure

```bash
# Stop current version
docker-compose down

# Checkout previous version
git checkout <previous-tag>

# Rebuild and start
docker-compose up -d --build
```

## Maintenance

### Updates

```bash
# Pull latest code
git pull

# Rebuild and restart
docker-compose up -d --build

# Run migrations
docker-compose exec app python -m alembic upgrade head
```

### Log Rotation

```bash
# Configure logrotate
/path/to/logs/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
}
```

## Support

For issues or questions:
1. Check logs: `docker-compose logs`
2. Review health endpoint: `/health`
3. Check metrics: `/metrics`
4. Review documentation: `PRODUCTION_READINESS.md`

