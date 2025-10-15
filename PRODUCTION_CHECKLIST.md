# PRODUCTION DEPLOYMENT CHECKLIST

## ✅ CRITICAL SECURITY CONFIGURATIONS

### 1. Environment Variables (.env)
- [ ] **SECRET_KEY**: Set to a strong, random secret key (NOT "NITIN_SIR")
- [ ] **PG_PASSWORD**: Set to production PostgreSQL password
- [ ] **DEFAULT_ADMIN_PASSWORD**: Set to strong password or disable
- [ ] **CREATE_DEFAULT_ADMIN**: Set to 0 in production (create admin manually)
- [ ] **FLASK_DEBUG**: Set to 0
- [ ] **APP_USE_DEBUGGER**: Set to 0

### 2. Database Configurations
- [ ] **PostgreSQL**: Update connection details in .env
- [ ] **SQL Server**: Update credentials in config/db_connections.yaml
- [ ] **Remove test data**: Clear any development/test databases
- [ ] **Backup strategy**: Implement database backup procedures

### 3. Network & Host Settings
- [ ] **APP_HOST**: Set to appropriate production host (0.0.0.0 for Docker, specific IP for server)
- [ ] **APP_PORT**: Set to production port (typically 5000 or 8080)
- [ ] **Firewall**: Configure firewall rules for required ports
- [ ] **SSL/HTTPS**: Configure reverse proxy (nginx/Apache) with SSL certificates

## ✅ CONFIGURATION AUDIT

### Hardcoded Values Found & Actions Required:
1. **SECRET_KEY fallback**: Change from "NITIN_SIR" to strong random key
2. **Default admin password**: "admin123" - MUST be changed
3. **Localhost references**: Update all localhost to production hostnames
4. **Test passwords**: Remove all "root", "test", "admin" passwords

### Files to Update:
- [ ] `.env` - All production values
- [ ] `config/db_connections.yaml` - Production database credentials
- [ ] Remove or secure test files: `test_*.py`, `debug_*.py`

## ✅ FUNCTIONAL VERIFICATION

### Core Features Tested:
- [x] **Schedule Creation**: Working ✓
- [x] **Schedule Execution**: Working ✓  
- [x] **Schedule Deletion**: Working ✓
- [x] **Background Syncs**: Working ✓
- [x] **Real-time Status**: Working ✓
- [x] **Authentication**: Working ✓
- [x] **API Endpoints**: Fixed missing /api/job-statuses ✓

### Features to Test in Production:
- [ ] **Email notifications** (if configured)
- [ ] **Slack notifications** (if configured)
- [ ] **Large database syncs** (performance testing)
- [ ] **Multiple concurrent syncs**
- [ ] **Error recovery and logging**

## ✅ PRODUCTION DEPLOYMENT STEPS

### 1. Pre-deployment
```bash
# Clone repository to production server
git clone <repository-url>
cd nitin_sir5

# Copy and configure environment
cp .env.example .env
# Edit .env with production values

# Install dependencies
pip install -r requirements.txt
```

### 2. Database Setup
```bash
# Ensure PostgreSQL is installed and running
# Create production database and user
# Update .env with production database credentials
```

### 3. Security Hardening
```bash
# Generate strong SECRET_KEY
python -c "import secrets; print(secrets.token_hex(32))"

# Create production admin user (disable auto-creation)
# Set CREATE_DEFAULT_ADMIN=0
# Use web interface to create admin manually
```

### 4. Production Deployment Options

#### Option A: Direct Python (Development/Testing)
```bash
python app.py
```

#### Option B: Production WSGI Server (Recommended)
```bash
# Install gunicorn
pip install gunicorn

# Run with gunicorn
gunicorn -w 4 -b 0.0.0.0:5000 app:app
```

#### Option C: Docker Deployment
```bash
# Use existing Dockerfile if available
docker build -t sql-pg-sync .
docker run -d -p 5000:5000 --env-file .env sql-pg-sync
```

#### Option D: Systemd Service (Linux)
Create `/etc/systemd/system/sql-pg-sync.service`:
```ini
[Unit]
Description=SQL-PostgreSQL Sync Service
After=network.target

[Service]
Type=simple
User=appuser
WorkingDirectory=/path/to/app
Environment=PATH=/path/to/venv/bin
ExecStart=/path/to/venv/bin/gunicorn -w 4 -b 0.0.0.0:5000 app:app
Restart=always

[Install]
WantedBy=multi-user.target
```

### 5. Reverse Proxy Setup (Nginx)
```nginx
server {
    listen 80;
    server_name your-domain.com;
    
    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

## ✅ MONITORING & MAINTENANCE

### Logging
- [ ] **Application logs**: Configure log rotation
- [ ] **Error monitoring**: Set up error tracking
- [ ] **Performance monitoring**: Monitor sync performance

### Backup Strategy
- [ ] **Database backups**: Automated PostgreSQL backups
- [ ] **Configuration backups**: Backup .env and YAML files
- [ ] **Application backups**: Source code and data directory

### Health Checks
- [ ] **Application health endpoint**: Monitor /dashboard/data
- [ ] **Database connectivity**: Monitor PostgreSQL and SQL Server connections
- [ ] **Scheduled job monitoring**: Verify schedules are running

## ✅ POST-DEPLOYMENT VERIFICATION

### Immediate Tests:
1. [ ] **Web interface accessible**
2. [ ] **Login with admin credentials**
3. [ ] **Create test schedule**
4. [ ] **Run manual sync**
5. [ ] **Verify data sync to PostgreSQL**
6. [ ] **Check all navigation menus**
7. [ ] **Test user creation/management**

### Performance Tests:
1. [ ] **Large database sync**
2. [ ] **Multiple concurrent syncs**
3. [ ] **Schedule execution during high load**

## ✅ MAINTENANCE PROCEDURES

### Regular Tasks:
- [ ] **Database cleanup**: Old sync logs, completed schedules
- [ ] **Log rotation**: Prevent disk space issues
- [ ] **Security updates**: Keep dependencies updated
- [ ] **Performance monitoring**: Database query optimization

### Emergency Procedures:
- [ ] **Backup restoration process**
- [ ] **Service restart procedures**
- [ ] **Database recovery procedures**
- [ ] **Emergency contact information**

## 🚨 CRITICAL WARNINGS

1. **NEVER use default passwords in production**
2. **ALWAYS use HTTPS in production**
3. **BACKUP before any major changes**
4. **TEST all functionality in staging first**
5. **MONITOR logs for security issues**

## 📞 SUPPORT INFORMATION

- **Application logs**: `app.log`
- **Sync logs**: Check dashboard for detailed sync history
- **Database logs**: PostgreSQL and SQL Server logs
- **Configuration**: `.env` and `config/db_connections.yaml`

---

**Status**: Ready for production deployment after addressing the checklist items above.