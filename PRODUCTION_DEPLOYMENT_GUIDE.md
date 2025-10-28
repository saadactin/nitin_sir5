# 🚀 Production Deployment Guide - ACTIN Data Sync

**Last Updated**: October 27, 2025  
**Production Server**: Waitress (Python WSGI Server)  
**Status**: ✅ Production-Ready

---

## 📋 Overview

The ACTIN Data Sync application now runs on **Waitress**, a production-ready pure-Python WSGI server that is:
- ✅ **Production-Ready**: Stable and battle-tested
- ✅ **Windows-Compatible**: Works excellently on Windows
- ✅ **Thread-Safe**: Handles concurrent requests efficiently
- ✅ **No Dependencies**: Pure Python, no C compiler needed
- ✅ **HTTP Support**: Works with HTTP (HTTPS can be added later via reverse proxy)

---

## 🎯 Quick Start

### Method 1: PowerShell Script (Recommended)
```powershell
.\run-production.ps1
```

### Method 2: Direct Python
```powershell
python run_production.py
```

### Method 3: Development Mode (Not for production)
```powershell
python app.py
```

---

## 📦 Installation

### 1. Install Dependencies
```powershell
pip install -r requirements.txt
```

This will install:
- `waitress==3.0.1` - Production WSGI server
- All other application dependencies

### 2. Configure Environment

Create or update `.env` file:
```bash
# Flask Configuration
SECRET_KEY=your-secret-key-change-in-production-2025
FLASK_DEBUG=0

# Production Server Configuration
APP_HOST=0.0.0.0
APP_PORT=5001
WAITRESS_THREADS=4
WAITRESS_CHANNEL_TIMEOUT=60

# Database Configuration
PG_HOST=localhost
PG_PORT=5432
PG_USER=postgres
PG_PASSWORD=your_password
PG_DATABASE=actin_sync

# ClickHouse Configuration
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=

# Email Configuration (Optional)
EMAIL_USER=your-email@gmail.com
EMAIL_PASSWORD=your-app-password
ADMIN_EMAIL=admin@actin.co.in

# Session Security
SESSION_COOKIE_SECURE=0  # Set to 1 when using HTTPS
SESSION_COOKIE_HTTPONLY=1
SESSION_COOKIE_SAMESITE=Lax

# Default Admin (Created on first run)
DEFAULT_ADMIN_PASSWORD=admin123
CREATE_DEFAULT_ADMIN=1
```

---

## 🔧 Configuration Options

### Server Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `APP_HOST` | `0.0.0.0` | Host to bind (0.0.0.0 = all interfaces) |
| `APP_PORT` | `5001` | Port to listen on |
| `WAITRESS_THREADS` | `4` | Number of worker threads |
| `WAITRESS_CHANNEL_TIMEOUT` | `60` | Request timeout in seconds |
| `FLASK_DEBUG` | `0` | Debug mode (0 for production) |

### Host Options:
- `0.0.0.0` - Listen on all network interfaces (accessible from network)
- `127.0.0.1` - Listen only on localhost (local access only)
- `192.168.1.100` - Listen on specific IP

---

## 🌐 Accessing the Application

### Local Access:
```
http://localhost:5001
http://127.0.0.1:5001
```

### Network Access:
```
http://<your-ip-address>:5001
http://192.168.1.100:5001  (example)
```

**Find your IP address**:
```powershell
ipconfig
# Look for IPv4 Address under your active network adapter
```

---

## 🚀 Starting the Server

### Option 1: PowerShell Script (Best for Windows)
```powershell
# Navigate to project directory
cd C:\Users\SaadSayyed\Desktop\test2\nitin_sir5

# Run production server
.\run-production.ps1
```

**Output**:
```
======================================================================
🚀 ACTIN DATA SYNC - PRODUCTION SERVER
======================================================================
📅 Date: 2025-10-27 17:32:33
🐍 Python: 3.12.0
🔧 WSGI Server: Waitress (Production-Ready)
======================================================================

✅ APPLICATION READY
🌍 Access the application at: http://localhost:5001
📊 Dashboard: http://localhost:5001/
🔐 Login: http://localhost:5001/login

💡 Press Ctrl+C to stop the server
======================================================================
```

### Option 2: Direct Python
```powershell
python run_production.py
```

### Option 3: Background Service (Windows)
```powershell
# Using NSSM (Non-Sucking Service Manager)
# 1. Download NSSM: https://nssm.cc/download
# 2. Install service
nssm install ActinDataSync "C:\Python312\python.exe" "C:\Users\SaadSayyed\Desktop\test2\nitin_sir5\run_production.py"

# Start service
nssm start ActinDataSync

# Stop service
nssm stop ActinDataSync

# Remove service
nssm remove ActinDataSync confirm
```

---

## 🛑 Stopping the Server

### Interactive Mode:
```
Press Ctrl+C
```

### Background Process:
```powershell
# Find Python processes
Get-Process python

# Stop specific process
Stop-Process -Id <PID>

# Or stop all Python processes (careful!)
Stop-Process -Name python
```

---

## 📊 Monitoring

### View Logs

**Production Log**:
```powershell
Get-Content production.log -Tail 50 -Wait
```

**Application Log**:
```powershell
Get-Content app.log -Tail 50 -Wait
```

### Check Server Status
```powershell
# Check if port is listening
netstat -ano | findstr :5001

# Check Python processes
Get-Process python

# Test connection
Invoke-WebRequest -Uri http://localhost:5001 -Method GET
```

---

## 🔒 Security Considerations

### Current Configuration (HTTP):
- ✅ Rate limiting enabled (10 login attempts/minute)
- ✅ Password strength validation
- ✅ Input sanitization (XSS prevention)
- ✅ Session security (HTTP-only cookies)
- ✅ Security event logging
- ⚠️ HTTP only (no encryption in transit)
- ⚠️ Force HTTPS disabled

### For Production (Recommended):
1. **Use Reverse Proxy** (Nginx/Apache) for HTTPS
2. **Enable Firewall** rules to restrict access
3. **Change Default Passwords**
4. **Set Strong SECRET_KEY**
5. **Enable HTTPS** in security.py (set `force_https=True`)

---

## 🔐 HTTPS Setup (Optional - Future)

### Option 1: Reverse Proxy (Recommended)

**Using Nginx**:
```nginx
server {
    listen 443 ssl;
    server_name yourdomain.com;

    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;

    location / {
        proxy_pass http://127.0.0.1:5001;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

**Enable HTTPS in security.py**:
```python
# In security.py, change:
force_https=True,
strict_transport_security=True,
```

**Set environment variable**:
```bash
SESSION_COOKIE_SECURE=1
```

### Option 2: Self-Signed Certificate (Development)
```powershell
# Generate self-signed certificate
openssl req -x509 -newkey rsa:4096 -nodes -out cert.pem -keyout key.pem -days 365

# Modify run_production.py to use SSL (advanced)
# Note: Waitress doesn't support SSL directly, use reverse proxy instead
```

---

## 🐛 Troubleshooting

### Issue: Port Already in Use
**Error**: `OSError: [WinError 10048] Only one usage of each socket address`

**Solution**:
```powershell
# Find process using port 5001
netstat -ano | findstr :5001

# Kill the process
taskkill /PID <PID> /F

# Or change port in .env
APP_PORT=5002
```

### Issue: "Bad request version" Errors (SSL Handshake)
**Cause**: Browser trying HTTPS on HTTP server

**Solution 1** - Use HTTP explicitly:
```
http://localhost:5001  (not https://)
```

**Solution 2** - Clear browser cache:
```
Ctrl + Shift + Delete > Clear browsing data
```

**Solution 3** - Use incognito/private mode

### Issue: Database Connection Failed
**Error**: Missing `PG_HOST`, `PG_PORT`, etc.

**Solution**:
```powershell
# Verify .env file exists and contains database config
Get-Content .env | Select-String "PG_"

# Test PostgreSQL connection
python -c "import psycopg2; conn = psycopg2.connect(host='localhost', port=5432, user='postgres', password='your_password', database='actin_sync'); print('✅ Connected')"
```

### Issue: Rate Limit Errors
**Error**: `429 Too Many Requests`

**Solution**:
```python
# In security.py, adjust limits:
default_limits=["1000 per hour", "200 per minute"]

# Or in specific route:
@security_manager.limiter.limit("20 per minute")
```

### Issue: Slow Performance
**Solution**:
```bash
# Increase worker threads
WAITRESS_THREADS=8

# Increase channel timeout
WAITRESS_CHANNEL_TIMEOUT=120
```

---

## 📈 Performance Tuning

### Recommended Settings by Load:

**Low Traffic (< 100 users/day)**:
```bash
WAITRESS_THREADS=4
WAITRESS_CHANNEL_TIMEOUT=60
```

**Medium Traffic (100-1000 users/day)**:
```bash
WAITRESS_THREADS=8
WAITRESS_CHANNEL_TIMEOUT=90
```

**High Traffic (> 1000 users/day)**:
```bash
WAITRESS_THREADS=16
WAITRESS_CHANNEL_TIMEOUT=120
# Consider using Gunicorn with multiple workers instead
```

---

## 🔄 Deployment Checklist

### Pre-Deployment:
- [ ] Install dependencies (`pip install -r requirements.txt`)
- [ ] Configure `.env` file with production values
- [ ] Set strong `SECRET_KEY`
- [ ] Change default admin password
- [ ] Test database connections
- [ ] Set `FLASK_DEBUG=0`
- [ ] Review security settings

### Deployment:
- [ ] Start production server (`python run_production.py`)
- [ ] Verify server is listening (`netstat -ano | findstr :5001`)
- [ ] Test HTTP access (`http://localhost:5001`)
- [ ] Test login functionality
- [ ] Test sync operations
- [ ] Check logs for errors

### Post-Deployment:
- [ ] Monitor `production.log` and `app.log`
- [ ] Set up monitoring/alerting
- [ ] Configure backups
- [ ] Document access URLs
- [ ] Train users on new URL/port
- [ ] Set up firewall rules
- [ ] Consider setting up Windows Service

---

## 📞 Support

### Logs Location:
- Production log: `production.log`
- Application log: `app.log`
- Sync logs: Database `sync_history` table

### Common Commands:
```powershell
# View real-time logs
Get-Content production.log -Tail 50 -Wait

# Check server status
Get-Process python | Where-Object {$_.Path -like "*nitin_sir5*"}

# Test endpoint
Invoke-WebRequest -Uri http://localhost:5001/login

# Check port
Test-NetConnection -ComputerName localhost -Port 5001
```

---

## 🎯 Next Steps

### Immediate:
1. ✅ Production server running (Waitress)
2. ✅ HTTP working correctly
3. ✅ Security features enabled

### Short-term:
1. Configure Windows Service for auto-start
2. Set up monitoring and alerting
3. Configure firewall rules
4. Document production URLs

### Long-term:
1. Set up reverse proxy (Nginx) for HTTPS
2. Implement SSL certificates
3. Set up load balancing (if needed)
4. Configure database replication
5. Set up CI/CD pipeline

---

## ✅ Success Criteria

**Your production server is ready when**:
- ✅ Server starts without errors
- ✅ Accessible via HTTP at configured port
- ✅ Login works correctly
- ✅ Sync operations complete successfully
- ✅ Logs show no critical errors
- ✅ Rate limiting prevents abuse
- ✅ Sessions persist across requests

---

**Congratulations! Your ACTIN Data Sync application is now running in production mode! 🎉**

For questions or issues, check the logs first, then refer to the troubleshooting section.

---

*Document Version: 1.0*  
*Last Updated: October 27, 2025*
