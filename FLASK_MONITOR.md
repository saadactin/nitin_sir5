# Flask Server Monitor

## Overview
This monitoring system watches your Flask application server and automatically sends email alerts when the server goes down, crashes, or becomes unresponsive.

## Features
✅ **Automatic Email Alerts** - Sends email immediately when Flask server stops responding
✅ **Continuous Monitoring** - Checks server health every 60 seconds
✅ **Alert Cooldown** - Prevents spam by waiting 5 minutes between repeat alerts
✅ **Easy to Use** - Simple startup scripts for both PowerShell and Command Prompt

## Email Alert Details
When the Flask server goes down, you'll receive an email with:
- **Subject**: `[ALERT][SERVER DOWN] Flask Application Server`
- **Recipients**: Your configured admin emails
- **Content**: Timestamp, error details, and restart instructions

## Quick Start

### Option 1: Using Batch File (Recommended for Windows)
1. Double-click `start-monitor.bat`
2. Keep the window open to continue monitoring
3. Press Ctrl+C to stop

### Option 2: Using PowerShell
```powershell
.\start-monitor.ps1
```

### Option 3: Using Python Directly
```powershell
.\myenv1\Scripts\python.exe monitor_flask_server.py
```

## How It Works
1. **Health Check**: Every 60 seconds, the monitor sends a request to `http://127.0.0.1:5001/login`
2. **Detection**: If the server doesn't respond, it's marked as DOWN
3. **Email Alert**: An immediate email is sent to all configured admin emails
4. **Recovery Detection**: When the server comes back online, a log message is recorded

## Configuration

### Monitor Settings (in `monitor_flask_server.py`)
```python
FLASK_URL = "http://127.0.0.1:5001"  # Flask server URL
CHECK_INTERVAL_SECONDS = 60          # Check every 60 seconds
ALERT_COOLDOWN_SECONDS = 300         # Wait 5 minutes between alerts
```

### Email Settings (in `.env`)
```env
EMAIL_HOST_USER=your-email@gmail.com
EMAIL_HOST_PASSWORD=your-app-password
ADMIN_EMAILS=email1@domain.com,email2@domain.com
```

## Running in Production

### As a Background Process
To run the monitor in the background without keeping a window open:

**PowerShell:**
```powershell
Start-Process powershell -ArgumentList "-File start-monitor.ps1" -WindowStyle Hidden
```

**Command Prompt:**
```cmd
start /B myenv1\Scripts\python.exe monitor_flask_server.py
```

### As a Windows Service
For production environments, consider using a Windows Service manager like:
- **NSSM (Non-Sucking Service Manager)** - Recommended
- **Windows Task Scheduler** - Built-in option

#### Using NSSM (Recommended):
1. Download NSSM from https://nssm.cc/download
2. Install the monitor as a service:
```cmd
nssm install FlaskMonitor "C:\path\to\myenv1\Scripts\python.exe" "C:\path\to\monitor_flask_server.py"
nssm start FlaskMonitor
```

#### Using Windows Task Scheduler:
1. Open Task Scheduler
2. Create Basic Task
3. Trigger: "At startup" or "Daily"
4. Action: Start program
   - Program: `C:\path\to\myenv1\Scripts\python.exe`
   - Arguments: `monitor_flask_server.py`
   - Start in: `C:\path\to\project\directory`

## Logs
The monitor outputs logs in real-time showing:
- ✓ Server check OK
- ✗ Server is DOWN
- Email alert sent confirmations

## Testing the Monitor

### Test if emails work:
```powershell
.\myenv1\Scripts\python.exe -c "from monitor_flask_server import send_alert_email; send_alert_email()"
```

### Test server health check:
```powershell
.\myenv1\Scripts\python.exe -c "from monitor_flask_server import check_server_health; print('Server is UP' if check_server_health() else 'Server is DOWN')"
```

## Troubleshooting

### Monitor not sending emails
1. Check `.env` file has correct EMAIL settings
2. Test email service directly:
   ```powershell
   .\myenv1\Scripts\python.exe test_email_now.py
   ```
3. Check email_delivery.log for error details

### False alerts (server is running but monitor says DOWN)
1. Check firewall settings allow local connections
2. Verify Flask is running on http://127.0.0.1:5001
3. Check Flask logs for errors

### Monitor stops running
1. Run as a Windows Service (see above)
2. Check for Python crashes in Event Viewer
3. Ensure virtual environment is activated

## Dependencies
- `requests` - HTTP library for health checks
- `python-dotenv` - Environment variable loading
- Your existing `utils.email_service` module

## Support
For issues or questions, check:
- `email_delivery.log` - Email sending history
- Flask application logs - Server errors
- Monitor console output - Health check status
