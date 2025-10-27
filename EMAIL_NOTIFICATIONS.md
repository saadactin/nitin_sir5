# Automatic Email Notification System

## 📧 Overview

Your application now automatically sends email notifications for all important sync events. No manual intervention required!

## ✅ Implemented Automatic Notifications

### 1. **Sync Completed Successfully** ✓
- **Triggered When**: Any sync finishes without errors
- **Includes**: Tables synced, row counts, duration
- **Example**: "[SYNC][COMPLETED] PROD-SQL-01"

### 2. **Sync Failed** ✗
- **Triggered When**: Sync encounters any error
- **Includes**: Error message, stack trace, server info
- **Example**: "[SYNC][FAILED] PROD-SQL-01"

### 3. **Server Down** ⚠️
- **Triggered When**: Connection to SQL Server fails
- **Includes**: Connection error details, troubleshooting info
- **Example**: "[ALERT][SERVER DOWN] BACKUP-SQL-02"

### 4. **Partial Sync Success** ⚠️
- **Triggered When**: Some tables sync successfully, others fail
- **Includes**: Success/failure breakdown, failed table list, error summary
- **Example**: "[SYNC][PARTIAL] PROD-SQL-01 - 17/20 tables synced"

### 5. **Row Count Mismatch** 📊
- **Triggered When**: Source and target row counts differ significantly (manual trigger)
- **Includes**: Source rows, target rows, percentage difference
- **Example**: "[DATA MISMATCH] PROD-SQL-01 - Orders"

### 6. **Daily Summary Report** 📊
- **Triggered When**: Every day at 11:59 PM automatically
- **Includes**: All syncs for the day, success rate, per-server status, top errors
- **Example**: "[DAILY SUMMARY] 2025-10-24 - 12/14 syncs successful"

---

## 🔧 How It Works

### Automatic Triggers

#### In `sync_manager.py`:
```python
# On Sync Success:
email_service.notify_sync_success(server_name, summary)

# On Sync Failure:
email_service.notify_sync_failed(server_name, error_message)

# On Connection Error:
email_service.notify_server_down(server_name, error_details)

# On Partial Success:
email_service.notify_sync_partial_success(
    server_name, total_tables, success_count, failed_tables, error_summary
)
```

#### Daily Summary:
- Runs automatically every day at 23:59
- Collects all sync data from the last 24 hours
- Sends comprehensive report

---

## 📬 Email Recipients

All emails are sent to addresses configured in `.env`:

```properties
ADMIN_EMAILS=saad.sayyed@actin.co.in,saadpractice4@gmail.com
```

To add more recipients, simply add them to the comma-separated list.

---

## 🛠️ Configuration

### Email Settings (in `.env`):

```properties
# Email Server
EMAIL_HOST=smtp.gmail.com
EMAIL_PORT=587
EMAIL_USE_TLS=True
EMAIL_HOST_USER=saadpractice4@gmail.com
EMAIL_HOST_PASSWORD=txludusmznqxoweo
ADMIN_EMAILS=saad.sayyed@actin.co.in,saadpractice4@gmail.com

# Email Behavior
EMAIL_RATE_LIMIT_PER_HOUR=5           # Max emails per hour per alert type
EMAIL_CIRCUIT_FAIL_THRESHOLD=3         # Open circuit after 3 failures
EMAIL_CIRCUIT_COOLDOWN_MINUTES=15      # Cool down period
EMAIL_DELIVERY_LOG=email_delivery.log  # Log file location
```

### What Each Setting Does:

- **RATE_LIMIT**: Prevents email spam by limiting emails per hour
- **CIRCUIT_BREAKER**: Temporarily stops sending if too many failures occur
- **DELIVERY_LOG**: Tracks all email attempts (success/failure)

---

## 📋 Email Features

### 1. **Professional HTML Formatting**
- Beautiful styled emails with colors
- Clear sections and tables
- Mobile-friendly design

### 2. **Plain Text Fallback**
- Works in all email clients
- Accessible for screen readers

### 3. **Rate Limiting**
- Prevents spam (max 5 per hour per type)
- Configurable threshold

### 4. **Retry Mechanism**
- 3 attempts with exponential backoff
- Automatic retry on temporary failures

### 5. **Circuit Breaker**
- Opens after 3 consecutive failures
- 15-minute cool down period
- Prevents wasting resources

### 6. **Delivery Logging**
- All attempts logged to `email_delivery.log`
- Includes success/failure, timestamps, recipients

---

## 🧪 Testing

### Test All Notifications:
```powershell
C:/Users/SaadSayyed/Desktop/test2/nitin_sir5/myenv1/Scripts/python.exe test_all_notifications.py
```

### Test Single Email:
```powershell
C:/Users/SaadSayyed/Desktop/test2/nitin_sir5/myenv1/Scripts/python.exe test_email_now.py
```

### Test Daily Summary Now:
```python
from daily_summary_scheduler import daily_summary_scheduler
daily_summary_scheduler.send_now()
```

---

## 📊 Monitoring

### Check Email Delivery Log:
```powershell
Get-Content email_delivery.log -Tail 50
```

### View Recent Deliveries:
```python
# In your Flask app or Python shell
import json
with open('email_delivery.log', 'r') as f:
    for line in f.readlines()[-10:]:  # Last 10 entries
        print(json.loads(line.strip()))
```

---

## 🔍 Troubleshooting

### Emails Not Sending?

1. **Check Configuration**:
   ```python
   from utils.email_service import email_service
   print(f"Host: {email_service.email_host}")
   print(f"User: {email_service.email_user}")
   print(f"Admin Emails: {email_service.admin_emails}")
   ```

2. **Check Circuit Breaker**:
   ```python
   print(f"Circuit open until: {email_service._circuit_open_until}")
   print(f"Consecutive failures: {email_service._consecutive_failures}")
   ```

3. **Check Delivery Log**:
   Look at `email_delivery.log` for errors

4. **Test Connection**:
   ```powershell
   python test_email_now.py
   ```

### Rate Limited?

If you see "Rate limited" in logs, wait 1 hour or adjust:
```properties
EMAIL_RATE_LIMIT_PER_HOUR=10  # Increase limit
```

### Gmail-Specific Issues?

Make sure you're using an **App Password**, not your regular Gmail password:
1. Go to Google Account Settings
2. Security → 2-Step Verification
3. App Passwords → Generate new password
4. Use that password in `.env`

---

## 📝 Customization

### Add Custom Recipients for Specific Alerts:
```python
# In your code
email_service.notify_sync_failed(
    "SERVER-01",
    "Error message",
    recipients=["custom@example.com", "admin@example.com"]
)
```

### Change Daily Summary Time:
Edit `daily_summary_scheduler.py`:
```python
# Change from 23:59 to 18:00 (6 PM)
schedule.every().day.at("18:00").do(self.send_daily_summary)
```

### Add More Notification Types:
Add new methods to `utils/email_service.py` following the existing pattern.

---

## ✨ Summary

Your application now has **production-ready automatic email notifications** that:

✅ Send automatically on sync events  
✅ Include detailed information for troubleshooting  
✅ Have beautiful HTML formatting  
✅ Prevent spam with rate limiting  
✅ Retry on failures  
✅ Log all deliveries  
✅ Send daily summaries  

**No manual intervention required!** Just keep the application running, and you'll receive emails for all important events.

---

## 📞 Support

For issues or questions:
- Check `email_delivery.log` for delivery status
- Review `.env` configuration
- Run test scripts to verify setup
- Check Flask application logs for email-related errors
