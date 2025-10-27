# Instant Email Alert System

## Overview
The system now sends **immediate email notifications** whenever ANY error or issue occurs during database synchronization. You will receive emails at:
- saad.sayyed@actin.co.in
- saadpractice4@gmail.com

## Alert Scenarios

### 1. **SQL Server Connection Failures** ⚠️
**When:** Cannot connect to SQL Server
**Email Sent:** Immediately when connection fails
**Scenarios:**
- SQL Browser service not running
- Instance not found
- Login timeout (server down/unreachable)
- Network interface errors
- Authentication failures
- Any other connection issues

**Email Content:**
```
Subject: [ALERT] Server Down - server_name
Details: SQL Server connection failed: [error details]
Additional context about the specific error
```

---

### 2. **PostgreSQL Connection Failures** ⚠️
**When:** Cannot connect to PostgreSQL database
**Email Sent:** Immediately when PostgreSQL connection fails
**Scenarios:**
- PostgreSQL server down
- Database doesn't exist
- Authentication failures
- Network issues

**Email Content:**
```
Subject: [ALERT] Server Down - PostgreSQL
Details: Failed to connect to PostgreSQL database
```

---

### 3. **Table Sync Failures** ⚠️
**When:** Individual table fails to sync (either full or incremental)
**Email Sent:** Immediately when table sync encounters error
**Scenarios:**
- Data type conversion errors
- Schema mismatch
- Permission issues
- Constraint violations
- Timeout errors

**Email Content:**
```
Subject: [SYNC][FAILED] server_name/database_name
Details: Table sync failed: schema.table_name
[Detailed error message]
```

---

### 4. **Server-Level Sync Failures** ⚠️
**When:** Entire server sync process fails critically
**Email Sent:** Immediately when server-level error occurs
**Scenarios:**
- Multiple database failures
- Critical system errors
- Unexpected exceptions
- Process crashes

**Email Content:**
```
Subject: [SYNC][FAILED] server_name
Details: Server sync process failed critically
[Detailed error message]
```

---

### 5. **SQL Engine Creation Failures** ⚠️
**When:** Cannot create SQLAlchemy engine for SQL Server
**Email Sent:** Immediately when engine creation fails
**Scenarios:**
- Driver issues
- Connection string problems
- Authentication configuration errors

**Email Content:**
```
Subject: [ALERT] Server Down - server_name
Details: Failed to create SQL Server engine
[Detailed error message]
```

---

## Email Delivery Features

✅ **Immediate Delivery** - Emails sent the moment an error occurs, not at the end of sync
✅ **Retry Mechanism** - 3 automatic retry attempts if email fails to send
✅ **Rate Limiting** - Maximum 5 emails per hour per alert type (prevents spam)
✅ **Circuit Breaker** - Stops sending if 3 consecutive failures; resumes after 15 minutes
✅ **HTML & Plain Text** - Professional HTML format with plain text fallback
✅ **Detailed Logs** - All email attempts logged to `email_delivery.log`

---

## Implementation Details

### Modified Files
1. **`hybrid_sync.py`**
   - Added `send_error_email()` helper function at top of file
   - Integrated email notifications in 5 critical error points:
     * SQL Server connection failures (line ~250)
     * SQLAlchemy engine creation (line ~368)
     * PostgreSQL engine creation (line ~384)
     * Table sync errors - incremental (line ~1570)
     * Table sync errors - full (line ~1515)
     * Server-level failures (line ~1720)

2. **`sync_manager.py`**
   - Already has email notifications for completed syncs
   - Uses lazy loading pattern for proper .env initialization

3. **`utils/email_service.py`**
   - Contains all email notification logic
   - Handles SMTP connection, retry, rate limiting
   - Professional HTML email templates

---

## Testing

### Test Connection Error
To test connection error emails:
1. Stop SQL Server service
2. Trigger a sync from the web interface
3. You'll receive email immediately: "Server Down - [server_name]"

### Test Table Error
To test table sync error emails:
1. Manually corrupt a table (add incompatible data type)
2. Run sync
3. You'll receive email: "Table sync failed: [table_name]"

### Test PostgreSQL Error
To test PostgreSQL error emails:
1. Stop PostgreSQL service
2. Trigger a sync
3. You'll receive email: "Server Down - PostgreSQL"

---

## Configuration

### Email Settings (in `.env` file)
```bash
# SMTP Configuration
EMAIL_HOST=smtp.gmail.com
EMAIL_PORT=587
EMAIL_USE_TLS=True
EMAIL_HOST_USER=saadpractice4@gmail.com
EMAIL_HOST_PASSWORD=txludusmznqxoweo
ADMIN_EMAILS=saad.sayyed@actin.co.in,saadpractice4@gmail.com

# Rate Limiting
EMAIL_RATE_LIMIT_PER_HOUR=5

# Circuit Breaker
EMAIL_CIRCUIT_FAIL_THRESHOLD=3
EMAIL_CIRCUIT_COOLDOWN_MINUTES=15
```

---

## Error Email Flow

```
┌─────────────────────────────────────┐
│  Sync Process Running               │
│  ├── Connecting to SQL Server...    │
│  ├── Reading databases...           │
│  ├── Syncing tables...              │
│  └── Writing to PostgreSQL...       │
└─────────────────────────────────────┘
              │
              │ ❌ ERROR OCCURS!
              ▼
┌─────────────────────────────────────┐
│  send_error_email() triggered       │
│  ├── Identify error type            │
│  ├── Build error message            │
│  └── Send email immediately         │
└─────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────┐
│  Email Service                      │
│  ├── Load recipients from .env      │
│  ├── Create HTML & text templates   │
│  ├── Connect to Gmail SMTP          │
│  ├── Retry up to 3 times if fail   │
│  └── Log to email_delivery.log      │
└─────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────┐
│  📧 Email Delivered                 │
│  To: saad.sayyed@actin.co.in       │
│      saadpractice4@gmail.com       │
└─────────────────────────────────────┘
```

---

## Benefits

🚨 **Instant Awareness** - Know immediately when something goes wrong
🔍 **Detailed Context** - Each email includes error details and location
⏱️ **Time Savings** - No need to check logs manually
📱 **Mobile Ready** - Receive alerts on your phone/email anywhere
🛡️ **Proactive** - Fix issues before they compound
📊 **Audit Trail** - All email attempts logged for troubleshooting

---

## Important Notes

⚠️ **Rate Limiting**: Maximum 5 emails per hour per error type to prevent spam
⚠️ **Non-Blocking**: Email failures won't stop the sync process
⚠️ **Duplicate Prevention**: Same error won't generate multiple emails within 1 hour
⚠️ **Circuit Breaker**: If email service fails 3 times, it pauses for 15 minutes

---

## Monitoring Email Delivery

Check the log file to verify emails are being sent:
```powershell
Get-Content email_delivery.log -Tail 20
```

Expected output when email sent successfully:
```
2025-10-24 12:34:56 - [SUCCESS] sync_failed to 2 recipients - Subject: [SYNC][FAILED] server1
```

---

## Next Steps

1. ✅ **System is live** - Emails will be sent automatically on errors
2. ✅ **No configuration needed** - Already integrated into sync process
3. ⚠️ **Test recommended** - Manually stop a server and trigger sync to verify

---

## Support

If you're not receiving emails:
1. Check `email_delivery.log` for errors
2. Verify `.env` file has correct ADMIN_EMAILS
3. Check Gmail account allows "Less secure app access" or use App Password
4. Check spam/junk folder
5. Verify SMTP credentials are correct

**Gmail App Password Setup:**
1. Go to Google Account → Security
2. Enable 2-Step Verification
3. Generate App Password for "Mail"
4. Use that password in `EMAIL_HOST_PASSWORD`
