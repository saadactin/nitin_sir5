# Email Notifications Enhancement

## Overview
This document describes the new email notification features added to the SQL Server Migration and Sync application.

## New Notification Types

### 1. User Creation Notifications

**Trigger:** Whenever a new user is created (admin, operator, or viewer role)

**Email Subject:** `[USER CREATED] New {ROLE} user: {username}`

**Information Included:**
- Username
- Role (ADMIN/OPERATOR/VIEWER)
- Created by (which admin created the user)
- Timestamp

**Example Use Case:**
When an admin creates a new operator account "john_doe", all configured admin emails receive a notification with full details.

**Integration Point:** `auth.py` - `create_user()` function

---

### 2. Schedule Success Notifications

**Trigger:** When a scheduled sync job completes successfully

**Email Subject:** `[SCHEDULE][SUCCESS] {server_name} - {job_type}`

**Information Included:**
- Server name
- Job type (e.g., interval_30m, daily_03:00)
- Duration (e.g., "2m 15s")
- Completion timestamp

**Example Use Case:**
A scheduled sync for "SQL2019-Primary" running every 30 minutes completes successfully. Admins receive a notification confirming the sync completed in 2 minutes 15 seconds.

**Integration Point:** `scheduler_utils.py` - `_job_wrapper()` function

---

### 3. Schedule Failure Notifications

**Trigger:** When a scheduled sync job encounters an error

**Email Subject:** `[SCHEDULE][FAILED] {server_name} - {job_type}`

**Information Included:**
- Server name
- Job type
- Detailed error message
- Failure timestamp

**Example Use Case:**
A scheduled sync for "SQL2019-Backup" fails due to connection timeout. Admins immediately receive an email with the full error details.

**Integration Point:** `scheduler_utils.py` - `_job_wrapper()` function

---

## Email Templates

All notifications include both HTML and plain text versions:

### HTML Email Features:
- Professional styling with color-coded sections
- Success notifications: Green borders and highlights
- Failure notifications: Red borders and alerts
- User creation: Blue informational styling
- Tables for structured information
- Responsive design

### Plain Text Email Features:
- Clean, readable format
- All essential information included
- Works with any email client

---

## Configuration

Email notifications use the existing email configuration from `.env`:

```ini
# Email Configuration (required)
EMAIL_HOST=smtp.gmail.com
EMAIL_PORT=587
EMAIL_USE_TLS=True
EMAIL_HOST_USER=your-email@gmail.com
EMAIL_HOST_PASSWORD=your-app-password
DEFAULT_FROM_EMAIL=your-email@gmail.com
ADMIN_EMAILS=admin1@example.com,admin2@example.com

# Rate Limiting (optional)
EMAIL_RATE_LIMIT_PER_HOUR=5
EMAIL_CIRCUIT_FAIL_THRESHOLD=3
EMAIL_CIRCUIT_COOLDOWN_MINUTES=15
```

---

## Rate Limiting & Circuit Breaker

All new notifications respect the existing rate limiting system:

- **Rate Limit:** Maximum 5 emails per hour per notification type per server
- **Circuit Breaker:** Opens after 3 consecutive failures, prevents spam for 15 minutes
- **Smart Tracking:** Different notification types have separate rate limits

---

## Testing

### Test User Creation Notification:
1. Log in as admin
2. Navigate to `/create-user`
3. Create a new user
4. Check your email for the notification

### Test Schedule Notifications:
1. Create a schedule for a server (e.g., every 30 minutes)
2. Wait for the schedule to run
3. Check email for success notification
4. To test failure: temporarily misconfigure a server connection
5. Wait for schedule to run and fail
6. Check email for failure notification

### Automated Testing:
Run the test script:
```powershell
python test_new_notifications.py
```

This script will:
- Verify email service configuration
- Send test emails for all notification types
- Confirm all methods are properly integrated

---

## Integration Details

### Files Modified:

1. **utils/email_service.py**
   - Added `notify_user_created()` method
   - Added `notify_schedule_success()` method
   - Added `notify_schedule_failed()` method
   - Added HTML/text templates for all notification types

2. **auth.py**
   - Modified `create_user()` to accept `created_by` parameter
   - Added email notification call after successful user creation
   - Graceful error handling (user creation succeeds even if email fails)

3. **app.py**
   - Modified `create_user_route()` to pass current user as `created_by`
   - Updated success flash message to mention email notification

4. **scheduler_utils.py**
   - Modified `_job_wrapper()` to send notifications on success/failure
   - Added duration tracking for success notifications
   - Improved console logging with duration information

---

## Features

### ✅ Seamless Integration
- No disruption to existing functionality
- Email failures don't break core operations
- Works with existing email configuration

### ✅ Comprehensive Information
- All notifications include relevant context
- Error messages are detailed and actionable
- Timestamps for audit trails

### ✅ Professional Templates
- HTML emails with proper styling
- Fallback plain text versions
- Mobile-friendly design

### ✅ Smart Delivery
- Rate limiting prevents spam
- Circuit breaker handles email service outages
- Delivery logging for troubleshooting

---

## Troubleshooting

### Not Receiving Emails?

1. **Check Email Configuration**
   ```powershell
   python -c "from utils.email_service import email_service; print(f'User: {email_service.email_user}'); print(f'Admins: {email_service.admin_emails}')"
   ```

2. **Check Email Delivery Log**
   - File: `email_delivery.log`
   - Contains delivery status for all emails

3. **Check Rate Limiting**
   - Maximum 5 emails per hour per notification type
   - Wait if limit reached

4. **Check Circuit Breaker**
   - Opens after 3 consecutive failures
   - Cooldown period: 15 minutes

5. **Test Email Service**
   ```powershell
   python test_new_notifications.py
   ```

### Common Issues:

**Issue:** "No recipients configured"
- **Solution:** Set `ADMIN_EMAILS` in `.env` file

**Issue:** "Circuit open until [timestamp]"
- **Solution:** Wait for cooldown period or fix underlying email configuration

**Issue:** "Rate limited"
- **Solution:** Normal behavior, emails will resume after rate limit window

---

## Security Considerations

1. **Password Safety:** User passwords are never included in emails
2. **Limited Details:** Only necessary information is shared
3. **Admin Only:** Notifications go to configured admin emails only
4. **Audit Trail:** All email deliveries are logged

---

## Future Enhancements

Potential additions (not yet implemented):
- Configurable notification preferences per admin
- Slack/Teams integration for notifications
- SMS notifications for critical failures
- Weekly summary reports
- Custom notification templates

---

## Support

For issues or questions:
1. Review this documentation
2. Check `email_delivery.log` for delivery details
3. Run `test_new_notifications.py` to diagnose issues
4. Verify `.env` configuration
5. Check application logs for email-related errors

---

Last Updated: October 24, 2025
Version: 1.0.0
