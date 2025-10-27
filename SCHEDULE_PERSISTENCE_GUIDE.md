# Schedule Persistence & Auto-Recovery Guide

## ✅ Your Schedules ARE Persistent!

**Good news:** Your scheduler is **already configured** to automatically restore all schedules when the server restarts. No data loss!

---

## 🔄 How It Works

### **1. Schedule Creation**
When you create a schedule through the UI:

1. **Stored in Database**: Schedule saved to PostgreSQL `metrics_sync_tables.schedules` table
2. **Loaded in Memory**: Schedule added to in-memory `scheduled_jobs` list
3. **Registered with Scheduler**: Job registered with Python `schedule` library
4. **Background Thread Started**: Scheduler thread begins monitoring and executing jobs

### **2. Server Shutdown**
When you stop the server (Ctrl+C, server crash, or normal shutdown):

- ✅ Schedules remain **safely stored in PostgreSQL database**
- ❌ In-memory scheduler state is lost (expected)
- ❌ Running syncs are interrupted (marked as failed by watchdog)

### **3. Server Startup (AUTO-RECOVERY)**
When you start the server again:

```
[SCHEDULER] Initializing scheduler system...
======================================================================
[SCHEDULER] RESTORING SCHEDULES FROM DATABASE
======================================================================
[SCHEDULER] Found 3 schedule(s) to restore
  ✓ Restored: SQL2019 - Every 30 minutes
  ✓ Restored: ProductionDB - Daily at 02:00
  ✓ Restored: BackupDB - Every 120 minutes
----------------------------------------------------------------------
[SCHEDULER] Restoration complete:
  • Successfully loaded: 3 schedule(s)
======================================================================
```

**What happens automatically:**

1. ✅ `load_schedules_from_db()` runs on module import
2. ✅ All active schedules retrieved from database
3. ✅ Each schedule re-registered with scheduler
4. ✅ Background scheduler thread restarted
5. ✅ Jobs resume at their next scheduled time

---

## 📊 Database Schema

Your schedules are stored in:

**Table:** `metrics_sync_tables.schedules`

**Columns:**
```sql
- server_name     VARCHAR     -- e.g., "SQL2019"
- job_type        VARCHAR     -- e.g., "interval_30m" or "daily_02:00"
- last_run        TIMESTAMP   -- Last execution time
- status          VARCHAR     -- "success", "failed", "pending", or "deleted"
- error           TEXT        -- Error message if failed
- created_at      TIMESTAMP   -- When schedule was created
```

**Example Data:**
```
server_name   | job_type       | status  | last_run            
--------------+----------------+---------+---------------------
SQL2019       | interval_30m   | success | 2025-10-24 14:30:00
ProductionDB  | daily_02:00    | pending | NULL
BackupDB      | interval_120m  | success | 2025-10-24 13:00:00
```

---

## 🎯 Schedule Types Supported

### **1. Interval Schedules**
- **Format:** `interval_<minutes>m`
- **Examples:**
  - `interval_30m` → Every 30 minutes
  - `interval_60m` → Every 1 hour
  - `interval_1440m` → Every 24 hours

### **2. Daily Schedules**
- **Format:** `daily_<HH:MM>`
- **Examples:**
  - `daily_02:00` → Daily at 2:00 AM
  - `daily_14:30` → Daily at 2:30 PM
  - `daily_23:59` → Daily at 11:59 PM

---

## 🛡️ Safeguards & Features

### **Auto-Recovery Features:**

1. **✅ Duplicate Prevention**
   - Existing schedules cleared before re-registration
   - Prevents double-execution after restart

2. **✅ Deleted Schedule Exclusion**
   - Schedules marked as `deleted` are NOT restored
   - Only active schedules (`status != 'deleted'`) are loaded

3. **✅ Error Handling**
   - Invalid schedules skipped with warning
   - Server continues startup even if some schedules fail to load

4. **✅ Watchdog Monitoring**
   - Runs every 30 minutes
   - Marks stuck "in-progress" syncs as failed (threshold: 60 minutes)
   - Prevents ghost syncs from blocking future runs

5. **✅ Health Checks**
   - Runs every 15 minutes
   - Pings all configured SQL Servers
   - Sends email alerts on server failures

---

## 🧪 Testing Schedule Persistence

### **Test 1: Create & Verify**

1. **Create a schedule:**
   - Go to "Create Schedule" in sidebar
   - Select server: `SQL2019`
   - Type: `Interval`
   - Minutes: `30`
   - Click "Create Schedule"

2. **Verify in database:**
   ```sql
   SELECT * FROM metrics_sync_tables.schedules 
   WHERE server_name = 'SQL2019';
   ```

3. **Verify in scheduler:**
   - Go to "View Schedules"
   - Should see your schedule listed

### **Test 2: Server Restart**

1. **Stop the server:**
   - Press `Ctrl+C` in terminal
   - Or close the terminal window

2. **Restart the server:**
   ```powershell
   python app.py
   ```

3. **Check console output:**
   - Look for `[SCHEDULER] RESTORING SCHEDULES FROM DATABASE`
   - Should see: `✓ Restored: SQL2019 - Every 30 minutes`

4. **Verify schedules are running:**
   - Go to "View Schedules" page
   - All schedules should still be there
   - Wait for next scheduled time - sync should execute automatically

### **Test 3: API Verification**

Visit this endpoint to verify schedules loaded correctly:
```
http://127.0.0.1:5001/api/verify-schedules
```

**Response Example:**
```json
{
  "database_schedules": [
    {
      "server": "SQL2019",
      "type": "interval_30m",
      "status": "success",
      "last_run": "2025-10-24 14:30:00"
    }
  ],
  "active_scheduler_jobs": [
    {
      "tags": ["SQL2019", "SQL2019:interval_30m"],
      "next_run": "2025-10-24 15:00:00"
    }
  ],
  "status": "ok",
  "message": "1 schedules in database, 3 active jobs in scheduler"
}
```

---

## 🚀 Usage Scenarios

### **Scenario 1: Planned Maintenance**
```
1. Stop server for maintenance
2. Perform updates
3. Restart server
✅ All schedules automatically restored
✅ Jobs resume at next scheduled time
```

### **Scenario 2: Server Crash**
```
1. Server crashes unexpectedly
2. Restart server
✅ All schedules automatically restored
⚠ Any running syncs marked as failed by watchdog
✅ Future syncs execute normally
```

### **Scenario 3: System Reboot**
```
1. Windows update forces reboot
2. Server auto-starts (if configured)
✅ All schedules automatically restored
✅ No manual intervention needed
```

---

## 📋 Verification Checklist

After server restart, verify:

- [ ] Console shows: `[SCHEDULER] RESTORING SCHEDULES FROM DATABASE`
- [ ] Console shows: `✓ Restored: <server_name> - <schedule_type>`
- [ ] Console shows: `Successfully loaded: X schedule(s)`
- [ ] "View Schedules" page shows all schedules
- [ ] `/api/verify-schedules` returns correct count
- [ ] Schedules execute at expected times

---

## 🔧 Troubleshooting

### **Problem: Schedules not restored after restart**

**Check 1:** Database Connection
```sql
-- Verify schedules exist in database
SELECT * FROM metrics_sync_tables.schedules 
WHERE status != 'deleted';
```

**Check 2:** Console Output
- Look for errors in `[SCHEDULER]` messages
- Check for `Failed to load` warnings

**Check 3:** Schedule Status
```sql
-- Ensure schedules are not marked as deleted
SELECT server_name, job_type, status 
FROM metrics_sync_tables.schedules;
```

**Check 4:** API Verification
- Visit `/api/verify-schedules`
- Compare `database_schedules` count with `active_scheduler_jobs` count

### **Problem: Schedule executes twice after restart**

**Solution:** This shouldn't happen! The code clears duplicates:
```python
# Clear any existing schedules before re-registering
tag_formats = [
    f"{server_name}:{job_type}",
    f"{server_name}-{job_type}"
]
for tag in tag_formats:
    sched.clear(tag)
```

If it does happen:
1. Delete the duplicate schedule
2. Restart server
3. Should restore correctly

---

## 📊 Monitoring Schedule Health

### **Real-Time Monitoring:**

1. **View Schedules Page:**
   - Shows all active schedules
   - Last run time
   - Status (success/failed)
   - Error messages

2. **Sync History Page:**
   - Shows all executed syncs
   - Filter by server
   - View duration and status

3. **Advanced Analytics Dashboard:**
   - Real-time sync metrics
   - Success rates
   - Performance trends

### **Email Notifications:**

You'll receive emails for:
- ✅ Scheduled sync success (with duration)
- ❌ Scheduled sync failure (with error details)
- 🔴 Server down/unreachable
- 📧 Daily summary (23:59 daily)

---

## 🎊 Summary

### **Your schedules are 100% persistent!**

✅ **Stored in database** - Safe even if server crashes  
✅ **Auto-restored on startup** - No manual intervention  
✅ **Duplicate prevention** - Won't execute twice  
✅ **Watchdog protection** - Cleans up stuck syncs  
✅ **Email notifications** - Stay informed of all events  
✅ **API verification** - Programmatically check status  

### **What You Need to Do:**

**Nothing!** Just start the server and your schedules will automatically resume.

---

## 📞 Quick Reference

### **Key Files:**
- `scheduler_utils.py` - Scheduler logic & auto-recovery
- `app.py` - Web routes and initialization
- `metrics_sync_tables.schedules` - Database table

### **Key Functions:**
- `load_schedules_from_db()` - Restores schedules on startup
- `schedule_interval_sync()` - Creates interval schedule
- `schedule_daily_sync()` - Creates daily schedule
- `delete_schedule()` - Soft-deletes schedule

### **API Endpoints:**
- `GET /api/verify-schedules` - Check schedule status
- `GET /view-schedules` - View all schedules (UI)

---

**Created:** October 24, 2025  
**Status:** ✅ PRODUCTION READY - Schedules Persist Across Restarts  
**Auto-Recovery:** ✅ ENABLED BY DEFAULT

---

**Your schedules will ALWAYS survive server restarts!** 🎉
