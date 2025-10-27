# How to Get Data in Advanced Analytics Dashboard

## ❌ Why You're Seeing "No Data Available"

The Advanced Analytics Dashboard shows **real sync operation data** from your database. Since you haven't run any syncs yet, the dashboard is empty.

---

## ✅ Quick Solution: Run Your First Sync

### **Method 1: Manual Sync (Fastest)**

1. **Go to the Homepage**
   - Click "Dashboard" in the sidebar
   - You should see your configured servers:
     - `server1` (localhost\SQL2019_Second)
     - `server3` (localhost)

2. **Start a Sync**
   - Click the **"Sync Now"** button next to any server
   - Wait for the sync to complete

3. **Check the Dashboard**
   - Go to "Advanced Analytics" in sidebar
   - You should now see:
     - ✓ Total syncs: 1
     - ✓ Success rate
     - ✓ Server in the status table
     - ✓ Activity in recent feed

---

### **Method 2: Create a Schedule (Automated)**

1. **Go to "Create Schedule"** in sidebar

2. **Fill in the form:**
   - Server: Choose `server1` or `server3`
   - Schedule Type: `Interval`
   - Minutes: `30` (runs every 30 minutes)
   - Click "Create Schedule"

3. **Wait for first scheduled run**
   - Or click "Sync Now" to trigger immediately

4. **Check "View Schedules"**
   - You'll see your scheduled job
   - Last run time will update after execution

---

## 🎯 Step-by-Step: Your First Sync

### **Step 1: Verify Server Connection**

Make sure your SQL Servers are running:
- SQL Server 2019 should be running
- Instances `SQL2019_Second` and default should be accessible

### **Step 2: Login to Your App**

```
http://127.0.0.1:5001
```

Login with:
- Username: `admin`
- Password: `admin123` (or your configured password)

### **Step 3: Navigate to Homepage**

Click "Dashboard" or "Home" in the sidebar

### **Step 4: Trigger First Sync**

You'll see your servers listed with "Sync Now" buttons:

```
┌────────────────────────────────────────┐
│ server1 (localhost\SQL2019_Second)    │
│ [🔄 Sync Now] [⚙️ Configure]          │
└────────────────────────────────────────┘

┌────────────────────────────────────────┐
│ server3 (localhost)                    │
│ [🔄 Sync Now] [⚙️ Configure]          │
└────────────────────────────────────────┘
```

Click **"Sync Now"** on any server!

### **Step 5: Monitor Progress**

- You'll see a progress indicator
- Check "Sync History" page for detailed logs
- Wait for completion message

### **Step 6: View Analytics**

1. Go to **"Advanced Analytics"** in sidebar
2. You should now see:
   - **Total Syncs Today**: 1 (or more)
   - **Success Rate**: 100% (if successful)
   - **Server Status Table**: Your server listed
   - **Recent Activity**: Your sync operation
   - **Charts**: Performance data

---

## 🔍 Troubleshooting: Still No Data?

### **Check 1: Is the database created?**

```powershell
python create_database.py
```

This creates the `test1` database if it doesn't exist.

### **Check 2: Are SQL Servers running?**

Open **SQL Server Configuration Manager** and verify:
- SQL Server (MSSQLSERVER) - Running
- SQL Server (SQL2019_SECOND) - Running

### **Check 3: Can you connect to SQL Servers?**

Test connection from the app:
- Go to homepage
- Look for connection status indicators
- Red = Offline, Green = Online

### **Check 4: Check sync history**

Visit the **"Sync History"** page:
- Shows all sync attempts
- Status: completed/failed
- Error messages if any

### **Check 5: Check the database**

Connect to PostgreSQL and run:

```sql
-- Check if any syncs are recorded
SELECT * FROM metrics_sync_tables.sync_history 
ORDER BY sync_date DESC 
LIMIT 10;

-- If no rows, that's why dashboard is empty!
```

---

## 🎨 What Data Populates What

Here's what each dashboard section needs:

### **Metric Cards:**
- **Total Syncs Today** ← Syncs with `sync_date` = today
- **Success Rate** ← Ratio of `status='completed'` vs total
- **Active Syncs** ← Syncs with `status='running'`
- **Avg Sync Time** ← Average `(end_time - sync_date)` for completed syncs

### **Charts:**
- **Sync Performance** ← Hourly average duration (last 24 hours)
- **Success vs Failure** ← Count of completed vs failed today
- **Top Servers** ← Sync count per server (last 7 days)
- **Duration Distribution** ← Syncs grouped by duration buckets

### **Server Status Table:**
- Latest sync per server from `sync_history`
- 7-day success rate calculated per server

### **Recent Activity:**
- Last 20 records from `sync_history` ordered by `sync_date DESC`

---

## 🚀 Quick Test Commands

### **Option 1: Run a test sync via terminal**

```powershell
python -c "from hybrid_sync import process_sql_server_hybrid; from manage_server import load_config; config = load_config(); process_sql_server_hybrid('server1', config['sqlservers']['server1'])"
```

### **Option 2: Use the web UI**

Much easier! Just click "Sync Now" in the browser.

---

## 📊 Expected Dashboard After First Sync

After running 1 successful sync, you should see:

```
┌─────────────────────────────────────────────────────────┐
│ 📈 Advanced Analytics Dashboard                         │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  [Sync] Today: 1                                        │
│  [✓] Success Rate: 100%                                 │
│  [⏳] Active: 0                                          │
│  [⚡] Avg Time: 1m 23s                                   │
│                                                          │
│  Server Status Overview                                 │
│  ┌─────────────────────────────────────────────┐       │
│  │ server1  | ● online | 2 mins ago | 1m 23s   │       │
│  └─────────────────────────────────────────────┘       │
│                                                          │
│  Recent Activity                                        │
│  • ✓ Successfully synced server1 (2 minutes ago)       │
└─────────────────────────────────────────────────────────┘
```

---

## 💡 Pro Tips

1. **Run syncs regularly** to build up trend data
2. **Create schedules** for automated syncing
3. **Check "Sync History"** page for detailed logs
4. **Use "View Schedules"** to see automated jobs
5. **Charts improve** with more data points over time

---

## ✅ Summary

**Your dashboard is working correctly!** It's just empty because:
- ✓ No syncs have been run yet
- ✓ Database is empty (no data to show)

**To populate the dashboard:**
1. Go to homepage (Dashboard/Home)
2. Click "Sync Now" on any server
3. Wait for sync to complete
4. Go to "Advanced Analytics"
5. See your data! 🎉

---

**The dashboard will NEVER show fake data** - only real sync operations from your database. This is intentional and correct behavior!

Run your first sync to see it come alive! 🚀
