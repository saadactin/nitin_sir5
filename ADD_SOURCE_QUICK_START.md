# Quick Start: Add Source Feature

## 🚀 Quick Overview
New feature to add multiple source systems (SQL Server, SAP HANA) with flexible target destinations (PostgreSQL, ClickHouse).

## 📍 Access the Feature
1. Log in as **admin** or **operator**
2. Click **"Add Source"** in the sidebar (with add_circle icon)
3. Choose your source type

## 🔵 SQL Server Source - 3 Steps

### Step 1: Basic Info
```
Source Name:    Production SQL Server
Server Address: localhost  (or SERVER\INSTANCE for named instances)
Username:       sa
Password:       ********
```

### Step 2: Test Connection (Recommended)
Click **"Test Connection"** button to verify credentials before saving.

### Step 3: Select Target
```
Target System:   PostgreSQL  (or ClickHouse)
Target Database: [Select from dropdown - auto-loads after choosing target]
```

Click **"Add SQL Server Source"** ✅

## 🟢 SAP HANA Source - 3 Steps

### Step 1: HANA Connection
```
Source Name:    SAP Production HANA
Host Address:   hanaserver.company.com
Port:           30015  (default, or 3NN15 where NN = instance)
Instance:       00  (optional)
Username:       HANAUSER
Password:       ********
```

### Step 2: Select Target
```
Target System:   PostgreSQL  (or ClickHouse)
Target Database: [Select from dropdown]
```

### Step 3: Add Source
Click **"Add SAP HANA Source"** ✅

**Note**: HANA connection tested during first sync (requires `hdbcli` library)

## 🎯 Target System Details

### PostgreSQL Target:
- Lists all databases from your PostgreSQL instance
- Excludes template databases
- Recommended for relational data and OLTP workloads

### ClickHouse Target:
- Lists all databases from your ClickHouse instance
- Recommended for analytical workloads and large-scale data
- High-performance columnar storage

## 📱 Mobile Friendly
- Fully responsive on all devices
- Touch-friendly buttons (44px minimum)
- Optimized for phones, tablets, and desktops

## ⚠️ Important Notes

### SQL Server Named Instances:
Format: `SERVER\INSTANCENAME`
Example: `MYSERVER\SQLEXPRESS`

Requirements:
- SQL Browser service must be running
- UDP port 1434 accessible
- Correct instance name spelling

### SAP HANA Port Format:
Default: `30015` (for instance 00)
Pattern: `3NN15` where NN is instance number
- Instance 00 → Port 30015
- Instance 01 → Port 30115
- Instance 02 → Port 30215

### Security:
- ⚠️ Passwords currently stored in plain text
- 🔒 Only admin and operator roles can access
- ✅ Connection testing validates credentials

## 🔍 Troubleshooting

### "Connection failed" for SQL Server?
1. ✅ Verify server address and instance name
2. ✅ Check credentials are correct
3. ✅ Ensure SQL Browser running (for named instances)
4. ✅ Check firewall allows connection

### Target databases not loading?
1. ✅ Ensure PostgreSQL/ClickHouse services running
2. ✅ Check environment variables configured
3. ✅ Look for errors in browser console (F12)

### "Source already exists"?
1. ✅ Use a different source name
2. ✅ Source names must be unique

## 📊 What Happens After Adding?

1. **Source Saved**: Configuration stored in `data_sources` table
2. **Ready for Sync**: Source available for synchronization (requires sync worker integration)
3. **Manageable**: Can be edited, disabled, or deleted later

## 🔄 Current Limitations

- ❌ No UI to view/manage existing sources (coming soon)
- ❌ Sync workers not yet integrated with new sources
- ❌ HANA sync logic not implemented (requires `hdbcli`)
- ❌ No automated testing of HANA connections

## 📚 Next Steps

After adding sources:
1. Configure sync schedules (when sync integration complete)
2. Monitor sync status on dashboard
3. Set up alerts for sync failures
4. Review sync history and analytics

## 📞 Need Help?

Check detailed documentation:
- `ADD_SOURCE_FEATURE.md` - Complete feature documentation
- `SQL_SERVER_CONNECTIONS.md` - SQL Server connection help
- `MOBILE_RESPONSIVE_GUIDE.md` - UI/UX information

---

**Quick Tip**: Always test SQL Server connections before saving to catch configuration errors early! 🎯
