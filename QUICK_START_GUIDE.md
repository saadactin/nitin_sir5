# Quick Start Guide - Unified Migration System

## 🚀 Quick Setup (5 Minutes)

### Step 1: Configure Environment

Add to your `.env` file:

```env
# ClickHouse Configuration
CLICKHOUSE_HOST=your_clickhouse_host
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your_password
CLICKHOUSE_DATABASE=JARVIS_DB

# HANA (if using)
HANA_HOST=your_hana_host
HANA_PORT=30015
HANA_USERNAME=your_username
HANA_PASSWORD=your_password
```

### Step 2: Add Data Sources

#### Add HANA Source:
1. Go to **"Add Source"** → **"Add SAP HANA Source"**
2. Fill in connection details
3. Select ClickHouse as target
4. Click **"Add SAP HANA Source"**

#### Add API Source:
1. Go to **"Add Source"** → **"Add API Source"**
2. Enter API URL
3. Configure authentication (if needed)
4. Set target database/table
5. Click **"Add API Source"**

### Step 3: Run Initial Migration

```bash
# Migrate all sources to ClickHouse
python unified_migration_manager.py
```

This will:
- ✅ Migrate all HANA tables (including empty ones)
- ✅ Migrate all API data (including empty responses)
- ✅ Create table structures even if no data
- ✅ Show detailed progress and statistics

### Step 4: Validate Migration

```bash
# Verify all migrations completed successfully
python migration_validator.py
```

## 📋 Common Tasks

### Migrate Specific Source

```bash
python unified_migration_manager.py --source-id 1
```

### Migrate Only HANA Sources

```bash
python unified_migration_manager.py --source-types sap_hana
```

### Migrate Only API Sources

```bash
python unified_migration_manager.py --source-types api
```

### Validate Specific Source

```bash
python migration_validator.py --source-id 1
```

## 🔄 Scheduled Syncs

### Set Up Schedule:

1. Go to **"Create Schedule"**
2. Select source type (HANA or API)
3. Choose your source
4. Set schedule:
   - **Interval**: Every X minutes
   - **Daily**: At specific time
5. Click **"Create Schedule"**

### View Schedules:

- Go to **"View Schedules"**
- See all active schedules
- Check last run time and status

## ✅ Verification Checklist

After migration, verify:

- [ ] All sources migrated successfully
- [ ] Empty tables have structure in ClickHouse
- [ ] Row counts match (use validator)
- [ ] Schedules are set up (if needed)
- [ ] Logs show no critical errors

## 🐛 Troubleshooting

### Migration Fails

1. Check logs: `unified_migration.log`
2. Verify source connectivity
3. Check ClickHouse connection
4. Re-run for specific source: `python unified_migration_manager.py --source-id X`

### Empty Tables Not Created

1. Check if source has schema information
2. Verify ClickHouse connection
3. Review logs for specific errors
4. Run validator to identify issues

### Data Mismatches

1. Run validator: `python migration_validator.py`
2. Check incremental sync configuration
3. Verify timestamp/ID columns exist
4. Review sync metadata

## 📊 Monitoring

### Log Files

- `unified_migration.log` - All migrations
- `hana_migration_test.log` - HANA logs
- `api_sync.log` - API sync logs

### UI Monitoring

- **Sync History**: View all sync operations
- **View Schedules**: Check scheduled syncs
- **Sync Summary**: Compare source vs target

## 🎯 Key Features

✅ **All Sources Supported**: HANA, DevOps API, Zoho API, REST APIs  
✅ **Empty Tables**: Structure created even if no data  
✅ **Error Handling**: Comprehensive error handling and recovery  
✅ **Validation**: Automatic validation of migrations  
✅ **Scheduling**: Automated incremental syncs  
✅ **Monitoring**: Full logging and statistics  

## 📞 Support

For issues:
1. Check logs first
2. Run validator
3. Review error messages
4. Check source connectivity

---

**Your unified migration system is ready!** 🎉

