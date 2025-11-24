# HANA Scheduling and Incremental Sync Guide

## Overview

This guide explains how to set up scheduled incremental syncs for SAP HANA sources, allowing automatic data synchronization at regular intervals.

## Features

✅ **Scheduled Syncs**: Set up interval-based (every X minutes) or daily (at specific time) syncs  
✅ **Incremental Sync**: Only syncs new/changed data since last sync  
✅ **Auto-Loading**: Schedules are automatically restored on server startup  
✅ **Error Handling**: Graceful failure handling with email notifications  
✅ **Connection Testing**: Verifies HANA and ClickHouse connections before sync  

## How It Works

### Incremental Sync Strategy

The HANA incremental sync uses multiple strategies to detect new/changed data:

1. **Timestamp-based** (Preferred):
   - Uses columns like `CREATED_AT`, `UPDATED_AT`, `TIMESTAMP`
   - Syncs only records with timestamp > last sync time

2. **ID-based** (Fallback):
   - Uses auto-incrementing ID columns
   - Syncs only records with ID > last sync ID

3. **Full Sync** (Last Resort):
   - If no timestamp/ID column found, performs full table sync
   - Sets up incremental sync for next run

### Scheduling Types

1. **Interval Sync**: Runs every X minutes (e.g., every 15 minutes)
2. **Daily Sync**: Runs once per day at a specific time (e.g., 2:00 AM)

## Setup Instructions

### Step 1: Add HANA Source

1. Navigate to **"Add Source"** → **"Add SAP HANA Source"**
2. Fill in connection details:
   - Host Address
   - Port (default: 30015)
   - Username
   - Password
   - HANA Database (optional)
   - Target Database (ClickHouse)
3. Click **"Add SAP HANA Source"**

### Step 2: Create Schedule

1. Navigate to **"Create Schedule"** page
2. Select **"SAP HANA"** as source type
3. Select your HANA source from the dropdown
4. Choose schedule type:
   - **Interval**: Enter minutes (e.g., 15 for every 15 minutes)
   - **Daily**: Enter hour and minute (e.g., 2:00 AM)
5. Click **"Create Schedule"**

### Step 3: Verify Schedule

1. Navigate to **"View Schedules"** page
2. Verify your HANA source schedule is listed
3. Check status and last run time

## Testing Incremental Sync

### Option 1: Test Script (Recommended)

Run the test script to verify incremental sync is working:

```bash
python test_hana_incremental_sync.py
```

The script will:
- ✅ Check if HANA sources are scheduled
- ✅ Test HANA and ClickHouse connections
- ✅ Perform incremental sync
- ✅ Show detailed results

### Option 2: Manual Test from UI

1. Navigate to your HANA source
2. Click **"Incremental Sync"** button
3. Monitor the sync progress
4. Check sync history for results

### Option 3: Check Logs

Monitor the application logs for sync activity:

```bash
# Check sync logs
tail -f app.log | grep -i hana

# Check scheduler logs
tail -f app.log | grep -i scheduler
```

## Schedule Management

### View Active Schedules

Navigate to **"View Schedules"** to see:
- All active schedules (interval and daily)
- Last run time
- Status (success/failed)
- Error messages (if any)

### Edit Schedule

1. Go to **"View Schedules"**
2. Click **"Edit"** on the schedule you want to modify
3. Update interval/time settings
4. Save changes

### Delete Schedule

1. Go to **"View Schedules"**
2. Click **"Delete"** on the schedule
3. Confirm deletion

## How Incremental Sync Works

### First Run

1. System discovers all tables in HANA schemas
2. For each table:
   - Creates table in ClickHouse (if not exists)
   - Performs full data migration
   - Sets up incremental sync metadata

### Subsequent Runs

1. System checks sync metadata for each table
2. For tables with timestamp columns:
   - Queries HANA for records with timestamp > last sync time
   - Inserts only new/changed records
   - Updates sync metadata

3. For tables with ID columns:
   - Queries HANA for records with ID > last sync ID
   - Inserts only new records
   - Updates sync metadata

4. For tables without timestamp/ID:
   - Performs full sync
   - Attempts to set up incremental sync

## Troubleshooting

### Schedule Not Running

**Check:**
1. Is the schedule active in "View Schedules"?
2. Is the scheduler thread running? (check server logs)
3. Are there any errors in the schedule status?

**Solution:**
- Restart the Flask application
- Check database connection
- Verify schedule is not marked as "deleted"

### Incremental Sync Not Working

**Symptoms:**
- Full sync runs every time
- No new records detected
- Error messages about timestamp columns

**Check:**
1. Do tables have timestamp columns? (CREATED_AT, UPDATED_AT, TIMESTAMP)
2. Are timestamp columns properly indexed in HANA?
3. Check sync metadata table in ClickHouse

**Solution:**
- Ensure tables have timestamp or ID columns
- Manually trigger full sync to reset metadata
- Check HANA table structure

### Connection Failures

**Symptoms:**
- Schedule shows "failed" status
- Error: "Failed to connect to HANA"

**Check:**
1. Is HANA server accessible?
2. Are credentials correct?
3. Is firewall blocking connection?

**Solution:**
- Test connection manually using "Test Connection" button
- Verify HANA server is running
- Check network connectivity

### No Data Synced

**Symptoms:**
- Sync completes but no new records
- Records count is 0

**Check:**
1. Are there actually new records in HANA?
2. Is the timestamp/ID column being used correctly?
3. Check sync metadata for last sync time

**Solution:**
- Verify data exists in HANA source tables
- Check if timestamp/ID values are increasing
- Review sync metadata in ClickHouse

## Monitoring

### Sync History

View sync history to see:
- When syncs ran
- How many records were synced
- Success/failure status
- Error messages

### Email Notifications

Configure email notifications to receive:
- Success notifications after scheduled syncs
- Failure alerts when syncs fail
- Connection error notifications

### Logs

Monitor application logs for:
- Schedule execution times
- Sync progress
- Error details
- Connection status

## Best Practices

1. **Start with Interval Sync**: Use interval sync (e.g., every 15 minutes) for active testing
2. **Use Daily Sync for Production**: Switch to daily sync for production workloads
3. **Monitor First Few Runs**: Watch the first few scheduled syncs to ensure they work correctly
4. **Set Up Email Notifications**: Enable email alerts to be notified of issues
5. **Regular Verification**: Periodically run the test script to verify sync is working
6. **Backup Sync Metadata**: The sync metadata table is critical - ensure it's backed up

## Schedule Configuration Examples

### Example 1: High-Frequency Sync (Every 5 Minutes)

```
Source Type: SAP HANA
Source: Production HANA
Schedule Type: Interval
Minutes: 5
```

### Example 2: Daily Sync (2:00 AM)

```
Source Type: SAP HANA
Source: Production HANA
Schedule Type: Daily
Hour: 2
Minute: 0
```

### Example 3: Business Hours Sync (Every 30 Minutes, 9 AM - 5 PM)

For business hours sync, you would need to:
1. Create multiple daily schedules, OR
2. Use interval sync and manually enable/disable, OR
3. Customize the scheduler code

## Technical Details

### Sync Metadata Table

The system creates a `sync_metadata` table in ClickHouse:

```sql
CREATE TABLE sync_metadata (
    source_schema String,
    source_table String,
    target_table String,
    last_sync_timestamp DateTime,
    last_sync_id UInt64,
    sync_enabled UInt8
) ENGINE = MergeTree()
ORDER BY (source_schema, source_table)
```

### Schedule Storage

Schedules are stored in PostgreSQL:

```sql
SELECT * FROM metrics_sync_tables.schedules
WHERE source_id IS NOT NULL
```

### Scheduler Thread

The scheduler runs in a background thread that:
- Checks for pending jobs every second
- Executes scheduled syncs
- Logs all activity
- Handles errors gracefully

## Support

If you encounter issues:

1. Run the test script: `python test_hana_incremental_sync.py`
2. Check application logs
3. Verify HANA and ClickHouse connections
4. Review sync metadata in ClickHouse
5. Check schedule status in "View Schedules"

## Summary

✅ HANA scheduling is fully integrated into the system  
✅ Incremental sync automatically detects and syncs new data  
✅ Schedules are automatically restored on server startup  
✅ Full error handling and logging is in place  
✅ Test scripts available for verification  

The system is ready for production use with HANA sources!

