# scheduler_utils.py
import schedule as sched
import threading
import time
import datetime
from db_utils import get_pg_connection, init_pg_schema
from hybrid_sync import process_sql_server_hybrid
from manage_server import load_config
from dashboard import log_sync
from utils.email_service import email_service
import os

# Initialize DB schema
init_pg_schema()

# In-memory scheduled jobs
scheduled_jobs = []
_scheduler_thread = None

# ---------------- Scheduler Loop ----------------
def run_scheduler():
    while True:
        sched.run_pending()
        time.sleep(1)

def _start_scheduler_thread():
    global _scheduler_thread
    if _scheduler_thread is None or not _scheduler_thread.is_alive():
        _scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        _scheduler_thread.start()

# ---------------- DB Utilities ----------------
def _save_schedule_to_db(server_name, job_type, last_run, status, error, source_id=None):
    conn = get_pg_connection()
    cur = conn.cursor()
    # First check if source_id column exists, if not, use old format
    try:
        cur.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_schema = 'metrics_sync_tables' 
            AND table_name = 'schedules' 
            AND column_name = 'source_id'
        """)
        has_source_id = cur.fetchone() is not None
        
        if has_source_id:
            cur.execute("""
                DELETE FROM metrics_sync_tables.schedules
                WHERE (server_name = %s AND job_type = %s) OR (source_id = %s AND job_type = %s)
            """, (server_name, job_type, source_id, job_type))
            cur.execute("""
                INSERT INTO metrics_sync_tables.schedules
                    (server_name, job_type, last_run, status, error, source_id)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (server_name, job_type, last_run, status, error, source_id))
        else:
            # Old format without source_id
            cur.execute("""
                DELETE FROM metrics_sync_tables.schedules
                WHERE server_name = %s AND job_type = %s
            """, (server_name, job_type))
            cur.execute("""
                INSERT INTO metrics_sync_tables.schedules
                    (server_name, job_type, last_run, status, error)
                VALUES (%s, %s, %s, %s, %s)
            """, (server_name, job_type, last_run, status, error))
    except Exception as e:
        # Fallback to old format
        cur.execute("""
            DELETE FROM metrics_sync_tables.schedules
            WHERE server_name = %s AND job_type = %s
        """, (server_name, job_type))
        cur.execute("""
            INSERT INTO metrics_sync_tables.schedules
                (server_name, job_type, last_run, status, error)
            VALUES (%s, %s, %s, %s, %s)
        """, (server_name, job_type, last_run, status, error))
    conn.commit()
    cur.close()
    conn.close()

# ---------------- Job Wrapper ----------------
def _job_wrapper(server_name, server_conf, job_type):
    status = "success"
    error_message = None
    timestamp = datetime.datetime.now()
    
    print(f"\n{'='*60}")
    print(f"[SYNC] STARTED: {server_name} ({job_type})")
    print(f"[TIME] {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[TARGET] {server_conf.get('server', 'Unknown')}:{server_conf.get('port', 'Unknown')}")
    print(f"[DATABASE] {server_conf.get('target_postgres_db', 'Unknown')}")
    print(f"{'='*60}\n")

    try:
        # Record that this sync has started so we can detect stuck runs later
        log_sync(server_name, 'started', f'Started {job_type} at {timestamp.strftime("%Y-%m-%d %H:%M:%S")}')

        process_sql_server_hybrid(server_name, server_conf)
        
        # Calculate duration
        end_timestamp = datetime.datetime.now()
        duration = end_timestamp - timestamp
        duration_str = f"{int(duration.total_seconds() // 60)}m {int(duration.total_seconds() % 60)}s"
        
        print(f"\n[OK] SYNC COMPLETED: {server_name} at {end_timestamp.strftime('%H:%M:%S')}")
        print(f"[STATUS] SUCCESS")
        print(f"[DURATION] {duration_str}\n")
        
        # Send success email notification for scheduled sync
        try:
            email_result = email_service.notify_schedule_success(
                server_name=server_name,
                job_type=job_type,
                duration=duration_str
            )
            if email_result.success:
                print(f"[EMAIL] Success notification sent for scheduled sync: {server_name} ({job_type})")
            else:
                print(f"[EMAIL] Failed to send success notification: {email_result.error}")
        except Exception as e:
            print(f"[EMAIL] Error sending success notification: {e}")
            
    except Exception as e:
        status = "failed"
        error_message = str(e)
        end_timestamp = datetime.datetime.now()
        
        print(f"\n[ERROR] SYNC FAILED: {server_name} at {end_timestamp.strftime('%H:%M:%S')}")
        print(f"[ERROR] {error_message}\n")
        
        # Send failure email notification for scheduled sync
        try:
            email_result = email_service.notify_schedule_failed(
                server_name=server_name,
                job_type=job_type,
                error_message=error_message
            )
            if email_result.success:
                print(f"[EMAIL] Failure notification sent for scheduled sync: {server_name} ({job_type})")
            else:
                print(f"[EMAIL] Failed to send failure notification: {email_result.error}")
        except Exception as e:
            print(f"[EMAIL] Error sending failure notification: {e}")

    # Update memory jobs
    for job in scheduled_jobs:
        if job["server"] == server_name and job["type"] == job_type:
            job.update({
                "last_run": timestamp,
                "status": status,
                "error": error_message
            })
            break

    log_sync(server_name, status, error_message)
    _save_schedule_to_db(server_name, job_type, timestamp, status, error_message)

def _source_job_wrapper(source_id, job_type):
    """Wrapper for data source syncs (HANA and SQL Server from database)"""
    import psycopg2
    from db_utils import load_pg_config, get_pg_connection
    from sync_logger import SyncLogger
    
    status = "success"
    error_message = None
    timestamp = datetime.datetime.now()
    source_name = f"source_{source_id}"
    source_type = "unknown"
    records_synced = 0
    
    try:
        # Get source info from database
        pg_conf = load_pg_config()
        conn = psycopg2.connect(
            dbname=pg_conf.get('database', 'metrics_sync_tables'),
            user=pg_conf.get('username'),
            password=pg_conf.get('password'),
            host=pg_conf.get('host'),
            port=int(pg_conf.get('port', 5432))
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT id, source_name, source_type, server_address, username, password, 
                   target_type, target_database, connection_details
            FROM data_sources WHERE id = %s AND is_active = true
        """, (source_id,))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Source ID {source_id} not found or inactive")
        
        source_name = row[1]
        source_type = row[2]
        server_address = row[3]
        username = row[4]
        password = row[5]
        target_type = row[6]
        target_database = row[7]
        connection_details = row[8] if row[8] else {}
        
        cur.close()
        conn.close()
        
        print(f"\n{'='*60}")
        print(f"[SYNC] STARTED: {source_name} ({source_type}, {job_type})")
        print(f"[TIME] {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"[SOURCE_ID] {source_id}")
        print(f"{'='*60}\n")
        
        # Log sync start
        SyncLogger.log_sync_start(
            source_type=source_type,
            source_name=source_name,
            source_id=source_id,
            details={"server_address": server_address, "target_type": target_type, "target_database": target_database}
        )
        
        # Perform sync based on source type
        if source_type == 'sap_hana':
            # HANA sync
            from hana_sync import HanaToClickHouseSync
            import json
            
            # Parse connection details
            if isinstance(connection_details, str):
                conn_details = json.loads(connection_details)
            else:
                conn_details = connection_details or {}
            
            hana_config = {
                'host': conn_details.get('host') or (server_address.split(':')[0] if ':' in server_address else server_address),
                'port': int(conn_details.get('port') or (server_address.split(':')[1] if ':' in server_address else 30015)),
                'username': username,
                'password': password or conn_details.get('password', '')
            }
            
            clickhouse_config = {
                'host': os.environ.get('CLICKHOUSE_HOST', 'localhost'),
                'port': int(os.environ.get('CLICKHOUSE_PORT', 9000)),
                'user': os.environ.get('CLICKHOUSE_USER', 'default'),
                'password': os.environ.get('CLICKHOUSE_PASSWORD', ''),
                'database': target_database
            }
            
            sync_engine = HanaToClickHouseSync(hana_config, clickhouse_config)
            
            # Try to connect to HANA - if offline, fail gracefully with clear message
            try:
                if not sync_engine.connect_hana():
                    raise Exception(f"Failed to connect to HANA at {hana_config['host']}:{hana_config['port']}. "
                                  f"Source may be offline. Schedule will retry on next run.")
            except Exception as conn_error:
                # If connection fails, provide helpful error message
                error_msg = str(conn_error)
                if "offline" not in error_msg.lower() and "connection" not in error_msg.lower():
                    error_msg = f"Connection failed: {error_msg}"
                raise Exception(f"Failed to connect to HANA at {hana_config['host']}:{hana_config['port']}. "
                              f"Source may be offline. Schedule will retry on next run. Details: {error_msg}")
            
            # Try to connect to ClickHouse
            try:
                if not sync_engine.connect_clickhouse():
                    raise Exception(f"Failed to connect to ClickHouse at {clickhouse_config['host']}:{clickhouse_config['port']}")
            except Exception as ch_error:
                raise Exception(f"Failed to connect to ClickHouse at {clickhouse_config['host']}:{clickhouse_config['port']}. "
                              f"Error: {str(ch_error)}")
            
            # Perform incremental sync
            try:
                results = sync_engine.sync_incremental(target_database)
                records_synced = sum(r.get('records_synced', 0) for r in results if r and isinstance(r, dict))
            except Exception as sync_error:
                raise Exception(f"HANA incremental sync failed: {str(sync_error)}")
            
        elif source_type == 'sql_server':
            # SQL Server from database - use existing hybrid sync logic
            from hybrid_sync import process_sql_server_hybrid
            from db_utils import decrypt_password
            
            # Parse connection details if present
            conn_details = {}
            if connection_details:
                if isinstance(connection_details, str):
                    import json
                    conn_details = json.loads(connection_details)
                else:
                    conn_details = connection_details or {}
            
            # Decrypt password if needed
            try:
                decrypted_password = decrypt_password(password) if password else ''
            except Exception as e:
                decrypted_password = password or ''
                print(f"[SCHEDULER] Could not decrypt password, using as-is: {e}")
            
            # Build server_conf similar to YAML config format
            server_conf = {
                'server': server_address,
                'username': username,
                'password': decrypted_password,
                'target_postgres_db': target_database,  # PostgreSQL target database
                'port': conn_details.get('port'),  # Optional port override
                'driver': conn_details.get('driver', 'ODBC Driver 17 for SQL Server'),  # Default driver
                'skip_databases': conn_details.get('skip_databases', []),  # Databases to skip
                'skip_schemas': conn_details.get('skip_schemas', []),  # Schemas to skip
            }
            
            # Use source name as server_name for process_sql_server_hybrid
            # This ensures proper logging and tracking
            print(f"[SCHEDULER] Starting SQL Server sync for {source_name} (ID: {source_id})")
            print(f"[SCHEDULER] Server: {server_address}, Target DB: {target_database}")
            
            # Call the hybrid sync process
            # This will handle connection, sync, and errors gracefully
            try:
                process_sql_server_hybrid(source_name, server_conf)
                
                # Get records synced from sync history (approximate)
                # Note: process_sql_server_hybrid doesn't return a count directly
                # We'll estimate based on sync tracking or log it as successful
                records_synced = 0  # Will be updated if we can track it
                print(f"[SCHEDULER] SQL Server sync completed successfully for {source_name}")
            except Exception as sync_error:
                # If sync fails (e.g., server offline), raise to be caught by outer try/except
                error_msg = str(sync_error)
                if "connection" in error_msg.lower() or "offline" in error_msg.lower() or "timeout" in error_msg.lower():
                    raise Exception(f"Failed to connect to SQL Server at {server_address}. "
                                 f"Source may be offline. Schedule will retry on next run. Error: {error_msg}")
                else:
                    raise Exception(f"SQL Server sync failed: {error_msg}")
        else:
            raise ValueError(f"Unsupported source type for scheduling: {source_type}")
        
        # Calculate duration
        end_timestamp = datetime.datetime.now()
        duration = (end_timestamp - timestamp).total_seconds()
        duration_str = f"{int(duration // 60)}m {int(duration % 60)}s"
        
        print(f"\n[OK] SYNC COMPLETED: {source_name} at {end_timestamp.strftime('%H:%M:%S')}")
        print(f"[STATUS] SUCCESS")
        print(f"[DURATION] {duration_str}\n")
        
        # Log sync completion
        SyncLogger.log_sync_complete(
            source_type=source_type,
            source_name=source_name,
            source_id=source_id,
            records_synced=records_synced,
            duration=duration,
            target_db=target_database
        )
        
        # Send success email notification
        try:
            email_result = email_service.notify_schedule_success(
                server_name=source_name,
                job_type=job_type,
                duration=duration_str
            )
            if email_result.success:
                print(f"[EMAIL] Success notification sent for scheduled sync: {source_name} ({job_type})")
        except Exception as e:
            print(f"[EMAIL] Error sending success notification: {e}")
            
    except Exception as e:
        status = "failed"
        error_message = str(e)
        end_timestamp = datetime.datetime.now()
        duration = (end_timestamp - timestamp).total_seconds()
        
        print(f"\n[ERROR] SYNC FAILED: {source_name} at {end_timestamp.strftime('%H:%M:%S')}")
        print(f"[ERROR] {error_message}\n")
        
        # Log sync failure
        try:
            SyncLogger.log_sync_failed(
                source_type=source_type,
                source_name=source_name,
                source_id=source_id,
                error_msg=error_message,
                duration=duration
            )
        except:
            pass
        
        # Send failure email notification
        try:
            email_result = email_service.notify_schedule_failed(
                server_name=source_name,
                job_type=job_type,
                error_message=error_message
            )
            if email_result.success:
                print(f"[EMAIL] Failure notification sent for scheduled sync: {source_name} ({job_type})")
        except Exception as e:
            print(f"[EMAIL] Error sending failure notification: {e}")

    # Update memory jobs
    for job in scheduled_jobs:
        if job.get("source_id") == source_id and job["type"] == job_type:
            job.update({
                "last_run": timestamp,
                "status": status,
                "error": error_message
            })
            break

    log_sync(source_name, status, error_message)
    _save_schedule_to_db(source_name, job_type, timestamp, status, error_message, source_id)

def _add_job_metadata(server_name, job_type, source_id=None):
    for job in scheduled_jobs:
        if job.get("source_id") == source_id and job["type"] == job_type:
            return
        if not source_id and job["server"] == server_name and job["type"] == job_type:
            return
    job_data = {
        "server": server_name,
        "type": job_type,
        "last_run": None,
        "status": "pending",
        "error": None
    }
    if source_id:
        job_data["source_id"] = source_id
    scheduled_jobs.append(job_data)
    _save_schedule_to_db(server_name, job_type, None, "pending", None, source_id)

# ---------------- Scheduling ----------------
def health_check_all_servers():
    """Ping all configured SQL servers and send email on failures."""
    try:
        cfg = load_config()
        for s_name, s_conf in cfg.get('sqlservers', {}).items():
            try:
                conn = get_pg_connection()  # ensure PG reachable
                conn.close()
            except Exception:
                # continue even if PG check fails; focus on SQL servers
                pass
            try:
                from hybrid_sync import get_sql_connection
                c = get_sql_connection(s_conf)
                c.close()
            except Exception as e:
                try:
                    email_service.notify_server_down(s_name, str(e))
                except Exception:
                    pass
    except Exception:
        # Don't raise from scheduler
        pass

def schedule_interval_sync(server_name, minutes):
    config = load_config()
    server_conf = config['sqlservers'].get(server_name)
    if not server_conf:
        raise ValueError(f"Server {server_name} not found in config")
    job_type = f"interval_{minutes}m"

    sched.every(minutes).minutes.do(
        _job_wrapper, server_name, server_conf, job_type
    ).tag(server_name, f"{server_name}:{job_type}")

    _add_job_metadata(server_name, job_type)
    _start_scheduler_thread()

def schedule_daily_sync(server_name, hour, minute):
    config = load_config()
    server_conf = config['sqlservers'].get(server_name)
    if not server_conf:
        raise ValueError(f"Server {server_name} not found in config")
    time_str = f"{hour:02d}:{minute:02d}"
    job_type = f"daily_{time_str}"

    sched.every().day.at(time_str).do(
        _job_wrapper, server_name, server_conf, job_type
    ).tag(server_name, f"{server_name}:{job_type}")

    _add_job_metadata(server_name, job_type)
    _start_scheduler_thread()

def schedule_source_interval_sync(source_id, minutes):
    """Schedule interval sync for a data source (HANA or SQL Server from database)
    
    Note: This schedules the sync job regardless of source connection status.
    If the source is offline when the job runs, it will fail gracefully with proper logging.
    """
    import psycopg2
    from db_utils import load_pg_config
    
    # Validate source_id
    if not source_id:
        raise ValueError("source_id is required")
    
    try:
        source_id = int(source_id)
    except (ValueError, TypeError):
        raise ValueError(f"Invalid source_id: {source_id}")
    
    # Get source name and type for display and validation
    source_name = f"source_{source_id}"
    source_type = None
    try:
        pg_conf = load_pg_config()
        conn = psycopg2.connect(
            dbname=pg_conf.get('database', 'metrics_sync_tables'),
            user=pg_conf.get('username'),
            password=pg_conf.get('password'),
            host=pg_conf.get('host'),
            port=int(pg_conf.get('port', 5432))
        )
        cur = conn.cursor()
        cur.execute("SELECT source_name, source_type FROM data_sources WHERE id = %s AND is_active = true", (source_id,))
        row = cur.fetchone()
        if row:
            source_name = row[0]
            source_type = row[1]
            print(f"[SCHEDULER] Scheduling {source_type} source '{source_name}' (ID: {source_id}) every {minutes} minutes")
        else:
            raise ValueError(f"Source ID {source_id} not found or inactive")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[SCHEDULER] WARNING: Could not verify source in database: {e}")
        print(f"[SCHEDULER] Proceeding with scheduling anyway (source_id: {source_id})")
    
    job_type = f"interval_{minutes}m"
    
    # Schedule the job - connection status is checked at runtime, not schedule time
    sched.every(minutes).minutes.do(
        _source_job_wrapper, source_id, job_type
    ).tag(f"source_{source_id}", f"source_{source_id}:{job_type}")
    
    print(f"[SCHEDULER] Successfully scheduled interval sync for source {source_id} ({source_name}) every {minutes} minutes")
    
    _add_job_metadata(source_name, job_type, source_id)
    _start_scheduler_thread()

def schedule_source_daily_sync(source_id, hour, minute):
    """Schedule daily sync for a data source (HANA or SQL Server from database)
    
    Note: This schedules the sync job regardless of source connection status.
    If the source is offline when the job runs, it will fail gracefully with proper logging.
    """
    import psycopg2
    from db_utils import load_pg_config
    
    # Validate source_id
    if not source_id:
        raise ValueError("source_id is required")
    
    try:
        source_id = int(source_id)
    except (ValueError, TypeError):
        raise ValueError(f"Invalid source_id: {source_id}")
    
    # Get source name and type for display and validation
    source_name = f"source_{source_id}"
    source_type = None
    try:
        pg_conf = load_pg_config()
        conn = psycopg2.connect(
            dbname=pg_conf.get('database', 'metrics_sync_tables'),
            user=pg_conf.get('username'),
            password=pg_conf.get('password'),
            host=pg_conf.get('host'),
            port=int(pg_conf.get('port', 5432))
        )
        cur = conn.cursor()
        cur.execute("SELECT source_name, source_type FROM data_sources WHERE id = %s AND is_active = true", (source_id,))
        row = cur.fetchone()
        if row:
            source_name = row[0]
            source_type = row[1]
            print(f"[SCHEDULER] Scheduling {source_type} source '{source_name}' (ID: {source_id}) daily at {hour:02d}:{minute:02d}")
        else:
            raise ValueError(f"Source ID {source_id} not found or inactive")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[SCHEDULER] WARNING: Could not verify source in database: {e}")
        print(f"[SCHEDULER] Proceeding with scheduling anyway (source_id: {source_id})")
    
    time_str = f"{hour:02d}:{minute:02d}"
    job_type = f"daily_{time_str}"
    
    # Schedule the job - connection status is checked at runtime, not schedule time
    sched.every().day.at(time_str).do(
        _source_job_wrapper, source_id, job_type
    ).tag(f"source_{source_id}", f"source_{source_id}:{job_type}")
    
    print(f"[SCHEDULER] Successfully scheduled daily sync for source {source_id} ({source_name}) at {time_str}")
    
    _add_job_metadata(source_name, job_type, source_id)
    _start_scheduler_thread()

def delete_schedule(server_name, job_type):
    """
    Delete a schedule completely from memory, DB, and the schedule library.
    This is the definitive deletion function that ensures permanent removal.
    """
    global scheduled_jobs
    
    # Remove from in-memory list
    scheduled_jobs = [job for job in scheduled_jobs if not (job["server"] == server_name and job["type"] == job_type)]
    
    # Mark as DELETED in database (instead of hard delete)
    conn = get_pg_connection()
    cur = conn.cursor()
    try:
        # Soft delete - mark as deleted to prevent reloading
        cur.execute("""
            UPDATE metrics_sync_tables.schedules 
            SET status = 'deleted', error = 'Schedule permanently deleted by user'
            WHERE server_name = %s AND job_type = %s
        """, (server_name, job_type))
        
        # If no rows updated, insert a deleted record to prevent future creation
        if cur.rowcount == 0:
            cur.execute("""
                INSERT INTO metrics_sync_tables.schedules 
                (server_name, job_type, status, error, last_run)
                VALUES (%s, %s, 'deleted', 'Schedule permanently deleted by user', NOW())
                ON CONFLICT (server_name, job_type) DO UPDATE SET
                status = 'deleted', error = 'Schedule permanently deleted by user'
            """, (server_name, job_type))
        
        print(f"[SCHEDULER_DELETE] Permanently marked {cur.rowcount} schedule record(s) as deleted for {server_name}-{job_type}")
    except Exception as e:
        print(f"[SCHEDULER_DELETE] Error updating database: {e}")
    
    conn.commit()
    cur.close()
    conn.close()
    
    # Clear scheduled jobs from schedule library with all possible tag formats
    try:
        tag_formats = [
            f"{server_name}:{job_type}",
            f"{server_name}-{job_type}", 
            f"{server_name}-interval" if job_type.startswith("interval") else f"{server_name}-daily",
            f"{server_name}_interval" if job_type.startswith("interval") else f"{server_name}_daily"
        ]
        
        for tag in tag_formats:
            sched.clear(tag)
            print(f"[SCHEDULER_DELETE] Cleared tag: {tag}")
            
        # Also manually remove any jobs that might match
        all_jobs = sched.jobs[:]
        for job in all_jobs:
            if hasattr(job, 'tags') and any(tag in job.tags for tag in tag_formats):
                sched.cancel_job(job)
                print(f"[SCHEDULER_DELETE] Cancelled job: {job}")
                
    except Exception as e:
        print(f"[SCHEDULER_DELETE] Error clearing schedule library: {e}")
    
    print(f"[SCHEDULER_DELETE] Schedule {server_name}-{job_type} completely deleted from all locations")

def clean_deleted_schedules():
    """Utility function to permanently remove schedules marked as deleted from database"""
    conn = get_pg_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM metrics_sync_tables.schedules WHERE status = 'deleted'")
        deleted_count = cur.rowcount
        conn.commit()
        print(f"[CLEAN_DELETED] Permanently removed {deleted_count} deleted schedule records")
        return deleted_count
    except Exception as e:
        print(f"[CLEAN_DELETED] Error cleaning deleted schedules: {e}")
        return 0
    finally:
        cur.close()
        conn.close()

def mark_stale_in_progress(threshold_minutes=60):
    """
    Mark any 'in-progress' sync_history rows older than threshold_minutes as 'failed'.
    This helps detect runs that were started but never completed (process killed, crash).
    """
    conn = get_pg_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE metrics_sync_tables.sync_history
            SET status = 'failed',
                details = COALESCE(details, '') || ' | Marked failed by watchdog: no completion observed'
            WHERE status = 'in-progress'
              AND sync_time < NOW() - (%s || ' minutes')::interval
        """, (str(threshold_minutes),))
        updated = cur.rowcount
        conn.commit()
        print(f"[WATCHDOG] Marked {updated} stale 'in-progress' sync(s) as failed (threshold: {threshold_minutes}m)")
        return updated
    except Exception as e:
        print(f"[WATCHDOG] Error marking stale in-progress syncs: {e}")
        return 0
    finally:
        cur.close()
        conn.close()

def update_schedule(server_name, job_type, **kwargs):
    delete_schedule(server_name, job_type)
    if job_type.startswith("interval"):
        minutes = kwargs.get("minutes")
        schedule_interval_sync(server_name, minutes)
    elif job_type.startswith("daily"):
        hour = kwargs.get("hour")
        minute = kwargs.get("minute")
        schedule_daily_sync(server_name, hour, minute)

def get_schedules():
    conn = get_pg_connection()
    cur = conn.cursor()
    # Check if source_id column exists
    try:
        cur.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_schema = 'metrics_sync_tables' 
            AND table_name = 'schedules' 
            AND column_name = 'source_id'
        """)
        has_source_id = cur.fetchone() is not None
        
        if has_source_id:
            cur.execute("""
                SELECT server_name, job_type,
                       COALESCE(last_run::text, '-') AS last_run,
                       status,
                       COALESCE(error, '-') AS error,
                       source_id
                FROM metrics_sync_tables.schedules
                WHERE (status != 'deleted' OR status IS NULL)
                ORDER BY created_at DESC
            """)
            rows = cur.fetchall()
            # Get source types for database sources
            result = []
            for r in rows:
                source_type = None
                if r[5]:  # source_id exists
                    # Try to get source type from data_sources
                    try:
                        cur2 = conn.cursor()
                        cur2.execute("SELECT source_type FROM data_sources WHERE id = %s", (r[5],))
                        type_row = cur2.fetchone()
                        if type_row:
                            source_type = type_row[0]
                        cur2.close()
                    except:
                        pass
                result.append({
                    "server": r[0], 
                    "type": r[1], 
                    "last_run": r[2], 
                    "status": r[3], 
                    "error": r[4],
                    "source_type": source_type
                })
        else:
            # Old format
            cur.execute("""
                SELECT server_name, job_type,
                       COALESCE(last_run::text, '-') AS last_run,
                       status,
                       COALESCE(error, '-') AS error
                FROM metrics_sync_tables.schedules
                WHERE status != 'deleted' OR status IS NULL
                ORDER BY created_at DESC
            """)
            rows = cur.fetchall()
            result = [{"server": r[0], "type": r[1], "last_run": r[2], "status": r[3], "error": r[4]} for r in rows]
    except Exception as e:
        # Fallback to old format
        cur.execute("""
            SELECT server_name, job_type,
                   COALESCE(last_run::text, '-') AS last_run,
                   status,
                   COALESCE(error, '-') AS error
            FROM metrics_sync_tables.schedules
            WHERE status != 'deleted' OR status IS NULL
            ORDER BY created_at DESC
        """)
        rows = cur.fetchall()
        result = [{"server": r[0], "type": r[1], "last_run": r[2], "status": r[3], "error": r[4]} for r in rows]
    
    cur.close()
    conn.close()
    return result

def load_schedules_from_db():
    """
    Load only active schedules from database (exclude deleted ones).
    This function runs automatically on server startup to restore all schedules.
    """
    try:
        conn = get_pg_connection()
    except Exception as e:
        print(f"\n[SCHEDULER WARNING] Cannot connect to database: {e}")
        print("[SCHEDULER] Skipping schedule restoration. Database may not be configured yet.")
        print("[SCHEDULER] Schedules will be loaded when database becomes available.\n")
        return
    
    cur = conn.cursor()
    try:
        # IMPORTANT: Exclude schedules marked as 'deleted'
        cur.execute("""
            SELECT server_name, job_type 
            FROM metrics_sync_tables.schedules 
            WHERE status != 'deleted' OR status IS NULL
        """)
        rows = cur.fetchall()
        loaded_count = 0
        failed_count = 0
        
        print("\n" + "="*70)
        print("[SCHEDULER] RESTORING SCHEDULES FROM DATABASE")
        print("="*70)
        
        if not rows:
            print("[SCHEDULER] No active schedules found in database")
            print("="*70 + "\n")
            return
        
        print(f"[SCHEDULER] Found {len(rows)} schedule(s) to restore")
        
        for server_name, job_type in rows:
            try:
                # Ensure no duplicate schedule remains - clear any existing first
                tag_formats = [
                    f"{server_name}:{job_type}",
                    f"{server_name}-{job_type}"
                ]
                for tag in tag_formats:
                    sched.clear(tag)
                
                if job_type.startswith("interval_"):
                    minutes = int(job_type.replace("interval_", "").replace("m", ""))
                    schedule_interval_sync(server_name, minutes)
                    print(f"  ✓ Restored: {server_name} - Every {minutes} minutes")
                    loaded_count += 1
                elif job_type.startswith("daily_"):
                    time_str = job_type.replace("daily_", "")
                    hour, minute = map(int, time_str.split(":"))
                    schedule_daily_sync(server_name, hour, minute)
                    print(f"  ✓ Restored: {server_name} - Daily at {time_str}")
                    loaded_count += 1
                else:
                    print(f"  ⚠ Skipped: {server_name} - Unknown schedule type: {job_type}")
                    failed_count += 1
            except ValueError as e:
                print(f"  ✗ Failed: {server_name} - Invalid format: {e}")
                failed_count += 1
            except Exception as e:
                print(f"  ✗ Failed: {server_name} - Error: {e}")
                failed_count += 1
        
        print("-"*70)
        print(f"[SCHEDULER] Restoration complete:")
        print(f"  • Successfully loaded: {loaded_count} schedule(s)")
        if failed_count > 0:
            print(f"  • Failed to load: {failed_count} schedule(s)")
        print("="*70 + "\n")
        
    except Exception as e:
        print(f"\n[SCHEDULER ERROR] Failed to load schedules from database: {e}\n")
    finally:
        cur.close()
        conn.close()

# Auto-load schedules on module import (happens on server startup)
print("\n[SCHEDULER] Initializing scheduler system...")
load_schedules_from_db()

# Schedule periodic watchdog to mark stale in-progress syncs (runs every 30 minutes)
try:
    sched.every(30).minutes.do(mark_stale_in_progress, 60).tag('watchdog')
    # run once at startup to catch any existing stale records
    mark_stale_in_progress(60)
except Exception as e:
    print(f"[WATCHDOG] Failed to schedule watchdog: {e}")

# Schedule periodic health checks for server availability
try:
    sched.every(15).minutes.do(health_check_all_servers).tag('health_check')
except Exception as e:
    print(f"[HEALTH] Failed to schedule health checks: {e}")
