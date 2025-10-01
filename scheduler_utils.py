# scheduler_utils.py
import schedule as sched
import threading
import time
import datetime
from db_utils import get_pg_connection, init_pg_schema
from hybrid_sync import process_sql_server_hybrid
from manage_server import load_config
from dashboard import log_sync

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
def _save_schedule_to_db(server_name, job_type, last_run, status, error):
    conn = get_pg_connection()
    cur = conn.cursor()
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
        process_sql_server_hybrid(server_name, server_conf)
        print(f"\n[OK] SYNC COMPLETED: {server_name} at {datetime.datetime.now().strftime('%H:%M:%S')}")
        print(f"[STATUS] SUCCESS\n")
    except Exception as e:
        status = "failed"
        error_message = str(e)
        print(f"\n[ERROR] SYNC FAILED: {server_name} at {datetime.datetime.now().strftime('%H:%M:%S')}")
        print(f"[ERROR] {error_message}\n")

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

def _add_job_metadata(server_name, job_type):
    for job in scheduled_jobs:
        if job["server"] == server_name and job["type"] == job_type:
            return
    scheduled_jobs.append({
        "server": server_name,
        "type": job_type,
        "last_run": None,
        "status": "pending",
        "error": None
    })
    _save_schedule_to_db(server_name, job_type, None, "pending", None)

# ---------------- Scheduling ----------------
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

def delete_schedule(server_name, job_type):
    """
    Delete a schedule completely from memory, DB, and the schedule library.
    This is the definitive deletion function that ensures permanent removal.
    """
    global scheduled_jobs
    
    # Remove from in-memory list
    scheduled_jobs = [job for job in scheduled_jobs if not (job["server"] == server_name and job["type"] == job_type)]
    
    # Permanently delete from database
    conn = get_pg_connection()
    cur = conn.cursor()
    try:
        # Hard delete - completely remove the record
        cur.execute("""
            DELETE FROM metrics_sync_tables.schedules
            WHERE server_name = %s AND job_type = %s
        """, (server_name, job_type))
        print(f"[SCHEDULER_DELETE] Permanently deleted {cur.rowcount} schedule record(s) for {server_name}-{job_type}")
    except Exception as e:
        print(f"[SCHEDULER_DELETE] Error deleting from database: {e}")
    
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
    cur.execute("""
        SELECT server_name, job_type,
               COALESCE(last_run::text, '-') AS last_run,
               status,
               COALESCE(error, '-') AS error
        FROM metrics_sync_tables.schedules
        WHERE status IS DISTINCT FROM 'deleted'
        ORDER BY created_at DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [{"server": r[0], "type": r[1], "last_run": r[2], "status": r[3], "error": r[4]} for r in rows]

def load_schedules_from_db():
    """
    Load only active schedules from database (exclude deleted ones)
    """
    conn = get_pg_connection()
    cur = conn.cursor()
    try:
        # Only load schedules that are not deleted - use proper exclusion
        cur.execute("""
            SELECT server_name, job_type 
            FROM metrics_sync_tables.schedules 
            WHERE status IS NULL OR status != 'deleted'
        """)
        rows = cur.fetchall()
        loaded_count = 0
        
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
                    loaded_count += 1
                elif job_type.startswith("daily_"):
                    time_str = job_type.replace("daily_", "")
                    hour, minute = map(int, time_str.split(":"))
                    schedule_daily_sync(server_name, hour, minute)
                    loaded_count += 1
            except ValueError as e:
                print(f"⚠️ Skipping invalid schedule for {server_name}: {e}")
                continue
        
        print(f"[LOAD_SCHEDULES] Successfully loaded {loaded_count} active schedules from database")
        
    except Exception as e:
        print(f"[LOAD_SCHEDULES] Error loading schedules: {e}")
    finally:
        cur.close()
        conn.close()

# Auto-load schedules
load_schedules_from_db()
