import schedule
import time
import threading
import datetime
from hybrid_sync import process_sql_server_hybrid
from manage_server import load_config
from dashboard import log_sync
from db_utils import get_pg_connection, init_pg_schema

# Ensure schema + table exist at startup
init_pg_schema()

# In-memory active jobs (needed for schedule lib)
scheduled_jobs = []

# Background thread (singleton)
_scheduler_thread = None


def run_scheduler():
    """Background scheduler loop"""
    while True:
        schedule.run_pending()
        time.sleep(1)


def _start_scheduler_thread():
    """Ensure scheduler thread is running only once"""
    global _scheduler_thread
    if _scheduler_thread is None or not _scheduler_thread.is_alive():
        _scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        _scheduler_thread.start()


def _save_schedule_to_db(server_name, job_type, last_run, status, error):
    """Upsert a schedule row in Postgres"""
    conn = get_pg_connection()
    cur = conn.cursor()

    # Delete old entry then insert fresh (simple upsert)
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


def _job_wrapper(server_name, server_conf, job_type):
    """Wrapper to run sync and log results"""
    status = "success"
    error_message = None
    timestamp = datetime.datetime.now()

    try:
        process_sql_server_hybrid(server_name, server_conf)
    except Exception as e:
        status = "failed"
        error_message = str(e)

    # Update memory jobs
    for job in scheduled_jobs:
        if job["server"] == server_name and job["type"] == job_type:
            job.update({
                "last_run": timestamp,
                "status": status,
                "error": error_message
            })
            break

    # ✅ Log to dashboard history
    log_sync(server_name, status, error_message)

    # ✅ Persist to DB
    _save_schedule_to_db(server_name, job_type, timestamp, status, error_message)


def _add_job_metadata(server_name, job_type):
    """Add a job entry in memory + DB when scheduled"""
    for job in scheduled_jobs:
        if job["server"] == server_name and job["type"] == job_type:
            return  # already exists

    job_entry = {
        "server": server_name,
        "type": job_type,
        "last_run": None,
        "status": "pending",
        "error": None
    }
    scheduled_jobs.append(job_entry)

    # Insert into DB (pending status)
    _save_schedule_to_db(server_name, job_type, None, "pending", None)


def schedule_interval_sync(server_name, minutes):
    """Schedule job every N minutes"""
    config = load_config()
    server_conf = config['sqlservers'].get(server_name)
    if not server_conf:
        raise ValueError(f"Server {server_name} not found in config")

    job_type = f"interval_{minutes}m"

    schedule.every(minutes).minutes.do(
        _job_wrapper, server_name, server_conf, job_type
    ).tag(server_name)

    _add_job_metadata(server_name, job_type)
    _start_scheduler_thread()


def schedule_daily_sync(server_name, hour, minute):
    """Schedule job daily at HH:MM"""
    config = load_config()
    server_conf = config['sqlservers'].get(server_name)
    if not server_conf:
        raise ValueError(f"Server {server_name} not found in config")

    time_str = f"{hour:02d}:{minute:02d}"
    job_type = f"daily_{time_str}"

    schedule.every().day.at(time_str).do(
        _job_wrapper, server_name, server_conf, job_type
    ).tag(server_name)

    _add_job_metadata(server_name, job_type)
    _start_scheduler_thread()


def clear_schedules():
    """Clear all scheduled jobs (memory + DB)"""
    schedule.clear()
    scheduled_jobs.clear()

    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("TRUNCATE metrics_sync_tables.schedules;")
    conn.commit()
    cur.close()
    conn.close()


def get_schedules():
    """Return list of schedules directly from DB (authoritative source)"""
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT server_name,
               job_type,
               COALESCE(last_run::text, '-') AS last_run,
               status,
               COALESCE(error, '-') AS error
        FROM metrics_sync_tables.schedules
        ORDER BY created_at DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {
            "server": r[0],
            "type": r[1],
            "last_run": r[2],
            "status": r[3],
            "error": r[4]
        }
        for r in rows
    ]


def load_schedules_from_db():
    """Reload schedules from DB after restart"""
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("SELECT server_name, job_type FROM metrics_sync_tables.schedules;")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    for server_name, job_type in rows:
        if job_type.startswith("interval_"):
            minutes = int(job_type.replace("interval_", "").replace("m", ""))
            schedule_interval_sync(server_name, minutes)
        elif job_type.startswith("daily_"):
            time_str = job_type.replace("daily_", "")
            hour, minute = map(int, time_str.split(":"))
            schedule_daily_sync(server_name, hour, minute)


# ✅ Auto-load existing schedules on startup
load_schedules_from_db()
