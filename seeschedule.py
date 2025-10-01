# seeschedular.py
import scheduler_utils
from flask import render_template, session
from hybrid_sync import process_sql_server_hybrid
from manage_server import load_config
from dashboard import log_sync
from db_utils import get_pg_connection, init_pg_schema

# ------------------ Setup ------------------
# Ensure Postgres schema + tables exist at startup
init_pg_schema()

# In-memory active jobs (for schedule library)
scheduled_jobs = []

# ------------------ DB Operations ------------------
def load_schedules_from_db():
    """
    Load schedules from Postgres into memory at startup and schedule them
    using the `schedule` library. Only loads active schedules (not deleted ones).
    """
    global scheduled_jobs
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            # Only load active schedules (exclude deleted ones)
            cur.execute("""
                SELECT server_name, job_type, minutes, hour, minute
                FROM metrics_sync_tables.schedules
                WHERE status != 'deleted' OR status IS NULL
            """)
            rows = cur.fetchall()

            for row in rows:
                server_name, job_type, minutes, hour, minute = row

                if job_type == "interval" and minutes is not None:
                    schedule_interval_sync(server_name, minutes)
                elif job_type == "daily" and hour is not None and minute is not None:
                    schedule_daily_sync(server_name, hour, minute)

    except Exception as e:
        print(f"[load_schedules_from_db] Failed to load schedules: {e}")
    finally:
        conn.close()

# ------------------ Page Rendering ------------------
def see_schedule_page():
    """
    Return the HTML page to view all scheduled jobs.
    Accessible to all roles: admin, operator, viewer.
    """
    jobs = get_schedules()
    role = session.get("role")
    return render_template("see_schedule.html", jobs=jobs, role=role)

# ------------------ Schedule Management ------------------
def delete_schedule(server_name: str, job_type: str):
    """
    Delete a schedule completely from memory, DB, and the schedule library.
    This ensures the schedule will never come back.

    Args:
        server_name (str): The SQL server name
        job_type (str): Type of job (interval/daily)
    """
    import schedule as sched
    global scheduled_jobs

    # Remove from in-memory list
    scheduled_jobs = [
        job for job in scheduled_jobs
        if not (job["server"] == server_name and job["type"] == job_type)
    ]

    # Permanently delete from Postgres DB
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            # Hard delete - completely remove the record
            cur.execute("""
                DELETE FROM metrics_sync_tables.schedules
                WHERE server_name = %s AND job_type = %s
            """, (server_name, job_type))
            print(f"[DELETE_SCHEDULE] Deleted {cur.rowcount} schedule record(s) for {server_name}-{job_type}")
        conn.commit()
    finally:
        conn.close()

    # Clear scheduled jobs from `schedule` library with all possible tag formats
    try:
        # Try different tag formats that might be used
        tag_formats = [
            f"{server_name}-{job_type}",
            f"{server_name}:{job_type}", 
            f"{server_name}-interval" if job_type.startswith("interval") else f"{server_name}-daily",
            f"{server_name}_interval" if job_type.startswith("interval") else f"{server_name}_daily"
        ]
        
        for tag in tag_formats:
            sched.clear(tag)
            print(f"[DELETE_SCHEDULE] Cleared schedule library tag: {tag}")
        
        # Also clear any jobs that might match this server and job type
        all_jobs = sched.jobs[:]
        for job in all_jobs:
            if hasattr(job, 'tags') and any(tag in job.tags for tag in tag_formats):
                sched.cancel_job(job)
                print(f"[DELETE_SCHEDULE] Cancelled job with tags: {job.tags}")
                
    except Exception as e:
        print(f"[DELETE_SCHEDULE] Error clearing schedule library: {e}")
    
    print(f"[DELETE_SCHEDULE] Successfully deleted schedule {server_name}-{job_type} from all locations")

# ------------------ Interval Scheduling ------------------
def schedule_interval_sync(server_name, minutes):
    """
    Schedule a repeating interval job.

    Args:
        server_name (str)
        minutes (int)
    """
    global scheduled_jobs

    # Schedule in memory
    job = scheduler_utils.every(minutes).minutes.do(process_sql_server_hybrid, server_name).tag(f"{server_name}-interval")
    scheduled_jobs.append({
        "server": server_name,
        "type": "interval",
        "minutes": minutes,
        "job": job
    })

    # Save to Postgres
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO metrics_sync_tables.schedules (server_name, job_type, minutes)
                VALUES (%s, %s, %s)
                ON CONFLICT (server_name, job_type) DO UPDATE
                SET minutes = EXCLUDED.minutes
            """, (server_name, "interval", minutes))
        conn.commit()
    finally:
        conn.close()

# ------------------ Daily Scheduling ------------------
def schedule_daily_sync(server_name, hour, minute):
    """
    Schedule a daily job at a specific hour and minute.

    Args:
        server_name (str)
        hour (int)
        minute (int)
    """
    global scheduled_jobs

    # Schedule in memory
    job = scheduler_utils.every().day.at(f"{hour:02d}:{minute:02d}").do(process_sql_server_hybrid, server_name).tag(f"{server_name}-daily")
    scheduled_jobs.append({
        "server": server_name,
        "type": "daily",
        "hour": hour,
        "minute": minute,
        "job": job
    })

    # Save to Postgres
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO metrics_sync_tables.schedules (server_name, job_type, hour, minute)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (server_name, job_type) DO UPDATE
                SET hour = EXCLUDED.hour,
                    minute = EXCLUDED.minute
            """, (server_name, "daily", hour, minute))
        conn.commit()
    finally:
        conn.close()

# ------------------ Update Schedule ------------------
def update_schedule(server_name, job_type, **kwargs):
    """
    Update an existing schedule in memory, DB, and re-schedule it.

    Args:
        server_name (str)
        job_type (str): 'interval' or 'daily'
        kwargs: minutes / hour & minute
    """
    # First delete old schedule
    delete_schedule(server_name, job_type)

    # Then add new schedule
    if job_type == "interval":
        minutes = kwargs.get("minutes")
        if minutes is None:
            raise ValueError("Missing 'minutes' for interval schedule")
        schedule_interval_sync(server_name, minutes)
    elif job_type == "daily":
        hour = kwargs.get("hour")
        minute = kwargs.get("minute")
        if hour is None or minute is None:
            raise ValueError("Missing 'hour' or 'minute' for daily schedule")
        schedule_daily_sync(server_name, hour, minute)

# ------------------ Utilities ------------------
def get_schedules():
    """
    Return a list of all scheduled jobs (interval + daily)
    for display in HTML pages.
    """
    global scheduled_jobs
    display_jobs = []
    for job in scheduled_jobs:
        j = {
            "server": job["server"],
            "type": job["type"],
            "last_run": getattr(job["job"], "last_run", None),
            "status": getattr(job["job"], "status", None),
            "error": getattr(job["job"], "error", None)
        }
        if job["type"] == "interval":
            j["minutes"] = job.get("minutes")
        elif job["type"] == "daily":
            j["hour"] = job.get("hour")
            j["minute"] = job.get("minute")
        display_jobs.append(j)
    return display_jobs

# Load schedules from DB at startup
load_schedules_from_db()
