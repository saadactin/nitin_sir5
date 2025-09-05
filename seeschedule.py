# seeschedule.py
import schedule
from flask import render_template, session

from hybrid_sync import process_sql_server_hybrid
from manage_server import load_config
from dashboard import log_sync
from db_utils import get_pg_connection, init_pg_schema
from scheduler import get_schedules, load_schedules_from_db

# ------------------ Setup ------------------
# Ensure Postgres schema + tables exist at startup
init_pg_schema()

# In-memory active jobs (for schedule library)
scheduled_jobs = []

# Load existing schedules from DB at startup
load_schedules_from_db()


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
    Delete a schedule from memory, DB, and the schedule library.

    Args:
        server_name (str): The SQL server name
        job_type (str): Type of job (interval/daily)
    """
    global scheduled_jobs

    # Remove from in-memory list
    scheduled_jobs = [
        job for job in scheduled_jobs
        if not (job["server"] == server_name and job["type"] == job_type)
    ]

    # Remove from Postgres DB
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM metrics_sync_tables.schedules
                WHERE server_name = %s AND job_type = %s
            """, (server_name, job_type))
        conn.commit()
    finally:
        conn.close()

    # Clear scheduled jobs from `schedule` library
    schedule.clear(tag=f"{server_name}-{job_type}")
