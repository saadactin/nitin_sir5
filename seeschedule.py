# seeschedule.py
from flask import render_template, session
from scheduler import get_schedules

def see_schedule_page():
    """
    Page to view all scheduled jobs.
    Accessible to all roles: admin, operator, viewer.
    """
    jobs = get_schedules()
    role = session.get("role")
    return render_template("see_schedule.html", jobs=jobs, role=role)
