"""
Dashboard routes
"""
from flask import Blueprint, render_template, session
from auth import require_role
from dashboard import get_last_10_syncs, get_last_sync_details
from scheduler_utils import get_schedules

# Create blueprint
dashboard_bp = Blueprint('dashboard', __name__)


@dashboard_bp.route("/dashboard")
@require_role(["admin", "operator", "viewer"])
def dashboard():
    last_10 = get_last_10_syncs()
    last_detail = get_last_sync_details()
    jobs = get_schedules()  # schedules for display
    return render_template(
        "dashboard.html",
        last_10=last_10,
        last_detail=last_detail,
        jobs=jobs,
        role=session.get("role")
    )


@dashboard_bp.route("/dashboard/data")
@require_role(["admin", "operator", "viewer"])
def dashboard_data():
    """Return sync history as JSON for auto-refresh"""
    return {
        "last_detail": get_last_sync_details(),
        "last_10": get_last_10_syncs(),
    }

