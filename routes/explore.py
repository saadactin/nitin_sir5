"""
Explore route
"""
from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from routes.utils import load_config
from auth import require_role
from hybrid_sync import get_sql_connection, get_all_databases as hs_get_all_databases

# Create blueprint
explore_bp = Blueprint('explore', __name__)


@explore_bp.route("/explore", methods=["GET", "POST"])
@require_role(["admin", "operator", "viewer"])
def explore():
    try:
        config = load_config()
        servers = list(config.get("sqlservers", {}).keys())
        dbs = []
        selected_server = request.values.get("server") or (servers[0] if servers else None)
        if selected_server:
            try:
                server_conf = config["sqlservers"][selected_server]
                conn = get_sql_connection(server_conf)
                dbs = hs_get_all_databases(conn)
                conn.close()
            except Exception:
                dbs = []

        if request.method == "POST":
            action = request.form.get("action")
            server = request.form.get("server")
            db = request.form.get("db")
            table = request.form.get("table")
            if action == "history" and server and db:
                return redirect(url_for('analytics.sync_history', server=server, db=db))
            if action == "schema" and server and db:
                return redirect(url_for('analytics.schema_changes', server=server, db=db))
            if action == "resume" and server and db and table:
                return redirect(url_for('analytics.resume_sync', server=server, db=db, table=table))

        return render_template("explore.html", servers=servers, dbs=dbs, selected_server=selected_server, role=session.get("role"))
    except Exception as e:
        flash(f"Error loading explorer: {e}", "danger")
        return redirect(url_for("main.index"))

