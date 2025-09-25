from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify, Response
import json
import psycopg2
import yaml
import os
from flask import Flask, render_template, request, redirect, url_for, flash
from alerts import LogAnalyzer
from datetime import datetime
from auth import create_user, authenticate_user, login_user, logout_user, require_role, init_admin_user
from hybrid_sync import process_sql_server_hybrid
from manage_server import load_config, save_config
from dashboard import get_last_10_syncs, get_last_sync_details, log_sync
from seeschedule import see_schedule_page , delete_schedule 
from scheduler_utils import (
    schedule_interval_sync,
    schedule_daily_sync,
    delete_schedule,
    update_schedule,
    get_schedules
)
from analytics import compare_table_rows, delta_tracking, top_changed_tables
from metrics import get_server_metrics, get_database_metrics, get_sync_summary
from analytics_advanced import (
    fetch_database_history,
    fetch_table_history,
    detect_failed_syncs,
    generate_sync_report,
    resume_sync_table,
    partial_sync_preview,
    parse_schema_changes_from_log,
    collect_alerts,
)
from hybrid_sync import get_sql_connection
from hybrid_sync import get_all_databases as hs_get_all_databases
from hybrid_sync import (
    get_sqlalchemy_engine,
    get_pg_engine,
    should_skip_database,
    full_sync_database,
    incremental_sync_database,
    update_sync_status,
    cleanup_system_tables,
    get_sync_status,
    create_sync_tracking_table,
    create_table_sync_tracking,
)
app = Flask(__name__)
app.secret_key = "supersecretkey"  # required for flash + sessions
init_admin_user()

def require_role(allowed_roles):
    def decorator(f):
        def wrapper(*args, **kwargs):
            role = session.get("role")
            if role not in allowed_roles:
                flash("❌ Access denied", "danger")
                return redirect(url_for("view_schedules"))
            return f(*args, **kwargs)
        wrapper.__name__ = f.__name__
        return wrapper
    return decorator



# ------------------ AUTH ROUTES ------------------
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]
        role = authenticate_user(username, password)
        if role:
            login_user(username, role)
            flash(f"✅ Logged in as {role}", "success")
            return redirect(url_for("index"))
        else:
            flash("❌ Invalid credentials", "danger")
    return render_template("login.html")


@app.route("/logout")
def logout():
    logout_user()
    flash("✅ Logged out", "info")
    return redirect(url_for("login"))



@app.route("/create-user", methods=["GET", "POST"])
@require_role(["admin"])
def create_user_route():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]
        role = request.form["role"]
        
        if create_user(username, password, role):
            flash(f"User {username} created with role {role}", "success")
        else:
            flash(f"User {username} already exists or creation failed", "warning")
            
        return redirect(url_for("index"))
    
    return render_template("create_user.html")
# ------------------ PROTECTED ROUTES ------------------
@app.route("/")
@require_role(["admin", "operator", "viewer"])
def index():
    """Homepage → show available servers and sync option"""
    config = load_config()
    sqlservers = config.get("sqlservers", {})
    role = session.get("role")  # make sure this is aligned with the above line
    return render_template("sync_servers.html", sqlservers=sqlservers, role=role)


@app.route("/server/<server_name>")
@require_role(["admin", "operator", "viewer"])
def view_server_databases(server_name):
    """Show databases for a server and allow selecting subset to sync."""
    try:
        config = load_config()
        server_conf = config["sqlservers"].get(server_name)
        if not server_conf:
            flash("Server not found", "danger")
            return redirect(url_for("index"))
        conn = get_sql_connection(server_conf)
        dbs = hs_get_all_databases(conn)
        conn.close()
        return render_template("server_databases.html", server_name=server_name, databases=dbs, role=session.get("role"))
    except Exception as e:
        flash(f"❌ Failed to load databases: {e}", "danger")
        return redirect(url_for("index"))


@app.route("/sync-selected/<server_name>", methods=["POST"])
@require_role(["admin", "operator"])
def sync_selected_databases(server_name):
    """Sync only selected databases for a server (incremental)."""
    try:
        selected = [d.strip() for d in request.form.getlist("databases") if d.strip()]
        if not selected:
            flash("No databases selected.", "warning")
            return redirect(url_for("view_server_databases", server_name=server_name))

        config = load_config()
        server_conf = config["sqlservers"].get(server_name)
        if not server_conf:
            flash("Server not found", "danger")
            return redirect(url_for("index"))

        # Validate requested databases actually exist on the server
        try:
            test_conn = get_sql_connection(server_conf)
            existing_dbs = set(hs_get_all_databases(test_conn))
            test_conn.close()
        except Exception as e:
            flash(f"❌ Could not read databases from server: {e}", "danger")
            return redirect(url_for("view_server_databases", server_name=server_name))

        selected = [d for d in selected if d in existing_dbs]
        if not selected:
            flash("No valid databases selected (not found on server).", "warning")
            return redirect(url_for("view_server_databases", server_name=server_name))

        # Prepare engines and ensure tracking tables exist
        pg_engine = get_pg_engine(server_conf.get("target_postgres_db"))
        create_sync_tracking_table(pg_engine)
        create_table_sync_tracking(pg_engine)
        server_clean = ''.join(c for c in server_conf['server'] if c.isalnum() or c in '_-')

        processed_summary = []
        for db_name in selected:
            if should_skip_database(db_name, server_conf):
                continue
            sql_engine = get_sqlalchemy_engine(server_conf, db_name)
            db_conn = get_sql_connection(server_conf, db_name)
            try:
                # Cleanup reserved system tables in target schema
                schema_name = f"{server_clean}_{db_name}".replace('-', '_').replace(' ', '_')
                cleanup_system_tables(pg_engine, schema_name)
                # Decide full vs incremental based on status
                status = get_sync_status(pg_engine, server_conf['server'], db_name)
                if status is None:
                    try:
                        count = full_sync_database(sql_engine, db_name, server_conf, server_clean, None, pg_engine)
                        update_sync_status(pg_engine, server_conf['server'], db_name, 'full', 'COMPLETED')
                        processed_summary.append(f"{db_name}: full({count})")
                    except Exception as e:
                        processed_summary.append(f"{db_name}: full(ERROR {e})")
                else:
                    try:
                        count = incremental_sync_database(sql_engine, db_conn, db_name, server_conf, server_clean, None, pg_engine)
                        update_sync_status(pg_engine, server_conf['server'], db_name, 'incremental', 'COMPLETED')
                        processed_summary.append(f"{db_name}: incr({count})")
                    except Exception as e:
                        processed_summary.append(f"{db_name}: incr(ERROR {e})")
            finally:
                db_conn.close()
                sql_engine.dispose()

        flash(f"✅ Sync completed: {'; '.join(processed_summary)}", "success")
        return redirect(url_for("view_server_databases", server_name=server_name))
    except Exception as e:
        flash(f"❌ Failed to sync selected: {e}", "danger")
        return redirect(url_for("view_server_databases", server_name=server_name))


@app.route("/sync/<server_name>")
@require_role(["admin", "operator"])
def sync_server(server_name):
    """Run sync for the selected server"""
    config = load_config()
    server_conf = config["sqlservers"].get(server_name)
    if server_conf:
        try:
            process_sql_server_hybrid(server_name, server_conf)
            flash(f"✅ Sync completed for {server_name}", "success")
            log_sync(server_name, "success")
        except Exception as e:
            flash(f"❌ Sync failed for {server_name}: {e}", "danger")
            log_sync(server_name, "failed", str(e))
    else:
        flash(f"Server {server_name} not found!", "danger")
    return redirect(url_for("index"))

CONFIG_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "config/db_connections.yaml")
)

def load_config():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

def load_pg_databases():
    """Return list of Postgres DBs, or [] if connection fails"""
    try:
        config = load_config()
        pg_conf = config.get("postgresql", {})

        conn = psycopg2.connect(
            dbname="postgres",  
            user=pg_conf.get("username"),
            password=pg_conf.get("password"),
            host=pg_conf.get("host"),
            port=pg_conf.get("port", 5432),
        )
        cur = conn.cursor()
        cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        dbs = [row[0] for row in cur.fetchall()]
        conn.close()
        return dbs
    except Exception as e:
        print(f"⚠️ Could not load Postgres DBs: {e}")
        return []


@app.route("/add-server", methods=["GET", "POST"])
@require_role(["admin", "operator"])
def add_server():
    if request.method == "POST":
        server_name = request.form["server_name"]
        server = request.form["server"]
        username = request.form["username"]
        password = request.form["password"]
        port = int(request.form.get("port", 1433))
        pg_database = request.form.get("pg_database")

        config = load_config()
        config.setdefault("sqlservers", {})
        if server_name in config["sqlservers"]:
            flash(f"❌ Server {server_name} already exists!", "error")
            return redirect(url_for("add_server"))

        config["sqlservers"][server_name] = {
            "server": server,
            "username": username,
            "password": password,
            "port": port,
            "check_new_databases": True,
            "skip_databases": [],
            "sync_mode": "hybrid",
            "target_postgres_db": pg_database,   # store selection
        }
        save_config(config)

        flash(f"✅ Server {server_name} added with Postgres target {pg_database}", "success")
        return redirect(url_for("index"))

    # ---- GET request ----
    postgres_dbs = load_pg_databases()   # <--- here you call it
    return render_template("add_sources.html", postgres_dbs=postgres_dbs)


@app.route("/delete-server/<server_name>", methods=["POST"])
@require_role(["admin", "operator"])
def delete_server_route(server_name):
    """Delete a SQL Server from config"""
    from manage_server import delete_server  # import here to avoid circular import

    try:
        delete_server(server_name)
        flash(f"✅ Server {server_name} deleted!", "success")
    except Exception as e:
        flash(f"❌ Failed to delete server: {e}", "danger")
    return redirect(url_for("index"))





@app.route("/dashboard")
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


@app.route("/dashboard/data")
@require_role(["admin", "operator", "viewer"])
def dashboard_data():
    """Return sync history as JSON for auto-refresh"""
    return {
        "last_detail": get_last_sync_details(),
        "last_10": get_last_10_syncs(),
    }
# ------------------ Schedule Routes ------------------

@app.route("/schedule", methods=["GET", "POST"])
@require_role(["admin", "operator"])
def schedule_page():
    """Create a new schedule"""
    config = load_config()
    servers = list(config.get("sqlservers", {}).keys())

    if request.method == "POST":
        schedule_type = request.form.get("schedule_type")
        server_name = request.form.get("server_name")
        try:
            if schedule_type == "interval":
                minutes = int(request.form.get("minutes"))
                schedule_interval_sync(server_name, minutes)
            elif schedule_type == "daily":
                hour = int(request.form.get("hour"))
                minute = int(request.form.get("minute"))
                schedule_daily_sync(server_name, hour, minute)
            flash(f"✅ Schedule set for {server_name}", "success")
        except Exception as e:
            flash(f"❌ Failed to set schedule: {e}", "danger")
        return redirect(url_for("schedule_page"))

    jobs = get_schedules()
    return render_template("schedule.html", servers=servers, jobs=jobs, role=session.get("role"))

@app.route("/view-schedules")
@require_role(["admin", "operator", "viewer"])
def view_schedules():
    """View all schedules"""
    jobs = get_schedules()
    return render_template("see_schedule.html", jobs=jobs, role=session.get("role"))

@app.route("/edit-schedule/<server_name>/<job_type>", methods=["GET", "POST"])
@require_role(["admin", "operator"])
def edit_schedule_page(server_name, job_type):
    """Edit an existing schedule"""
    jobs = get_schedules()
    job = next((j for j in jobs if j["server"] == server_name and j["type"] == job_type), None)
    if not job:
        flash(f"❌ Schedule not found", "danger")
        return redirect(url_for("view_schedules"))

    if request.method == "POST":
        try:
            if job_type.startswith("interval"):
                minutes = int(request.form.get("minutes"))
                update_schedule(server_name, job_type, minutes=minutes)
            elif job_type.startswith("daily"):
                hour = int(request.form.get("hour"))
                minute = int(request.form.get("minute"))
                update_schedule(server_name, job_type, hour=hour, minute=minute)
            flash(f"✅ Schedule updated for {server_name}", "success")
            return redirect(url_for("view_schedules"))
        except Exception as e:
            flash(f"❌ Failed to update schedule: {e}", "danger")

    return render_template("edit_schedule.html", job=job)

@app.route("/delete-schedule/<server_name>/<job_type>", methods=["POST"])
@require_role(["admin", "operator"])
def delete_schedule_route(server_name, job_type):
    """Delete a schedule"""
    try:
        delete_schedule(server_name, job_type)
        flash(f"✅ Schedule deleted for {server_name}", "success")
    except Exception as e:
        flash(f"❌ Failed to delete schedule: {e}", "danger")
    return redirect(url_for("view_schedules"))

# ------------------ ANALYTICS ROUTES ------------------

@app.route("/compare/<server>/<db>/<table>")
@require_role(["admin", "operator", "viewer"])
def compare_table(server, db, table):
    """Compare source vs destination rows for a table"""
    try:
        comparison = compare_table_rows(server, db, table)
        delta_info = delta_tracking(server, db, table)
        
        return render_template("compare_table.html", 
                             server=server, 
                             db=db, 
                             table=table,
                             comparison=comparison,
                             delta_info=delta_info,
                             role=session.get("role"))
    except Exception as e:
        flash(f"❌ Error comparing table {table}: {e}", "danger")
        return redirect(url_for("index"))


@app.route("/top-changed/<server>/<db>")
@require_role(["admin", "operator", "viewer"])
def top_changed(server, db):
    """Show top changed tables for a database"""
    try:
        changed_tables = top_changed_tables(server, db)
        
        return render_template("top_changed.html", 
                             server=server, 
                             db=db,
                             changed_tables=changed_tables,
                             role=session.get("role"))
    except Exception as e:
        flash(f"❌ Error getting top changed tables: {e}", "danger")
        return redirect(url_for("index"))


# -----------------------------Alerts---------------------------------
@app.route("/alerts")
@require_role(["admin", "operator", "viewer"])
def alerts():
    """Show system alerts and notifications"""
    try:
        # Collect alerts from various sources
        alert_data = collect_alerts()
        return render_template("alerts.html", 
                             alerts=alert_data.get("alerts", []),
                             warnings=alert_data.get("warnings", []),
                             infos=alert_data.get("infos", []),
                             role=session.get("role"))
    except Exception as e:
        flash(f"❌ Error loading alerts: {e}", "danger")
        return redirect(url_for("index"))

@app.route("/logs")
@require_role(["admin", "operator", "viewer"])
def view_logs():
    """View and analyze log files with pagination"""
    log_file = 'load_postgres.log'
    if not os.path.exists(log_file):
        flash(f"❌ Log file '{log_file}' not found", "danger")
        return render_template(
            "logs.html",
            alerts=[],
            warnings=[],
            infos=[],
            log_exists=False,
            alerts_total=0,
            warnings_total=0,
            infos_total=0,
            errors_page=1,
            warnings_page=1,
            info_page=1,
            per_page=10
        )

    analyzer = LogAnalyzer(log_file)
    analyzer.parse_logs()

    # Pagination settings
    per_page = 10
    warnings_page = int(request.args.get("warnings_page", 1))
    errors_page = int(request.args.get("errors_page", 1))
    info_page = int(request.args.get("info_page", 1))

    # Reverse to show latest first
    warnings = analyzer.warnings[::-1]
    alerts = analyzer.alerts[::-1]
    infos = analyzer.infos[::-1]

    # Slice logs for current page
    warnings_paginated = warnings[(warnings_page-1)*per_page : warnings_page*per_page]
    alerts_paginated = alerts[(errors_page-1)*per_page : errors_page*per_page]
    infos_paginated = infos[(info_page-1)*per_page : info_page*per_page]

    return render_template(
        "logs.html",
        alerts=alerts_paginated,
        warnings=warnings_paginated,
        infos=infos_paginated,
        warnings_page=warnings_page,
        errors_page=errors_page,
        info_page=info_page,
        alerts_total=len(alerts),
        warnings_total=len(warnings),
        infos_total=len(infos),
        per_page=per_page,
        log_exists=True,
        role=session.get("role")
    )

@app.route("/logs/generate-report")
@require_role(["admin", "operator"])
def generate_log_report():
    """Generate HTML report from logs"""
    log_file = 'load_postgres.log'
    if not os.path.exists(log_file):
        flash(f"❌ Log file '{log_file}' not found", "danger")
        return redirect(url_for("view_logs"))
    
    try:
        analyzer = LogAnalyzer(log_file)
        output_file = f"alerts_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        analyzer.generate_html_report(output_file)
        flash(f"✅ HTML report generated: {output_file}", "success")
    except Exception as e:
        flash(f"❌ Error generating report: {e}", "danger")
    
    return redirect(url_for("view_logs"))

@app.route("/logs/download")
@require_role(["admin", "operator", "viewer"])
def download_logs():
    """Download raw log file"""
    log_file = 'load_postgres.log'
    if not os.path.exists(log_file):
        flash(f"❌ Log file '{log_file}' not found", "danger")
        return redirect(url_for("view_logs"))
    
    from flask import send_file
    return send_file(log_file, as_attachment=True, download_name="postgres_sync_log.log")



# ------------------ METRICS ROUTES ------------------

@app.route("/metrics/<server>")
@require_role(["admin", "operator", "viewer"])
def server_metrics(server):
    """Show metrics for all tables in a server"""
    try:
        metrics = get_server_metrics(server)
        return render_template("server_metrics.html", 
                             server=server,
                             metrics=metrics,
                             role=session.get("role"))
    except Exception as e:
        flash(f"❌ Error getting server metrics: {e}", "danger")
        return redirect(url_for("index"))


@app.route("/metrics/<server>.json")
@require_role(["admin", "operator", "viewer"])
def server_metrics_json(server):
    """Return server metrics as JSON"""
    try:
        metrics = get_server_metrics(server)
        return jsonify(metrics)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/metrics/<server>/<db>")
@require_role(["admin", "operator", "viewer"])
def database_metrics(server, db):
    """Show metrics for tables in a single database"""
    try:
        metrics = get_database_metrics(server, db)
        return render_template("database_metrics.html", 
                             server=server,
                             db=db,
                             metrics=metrics,
                             role=session.get("role"))
    except Exception as e:
        flash(f"❌ Error getting database metrics: {e}", "danger")
        return redirect(url_for("index"))


@app.route("/metrics/<server>/<db>.json")
@require_role(["admin", "operator", "viewer"])
def database_metrics_json(server, db):
    """Return database metrics as JSON"""
    try:
        metrics = get_database_metrics(server, db)
        return jsonify(metrics)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/sync-summary")
@require_role(["admin", "operator", "viewer"])
def sync_summary():
    """Show overall sync summary across all servers"""
    try:
        summary = get_sync_summary()
        return render_template("sync_summary.html", 
                             summary=summary,
                             role=session.get("role"))
    except Exception as e:
        flash(f"❌ Error getting sync summary: {e}", "danger")
        return redirect(url_for("index"))


@app.route("/sync-summary.json")
@require_role(["admin", "operator", "viewer"])
def sync_summary_json():
    """Return sync summary as JSON"""
    try:
        summary = get_sync_summary()
        payload = json.dumps(summary, indent=2)
        return Response(payload, mimetype='application/json', headers={
            'Content-Disposition': 'attachment; filename=sync_summary.json'
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ------------------ ADVANCED ANALYTICS ROUTES ------------------

@app.route("/sync-history/<server>/<db>")
@require_role(["admin", "operator", "viewer"])
def sync_history(server, db):
    try:
        db_hist = fetch_database_history(server, db, limit=100)
        tbl_hist = fetch_table_history(server, db, limit=500)
        failed = detect_failed_syncs(server, db)

        # CSV/XLSX download request via query param
        export = request.args.get("export")
        if export in ("csv", "xlsx"):
            buf, mimetype, filename = generate_sync_report(db_hist + tbl_hist, fmt=export)
            from flask import send_file
            return send_file(buf, mimetype=mimetype, as_attachment=True, download_name=filename)

        return render_template("sync_history.html", server=server, db=db, db_hist=db_hist, tbl_hist=tbl_hist, failed=failed, role=session.get("role"))
    except Exception as e:
        flash(f"❌ Error loading sync history: {e}", "danger")
        return redirect(url_for("index"))


@app.route("/sync-history/<server>/<db>.json")
@require_role(["admin", "operator", "viewer"])
def sync_history_json(server, db):
    try:
        db_hist = fetch_database_history(server, db, limit=100)
        tbl_hist = fetch_table_history(server, db, limit=500)
        failed = detect_failed_syncs(server, db)
        return jsonify({"database": db_hist, "tables": tbl_hist, "failed": failed})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/resume-sync/<server>/<db>/<table>", methods=["GET", "POST"])
@require_role(["admin", "operator", "viewer"])
def resume_sync(server, db, table):
    try:
        info = None
        preview = None
        if request.method == "POST":
            # Optional preview before resume
            columns = request.form.get("columns") or ""
            filter_sql = request.form.get("filter_sql") or ""
            columns_list = [c.strip() for c in columns.split(',') if c.strip()] if columns else None
            if request.form.get("action") == "preview":
                preview = partial_sync_preview(server, db, table, columns_list, filter_sql)
            else:
                info = resume_sync_table(server, db, table)
                flash("✅ Resume requested. The next incremental run will continue from last PK.", "success")

        return render_template("resume_sync.html", server=server, db=db, table=table, info=info, preview=preview, role=session.get("role"))
    except Exception as e:
        flash(f"❌ Error preparing resume: {e}", "danger")
        return redirect(url_for("index"))


@app.route("/schema-changes/<server>/<db>")
@require_role(["admin", "operator", "viewer"])
def schema_changes(server, db):
    try:
        # Parse from log file; optionally filter client-side in template
        events = parse_schema_changes_from_log()
        return render_template("schema_changes.html", server=server, db=db, events=events, role=session.get("role"))
    except Exception as e:
        flash(f"❌ Error loading schema changes: {e}", "danger")
        return redirect(url_for("index"))


@app.route("/schema-changes/<server>/<db>.json")
@require_role(["admin", "operator", "viewer"])
def schema_changes_json(server, db):
    try:
        events = parse_schema_changes_from_log()
        return jsonify({"events": events})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ------------------ Explore (selector) ------------------
@app.route("/explore", methods=["GET", "POST"])
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
                return redirect(url_for('sync_history', server=server, db=db))
            if action == "schema" and server and db:
                return redirect(url_for('schema_changes', server=server, db=db))
            if action == "resume" and server and db and table:
                return redirect(url_for('resume_sync', server=server, db=db, table=table))

        return render_template("explore.html", servers=servers, dbs=dbs, selected_server=selected_server, role=session.get("role"))
    except Exception as e:
        flash(f"❌ Error loading explorer: {e}", "danger")
        return redirect(url_for("index"))

# ------------------ MAIN ------------------
if __name__ == "__main__":
    app.run(debug=True)
