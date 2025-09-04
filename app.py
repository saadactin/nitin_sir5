from flask import Flask, render_template, request, redirect, url_for, flash, session
from auth import create_user, authenticate_user, login_user, logout_user, require_role, init_admin_user
from hybrid_sync import process_sql_server_hybrid
from scheduler import schedule_interval_sync, schedule_daily_sync, get_schedules
from manage_server import load_config, save_config
from dashboard import get_last_10_syncs, get_last_sync_details, log_sync
from seeschedule import see_schedule_page

app = Flask(__name__)
app.secret_key = "supersecretkey"  # required for flash + sessions
init_admin_user()

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
@require_role(["admin"])  # Only admin can create users
def create_user_route():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]
        role = request.form["role"]
        create_user(username, password, role)
        flash(f"✅ User {username} created with role {role}", "success")
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


@app.route("/add-server", methods=["GET", "POST"])
@require_role(["admin", "operator"])
def add_server():
    """Add new SQL Server to config"""
    if request.method == "POST":
        server_name = request.form["server_name"]
        server = request.form["server"]
        username = request.form["username"]
        password = request.form["password"]
        port = int(request.form.get("port", 1433))

        config = load_config()
        config.setdefault("sqlservers", {})[server_name] = {
            "server": server,
            "username": username,
            "password": password,
            "port": port,
            "check_new_databases": True,
            "skip_databases": [],
            "sync_mode": "hybrid",
        }
        save_config(config)
        flash(f"✅ Server {server_name} added!", "success")
        return redirect(url_for("index"))

    return render_template("add_sources.html")

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



@app.route("/schedule", methods=["GET", "POST"])
@require_role(["admin", "operator"])
def schedule_page():
    """Schedule sync jobs + view jobs"""
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
            flash(f"✅ Schedule set successfully for {server_name}", "success")
        except Exception as e:
            flash(f"❌ Failed to set schedule: {e}", "danger")

        return redirect(url_for("schedule_page"))

    jobs = get_schedules()
    return render_template("schedule.html", servers=servers, jobs=jobs)


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


@app.route("/view-schedules")
@require_role(["admin", "operator", "viewer"])
def view_schedules():
    """Dedicated page for all users to see schedules"""
    return see_schedule_page()


# ------------------ MAIN ------------------
if __name__ == "__main__":
    app.run(debug=True)
