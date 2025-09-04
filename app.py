from flask import Flask, render_template, request, redirect, url_for, flash
from hybrid_sync import process_sql_server_hybrid
from scheduler import (
    schedule_interval_sync,
    schedule_daily_sync,
    clear_schedules,
    get_schedules,
)

from manage_server import load_config, save_config
from dashboard import get_last_10_syncs, get_last_sync_details, log_sync  # ADD log_sync

app = Flask(__name__)
app.secret_key = "supersecretkey"  # needed for flash messages


@app.route('/')
def index():
    """Homepage → show available servers and sync option"""
    config = load_config()
    sqlservers = config.get('sqlservers', {})
    return render_template('sync_servers.html', sqlservers=sqlservers)


@app.route('/sync/<server_name>')
def sync_server(server_name):
    """Run sync for the selected server"""
    config = load_config()
    server_conf = config['sqlservers'].get(server_name)
    if server_conf:
        try:
            process_sql_server_hybrid(server_name, server_conf)
            flash(f"✅ Sync completed for {server_name}", "success")
            log_sync(server_name, "success")   # ✅ LOG SUCCESS
        except Exception as e:
            flash(f"❌ Sync failed for {server_name}: {e}", "danger")
            log_sync(server_name, "failed", str(e))   # ✅ LOG FAILURE
    else:
        flash(f"Server {server_name} not found!", "danger")
    return redirect(url_for('index'))

@app.route('/add-server', methods=['GET', 'POST'])
def add_server():
    """Add new SQL Server to config"""
    if request.method == 'POST':
        server_name = request.form['server_name']
        server = request.form['server']
        username = request.form['username']
        password = request.form['password']
        port = int(request.form.get('port', 1433))

        config = load_config()
        config.setdefault('sqlservers', {})[server_name] = {
            'server': server,
            'username': username,
            'password': password,
            'port': port,
            'check_new_databases': True,
            'skip_databases': [],
            'sync_mode': 'hybrid'
        }
        save_config(config)
        flash(f"✅ Server {server_name} added!", "success")
        return redirect(url_for('index'))

    return render_template('add_sources.html')


@app.route('/schedule', methods=['GET', 'POST'])
def schedule_page():
    """Schedule sync jobs + view jobs"""
    config = load_config()
    servers = list(config.get("sqlservers", {}).keys())

    if request.method == 'POST':
        schedule_type = request.form.get('schedule_type')
        server_name = request.form.get('server_name')

        try:
            # clear_schedules()  # reset schedules before adding

            if schedule_type == 'interval':
                minutes = int(request.form.get('minutes'))
                schedule_interval_sync(server_name, minutes)

            elif schedule_type == 'daily':
                hour = int(request.form.get('hour'))
                minute = int(request.form.get('minute'))
                schedule_daily_sync(server_name, hour, minute)

            flash(f"✅ Schedule set successfully for {server_name}", "success")
        except Exception as e:
            flash(f"❌ Failed to set schedule: {e}", "danger")

        return redirect(url_for('schedule_page'))

    jobs = get_schedules()
    return render_template('schedule.html', servers=servers, jobs=jobs)

@app.route("/dashboard")
def dashboard():
    last_10 = get_last_10_syncs()
    last_detail = get_last_sync_details()
    return render_template("dashboard.html", last_10=last_10, last_detail=last_detail)
@app.route("/dashboard/data")
def dashboard_data():
    """Return sync history as JSON for auto-refresh"""
    return {
        "last_detail": get_last_sync_details(),
        "last_10": get_last_10_syncs(),
    }

if __name__ == '__main__':
    app.run(debug=True)
