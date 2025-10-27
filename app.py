from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify, Response
import pandas as pd
from werkzeug.utils import secure_filename
import json
import psycopg2
import yaml
import os
import logging
# Load environment variables from .env; also try email.env fallback
try:
    from dotenv import load_dotenv
    load_dotenv()
    try:
        load_dotenv("email.env")
    except Exception:
        pass
except Exception:
    # python-dotenv is optional in production; if not installed, rely on real environment
    pass
import sys
import signal
import atexit
# ...existing imports above...
from alerts import LogAnalyzer
from datetime import datetime
import threading
from auth import create_user, authenticate_user, login_user, logout_user, require_role, init_admin_user
from connection_sync import sync_yaml_to_db, sync_db_to_yaml, update_connection, remove_connection
from hybrid_sync import process_sql_server_hybrid
from manage_server import load_config, save_config
from dashboard import get_last_10_syncs, get_last_sync_details, log_sync, get_last_sync_for_server
from seeschedule import see_schedule_page , delete_schedule 
from scheduler_utils import (
    schedule_interval_sync,
    schedule_daily_sync,
    delete_schedule,
    update_schedule,
    get_schedules
)
from analytics import compare_table_rows, delta_tracking, top_changed_tables
from metrics import get_server_metrics, get_database_metrics
from sync_summary import get_sync_comparison, get_all_server_comparisons, get_individual_server_comparison, get_detailed_table_comparison, bp as sync_summary_bp
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
from sync_manager import sync_manager
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
from utils.email_service import email_service

# Global flag to track if shutdown email was sent
_shutdown_email_sent = False
_shutdown_in_progress = False

def send_shutdown_email(reason="Manual shutdown"):
    """Send email notification when Flask server is shutting down"""
    global _shutdown_email_sent
    
    # Only send once
    if _shutdown_email_sent:
        return
    
    _shutdown_email_sent = True  # Set immediately to prevent duplicates
    
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        shutdown_message = f"""Flask Application Server is shutting down.

Timestamp: {timestamp}
Shutdown Reason: {reason}
Server URL: http://127.0.0.1:5001

The Flask server has been stopped.

Possible reasons:
- Manual shutdown (Ctrl+C pressed)
- Application error or crash
- System shutdown
- Process terminated

Action Required: Restart the Flask application server if this was not intentional.

To restart:
1. Open PowerShell in project directory
2. Run: .\\myenv1\\Scripts\\python.exe app.py
"""
        
        print(f"\n[SHUTDOWN EMAIL] Sending shutdown notification...")
        result = email_service.notify_server_down(
            server_name="Flask Application Server",
            error_message=shutdown_message
        )
        
        if result.success:
            print(f"[SHUTDOWN EMAIL] ✓ Alert sent successfully to {len(result.recipients)} recipients")
        else:
            print(f"[SHUTDOWN EMAIL] ✗ Failed to send alert: {result.error}")
            
    except Exception as e:
        print(f"[SHUTDOWN EMAIL] Exception: {e}")

def signal_handler(sig, frame):
    """Handle Ctrl+C and other termination signals"""
    global _shutdown_in_progress
    
    # Prevent multiple signal handlers from running
    if _shutdown_in_progress:
        return
    
    _shutdown_in_progress = True
    
    print("\n" + "="*60)
    print("[SHUTDOWN] Shutdown signal received (Ctrl+C)")
    print("="*60)
    
    send_shutdown_email("Manual shutdown (Ctrl+C)")
    
    print("[SHUTDOWN] Cleanup complete. Exiting...")
    print("="*60)
    
    sys.exit(0)

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
signal.signal(signal.SIGTERM, signal_handler)  # Termination signal

def test_sql_connection(server_conf):
    """Test if a SQL Server connection is valid
    
    Args:
        server_conf: SQL server configuration dictionary
        
    Returns:
        tuple: (success, error_message)
    """
    try:
        # Log the connection attempt details
        server = server_conf.get('server', '')
        app.logger.info(f"Testing connection to SQL Server: {server}")
        
        if '\\' in server:
            app.logger.info(f"Detected named instance format. Will use SQL Browser for port resolution.")
        
        # Attempt to connect
        conn = get_sql_connection(server_conf)
        
        # Execute a simple query to verify the connection
        cursor = conn.cursor()
        cursor.execute("SELECT @@SERVERNAME, @@VERSION")
        server_info = cursor.fetchone()
        app.logger.info(f"Connected successfully to {server_info[0]}")
        cursor.close()
        conn.close()
        return True, None
    except Exception as e:
        error_message = str(e)
        app.logger.error(f"Connection test failed: {error_message}")
        
        # Enhanced error messages for common issues
        user_message = error_message
        
        if "Error Locating Server/Instance Specified" in error_message:
            user_message = (
                f"Could not locate SQL Server instance '{server_conf.get('server', '')}'. "
                f"Check that the instance name is correct and SQL Browser service is running. "
                f"For named instances (server\\instance), ensure SQL Browser service is enabled and UDP port 1434 is open."
            )
        elif "Login timeout expired" in error_message:
            user_message = (
                f"Connection timeout to '{server_conf.get('server', '')}'. "
                f"Check if server is reachable and firewall allows connection."
            )
        elif "Login failed for user" in error_message:
            user_message = (
                f"Authentication failed. Check username and password. "
                f"If using Windows Authentication, ensure your account has access."
            )
        
        return False, user_message

# Configure logging for better visibility
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('app.log')
    ]
)

app = Flask(__name__)

# Register blueprints
app.register_blueprint(sync_summary_bp, url_prefix='/sync-summary')

@app.route('/sync-summary/<server_name>/tables')
def server_tables(server_name):
    """Direct route for server tables view for backward compatibility"""
    config = load_config()
    sqlservers = config.get("sqlservers", {})
    
    if server_name not in sqlservers:
        flash(f"Server '{server_name}' not found", "error")
        return redirect(url_for('index'))
    
    target_db = sqlservers[server_name].get('target_postgres_db')
    if not target_db:
        flash(f"No target database configured for server '{server_name}'", "error")
        return redirect(url_for('index'))
    
    return render_template( 
        'server_tables.html',
        server_name=server_name,
        target_db=target_db
    )

# Prefer environment-provided secret key. If missing, warn but keep compatibility.
env_secret = os.environ.get('SECRET_KEY') or os.environ.get('SECRET')
if env_secret:
    app.secret_key = env_secret
else:
    # Fallback to older default to avoid breaking existing installs, but log strongly
    app.secret_key = os.environ.get('SECRET_KEY', 'your-secret-key-change-in-production-2025')
    app.logger.warning('No SECRET_KEY found in environment; using fallback secret. Set SECRET_KEY in .env for production.')

# Session configuration for security
app.config['SESSION_COOKIE_HTTPONLY'] = True
# Respect environment to enable secure cookies in production
app.config['SESSION_COOKIE_SECURE'] = os.environ.get('SESSION_COOKIE_SECURE', '0') in ('1', 'true', 'True')
app.config['SESSION_COOKIE_SAMESITE'] = os.environ.get('SESSION_COOKIE_SAMESITE', 'Lax')
app.config['PERMANENT_SESSION_LIFETIME'] = int(os.environ.get('PERMANENT_SESSION_LIFETIME', '3600'))  # 1 hour
app.config['SESSION_REFRESH_EACH_REQUEST'] = True

# Server restart detection - invalidate old sessions
SERVER_START_TIME = datetime.now().timestamp()

# Enable Flask request logging
app.logger.setLevel(logging.INFO)
logging.getLogger('werkzeug').setLevel(logging.INFO)

# If the hybrid sync simple terminal mode is enabled, reduce console noise from Flask/werkzeug
if os.environ.get('HYBRID_SYNC_SIMPLE_TERMINAL', '1').lower() in ('1', 'true', 'yes'):
    # Lower werkzeug console logs to WARNING so only important messages show on the terminal
    logging.getLogger('werkzeug').setLevel(logging.WARNING)
    # Also set the root stream handler to WARNING so app-level INFO logs go to file only
    for h in logging.getLogger().handlers:
        if isinstance(h, logging.StreamHandler):
            h.setLevel(logging.WARNING)

# Initialize database schema and sync YAML configuration to database
logging.info("Initializing connection database and syncing with YAML config...")
if sync_yaml_to_db():
    logging.info("Successfully synced YAML configuration to database")
else:
    logging.warning("Failed to sync YAML configuration to database, using YAML file as fallback")

# Add request logging middleware
@app.before_request
def log_request_info():
    app.logger.info(f'[REQ] {request.method} {request.path} - {request.remote_addr}')

@app.before_request
def check_authentication():
    """Global authentication check for all requests - CANNOT BE BYPASSED"""
    # List of endpoints that don't require authentication
    public_endpoints = ['login', 'static']
    
    # Skip authentication check for public endpoints and static files
    if request.endpoint in public_endpoints or request.path.startswith('/static/'):
        return
    
    # Debug logging - avoid logging flash contents (may contain Unicode/emoji)
    # Log only key session attributes to prevent UnicodeEncodeError when console encoding is limited
    app.logger.info(f'[DEBUG] Checking auth for {request.path} - username={session.get("username")}, role={session.get("role")}, ip={session.get("session_ip")}')
    
    # Check if session is valid and not from previous server instance
    session_start_time = session.get('session_start_time')
    if session_start_time and session_start_time < SERVER_START_TIME:
        # Session is from previous server instance, clear it
        session.clear()
        app.logger.info('[AUTH] Cleared old session from previous server instance')
    
    # MANDATORY authentication check - NO BYPASS ALLOWED
    if 'role' not in session or 'username' not in session:
        app.logger.warning(f'[AUTH] BLOCKED unauthenticated access to {request.path} from {request.remote_addr}')
        app.logger.warning(f'[AUTH] Session contents: {dict(session)}')
        # Clear any existing session data to be safe
        session.clear()
        flash('Please log in to access this page', 'warning')
        return redirect(url_for('login'), code=302)
    
    # Additional validation - check if session values are actually valid
    username = session.get('username')
    role = session.get('role')
    if not username or not role or role not in ['admin', 'operator', 'viewer']:
        app.logger.warning(f'[AUTH] Invalid session data - username: {username}, role: {role}')
        session.clear()
        flash('Invalid session. Please log in again.', 'warning')
        return redirect(url_for('login'), code=302)
    
    # Log successful authentication
    app.logger.info(f'[AUTH] Authenticated access: {username} ({role}) to {request.path}')

@app.after_request
def log_response_info(response):
    app.logger.info(f'[RESP] {request.method} {request.path} - {response.status_code}')
    return response

# Custom error handlers
@app.errorhandler(404)
def not_found_error(error):
    """Handle 404 errors with a friendly page"""
    app.logger.warning(f'[404] Page not found: {request.path} - {request.remote_addr}')
    return render_template('404.html'), 404

# Conditionally create default admin at import time when explicitly enabled via env
try:
    if os.environ.get('CREATE_DEFAULT_ADMIN', '0') in ('1', 'true', 'True'):
        init_admin_user(create_if_missing=True, default_password=os.environ.get('DEFAULT_ADMIN_PASSWORD'))
except Exception:
    app.logger.info('Default admin creation skipped or failed at import-time')



# ------------------ AUTH ROUTES ------------------
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]
        role = authenticate_user(username, password)
        if role:
            # Set session data with timestamp and IP
            session["username"] = username
            session["role"] = role
            session["session_start_time"] = datetime.now().timestamp()
            session["session_ip"] = request.remote_addr
            session.permanent = True
            flash(f"Welcome, {username}!", "success")
            app.logger.info(f"[LOGIN] Successful login: {username} ({role}) from {request.remote_addr}")
            return redirect(url_for("index"))
        else:
            flash("Invalid credentials", "danger")
            app.logger.warning(f"[LOGIN] Failed login attempt: {username} from {request.remote_addr}")
    return render_template("login.html")


@app.route("/logout")
def logout():
    username = session.get('username', 'Unknown')
    session.clear()  # Clear all session data
    flash("You have been logged out successfully.", "info")
    app.logger.info(f"[LOGOUT] User logged out: {username}")
    return redirect(url_for("login"))

@app.route("/force-logout")
def force_logout():
    """Force logout - clears ALL session data"""
    app.logger.info(f"[FORCE-LOGOUT] Clearing session: {dict(session)}")
    session.clear()
    session.modified = True
    flash("Session forcefully cleared. Please log in.", "warning")
    return redirect(url_for("login"))



@app.route("/create-user", methods=["GET", "POST"])
@require_role(["admin"])
def create_user_route():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]
        role = request.form["role"]
        
        # Get the current user who is creating this user
        created_by = session.get("username", "admin")
        
        if create_user(username, password, role, created_by=created_by):
            flash(f"User {username} created with role {role}. Notification email sent.", "success")
        else:
            flash(f"User {username} already exists or creation failed", "warning")
            
        return redirect(url_for("index"))
    
    return render_template("create_user.html")
# ------------------ PROTECTED ROUTES ------------------
@app.route("/")
@require_role(["admin", "operator", "viewer"])
def index():
    """Homepage → show available servers and sync option"""
    app.logger.info(f"[HOME] Homepage accessed by user: {session.get('username', 'Unknown')}")
    config = load_config()
    sqlservers = config.get("sqlservers", {})
    
    # Check connection status for each server
    server_statuses = {}
    for server_name, server_conf in sqlservers.items():
        success, error = test_sql_connection(server_conf)
        server_statuses[server_name] = {
            "online": success,
            "error": error
        }
    
    role = session.get("role")
    app.logger.info(f"[INFO] Loaded {len(sqlservers)} SQL servers for display")
    return render_template("sync_servers.html", sqlservers=sqlservers, server_statuses=server_statuses, role=role)


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
        flash(f"Failed to load databases: {e}", "danger")
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
            flash(f"Could not read databases from server: {e}", "danger")
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
        # outer try completed successfully
        flash(f"Sync completed: {'; '.join(processed_summary)}", "success")
        return redirect(url_for("view_server_databases", server_name=server_name))
    except Exception as e:
        flash(f"Failed to sync selected: {e}", "danger")
        return redirect(url_for("view_server_databases", server_name=server_name))


@app.route("/sync/<server_name>")
@require_role(["admin", "operator"])
def sync_server(server_name):
    """Run sync for the selected server"""
    app.logger.info(f"[SYNC START] Starting sync operation for server: {server_name}")
    app.logger.info(f"[SYNC STARTED] {server_name} at {datetime.now().strftime('%H:%M:%S')}")
    
    # Log an in-progress entry so manual/stuck runs can be detected later
    try:
        log_sync(server_name, 'started', f'Started manual sync at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    except Exception:
        pass
    config = load_config()
    server_conf = config["sqlservers"].get(server_name)
    if server_conf:
        try:
            app.logger.info(f"[PROCESS] Processing hybrid sync for {server_name}")
            process_sql_server_hybrid(server_name, server_conf)
            app.logger.info(f"Sync completed successfully for {server_name}")
            app.logger.info(f"[SYNC COMPLETED] {server_name} at {datetime.now().strftime('%H:%M:%S')}")
            flash(f"Sync completed for {server_name}", "success")
            log_sync(server_name, "success")
            try:
                email_service.notify_sync_success(server_name)
            except Exception:
                pass
        except Exception as e:
            app.logger.error(f"Sync failed for {server_name}: {e}")
            app.logger.error(f"[SYNC FAILED] {server_name} - {e}")
            flash(f"Sync failed for {server_name}: {e}", "danger")
            log_sync(server_name, "failed", str(e))
            try:
                email_service.notify_sync_failed(server_name, str(e))
            except Exception:
                pass
    else:
        app.logger.warning(f"Server {server_name} not found in configuration")
        flash(f"Server {server_name} not found!", "danger")
    return redirect(url_for("index"))


@app.route('/sync_background/<server_name>', methods=['GET'])
@require_role(["admin", "operator"])
def sync_background(server_name):
    """Start the sync in a background thread using the enhanced sync manager.
    This provides better isolation, progress tracking, and prevents interference
    from navigation or other operations.
    """
    app.logger.info(f"[SYNC-ASYNC START] Request to start background sync for: {server_name}")
    
    config = load_config()
    server_conf = config.get("sqlservers", {}).get(server_name)
    
    if not server_conf:
        app.logger.warning(f"[SYNC-ASYNC] Server {server_name} not found in configuration")
        return jsonify({
            "success": False,
            "message": f"Server {server_name} not found in configuration"
        }), 404
    
    # Use the sync manager to start background sync
    result = sync_manager.start_sync(server_name, server_conf, app)
    
    if result["success"]:
        app.logger.info(f"[SYNC-ASYNC] Background sync started successfully for {server_name} (ID: {result['sync_id']})")
        flash(f"Background sync started for {server_name}", "success")
        return jsonify(result), 202
    else:
        app.logger.warning(f"[SYNC-ASYNC] Failed to start background sync for {server_name}: {result['message']}")
        flash(f"Failed to start sync for {server_name}: {result['message']}", "warning")
        return jsonify(result), 409


@app.route('/sync_status/<server_name>', methods=['GET'])
@require_role(["admin", "operator", "viewer"])
def sync_status(server_name):
    """Return the most recent sync status for a server as JSON.
    This now includes real-time status from the sync manager if a sync is currently running.
    """
    try:
        # Check if there's an active sync running
        active_sync = sync_manager.get_sync_status(server_name)
        
        if active_sync:
            # Check if sync is actually active (running) or completed
            sync_status = active_sync["status"]
            is_actually_active = sync_status not in ["completed", "failed"]
            
            # Return real-time sync status
            return jsonify({
                "server": server_name,
                "status": active_sync["status"],
                "progress": active_sync.get("progress", 0),
                "message": active_sync.get("message", ""),
                "sync_id": active_sync.get("sync_id"),
                "start_time": active_sync["start_time"].isoformat() if active_sync.get("start_time") else None,
                "current_database": active_sync.get("current_database"),
                "databases_processed": active_sync.get("databases_processed", 0),
                "total_databases": active_sync.get("total_databases", 0),
                "is_active": is_actually_active
            }), 200
        
        # No active sync, return last recorded status
        last = get_last_sync_for_server(server_name)
        if not last:
            return jsonify({"server": server_name, "status": "none", "is_active": False}), 200
        
        return jsonify({
            "server": last["server"],
            "status": last["status"],
            "time": last["time"],
            "details": last["details"],
            "is_active": False
        }), 200
        
    except Exception as e:
        app.logger.exception(f"Error fetching sync status for {server_name}: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/sync_status/all', methods=['GET'])
@require_role(["admin", "operator", "viewer"])
def all_sync_status():
    """Return status of all active syncs"""
    try:
        active_syncs = sync_manager.get_all_active_syncs()
        
        # Convert datetime objects to ISO format for JSON serialization
        for server_name, sync_data in active_syncs.items():
            if sync_data.get("start_time"):
                sync_data["start_time"] = sync_data["start_time"].isoformat()
            # Convert error timestamps too
            if sync_data.get("errors"):
                for error in sync_data["errors"]:
                    if error.get("timestamp"):
                        error["timestamp"] = error["timestamp"].isoformat()
        
        return jsonify({
            "active_syncs": active_syncs,
            "count": len(active_syncs)
        }), 200
        
    except Exception as e:
        app.logger.exception(f"Error fetching all sync statuses: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/sync_stop/<server_name>', methods=['POST'])
@require_role(["admin", "operator"])
def stop_sync(server_name):
    """Stop a running sync for the specified server"""
    try:
        result = sync_manager.stop_sync(server_name)
        
        if result["success"]:
            app.logger.info(f"[SYNC-STOP] Stop request successful for {server_name}")
            flash(f"Stop request sent for {server_name}", "info")
        else:
            app.logger.warning(f"[SYNC-STOP] Stop request failed for {server_name}: {result['message']}")
            flash(f"Cannot stop sync for {server_name}: {result['message']}", "warning")
        
        return jsonify(result), 200
        
    except Exception as e:
        app.logger.exception(f"Error stopping sync for {server_name}: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

CONFIG_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "config/db_connections.yaml")
)

def load_config():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

def load_pg_databases():
    """Return list of Postgres DBs, or [] if connection fails"""
    try:
        # Use db_utils approach that handles both env vars and YAML
        from db_utils import load_pg_config
        pg_conf = load_pg_config()
        
        # Connect to the configured Postgres database (or fallback to 'postgres')
        connect_db = pg_conf.get('database') or 'postgres'
        conn = psycopg2.connect(
            dbname=connect_db,
            user=pg_conf.get("username"),
            password=pg_conf.get("password"),
            host=pg_conf.get("host"),
            port=int(pg_conf.get("port", 5432)),
        )
        cur = conn.cursor()
        cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        dbs = [row[0] for row in cur.fetchall()]
        conn.close()
        return dbs
    except Exception as e:
        app.logger.exception(f"WARNING: Could not load Postgres DBs: {e}")
        return []


@app.route("/add-server", methods=["GET", "POST"])
@require_role(["admin", "operator"])
def add_server():
    if request.method == "POST":
        server_name = request.form["server_name"]
        server = request.form["server"]
        username = request.form["username"]
        password = request.form["password"]
        pg_database = request.form.get("pg_database")

        # Log the server details (without password)
        app.logger.info(f"Attempting to add new server '{server_name}' with address '{server}'")
        
        # Handle special characters in server name
        if '\\' in server:
            app.logger.info(f"Named instance detected: {server}")
        
        config = load_config()
        config.setdefault("sqlservers", {})

        if server_name in config["sqlservers"]:
            flash(f"Server {server_name} already exists!", "error")
            return redirect(url_for("add_server"))
            
        # Create the server config - no explicit port as we'll auto-detect
        server_conf = {
            "server": server,
            "username": username,
            "password": password,
            "check_new_databases": True,
            "skip_databases": [],
            "sync_mode": "hybrid",
            "target_postgres_db": pg_database,
        }
        
        # Test the connection before saving
        app.logger.info(f"Testing connection to {server} before saving configuration")
        success, error = test_sql_connection(server_conf)
        
        if not success:
            app.logger.error(f"Connection test failed for {server}: {error}")
            
            # Add diagnostic information for named instances
            diagnostic_info = ""
            if '\\' in server:
                diagnostic_info = (
                    " For named instances like 'server\\instancename', ensure that: "
                    "1) The SQL Browser service is running on the target server, "
                    "2) UDP port 1434 is accessible, and "
                    "3) The instance name is spelled correctly."
                )
            
            flash(f"Connection failed! {error}{diagnostic_info}", "danger")
            
            # Return to the form with the previously entered values
            postgres_dbs = load_pg_databases()
            return render_template("add_sources.html", 
                                  postgres_dbs=postgres_dbs,
                                  server_name=server_name,
                                  server=server,
                                  username=username,
                                  pg_database=pg_database)
        
        # If connection successful, save the configuration
        config["sqlservers"][server_name] = server_conf
        save_config(config)

        flash(f"Server {server_name} added with Postgres target {pg_database}", "success")
        return redirect(url_for("index"))

    # GET request
    postgres_dbs = load_pg_databases()
    return render_template("add_sources.html", postgres_dbs=postgres_dbs)


@app.route("/test-connection", methods=["POST"])
@require_role(["admin", "operator"])
def test_connection():
    """Test SQL Server connection via AJAX"""
    try:
        data = request.get_json()
        
        # Extract connection details from the request
        server_name = data.get("server_name", "").strip()
        server = data.get("server", "").strip()
        username = data.get("username", "").strip()
        password = data.get("password", "").strip()
        
        # Validate required fields
        if not all([server_name, server, username, password]):
            return jsonify({
                "success": False, 
                "message": "All fields are required"
            }), 400
        
        # Create temporary server config for testing
        server_conf = {
            "server": server,
            "username": username,
            "password": password,
        }
        
        # Test the connection
        app.logger.info(f"Testing connection to {server} for user {username}")
        success, error = test_sql_connection(server_conf)
        
        if success:
            return jsonify({
                "success": True,
                "message": "Connection successful!"
            })
        else:
            return jsonify({
                "success": False,
                "message": f"Connection failed: {error}"
            }), 400
            
    except Exception as e:
        app.logger.exception(f"Error testing connection: {e}")
        return jsonify({
            "success": False,
            "message": f"Connection test error: {str(e)}"
        }), 500


@app.route("/edit-server/<server_name>", methods=["GET", "POST"])
@require_role(["admin", "operator"])
def edit_server(server_name):
    config = load_config()
    servers = config.get("sqlservers", {})

    if request.method == "POST":
        server = request.form["server"]
        username = request.form["username"]
        password = request.form["password"]
        pg_database = request.form.get("pg_database")

        if server_name not in servers:
            flash(f"Server {server_name} does not exist!", "error")
            return redirect(url_for("index"))
            
        # Create server config - no explicit port as we'll auto-detect
        server_conf = {
            "server": server,
            "username": username,
            "password": password,
            "check_new_databases": True,
            "skip_databases": servers[server_name].get("skip_databases", []),
            "sync_mode": servers[server_name].get("sync_mode", "hybrid"),
            "target_postgres_db": pg_database,
        }
        
        # Test the connection before saving
        success, error = test_sql_connection(server_conf)
        if not success:
            flash(f"Connection failed! Error: {error}", "danger")
            # Return to the form with the previously entered values
            postgres_dbs = load_pg_databases()
            return render_template("edit_sources.html", 
                                  postgres_dbs=postgres_dbs,
                                  server_name=server_name,
                                  server_config={
                                      "server": server,
                                      "username": username,
                                      "password": password,
                                      "target_postgres_db": pg_database
                                  })
                                  
        # If connection successful, update the configuration
        servers[server_name] = server_conf
        save_config(config)

        flash(f"Server {server_name} updated successfully!", "success")
        return redirect(url_for("index"))

    # GET request → pre-fill form
    server_config = servers.get(server_name)
    if not server_config:
        flash(f"Server {server_name} not found!", "error")
        return redirect(url_for("index"))

    postgres_dbs = load_pg_databases()
    return render_template(
        "edit_sources.html",   # now points to your edit page
        postgres_dbs=postgres_dbs,
        server_name=server_name,
        server_config=server_config
    )

@app.route("/delete-server/<server_name>", methods=["POST"])
@require_role(["admin", "operator"])
def delete_server_route(server_name):
    """Delete a SQL Server from config"""
    from manage_server import delete_server  # import here to avoid circular import

    try:
        delete_server(server_name)
        flash(f"Server {server_name} deleted!", "success")
    except Exception as e:
        flash(f"Failed to delete server: {e}", "danger")
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
            flash(f"Schedule set for {server_name}", "success")
        except Exception as e:
            flash(f"Failed to set schedule: {e}", "danger")
        return redirect(url_for("schedule_page"))

    jobs = get_schedules()
    return render_template("schedule.html", servers=servers, jobs=jobs, role=session.get("role"))

# ------------------ CSV/Excel/Text Upload → Postgres ------------------

ALLOWED_EXTENSIONS = {"csv", "xlsx", "xls", "txt"}

# Global variable to track upload progress
upload_progress = {}


def _normalize_dataframe(df, file_content=None):
    """Normalize dataframe column names and handle malformed headers.
    If there are duplicate/very long column names or too many columns,
    fallback to a single-column dataframe with the full file content.
    """
    import re
    # If df is None or empty, try to use file_content
    try:
        rows, cols = df.shape
    except Exception:
        rows, cols = (0, 0)

    if (rows == 0 or cols == 0) and file_content:
        return pd.DataFrame({"content": [file_content]})

    cols_list = [str(c) for c in df.columns]
    # Detect problematic cases: duplicate column names, extremely long names, or too many columns
    dup = len(cols_list) != len(set(cols_list))
    long_name = any(len(c) > 120 for c in cols_list)
    too_many = len(cols_list) > 100

    if dup or long_name or too_many:
        # fallback to single-column containing the full file content (or join rows)
        if file_content is None:
            # join rows into one long string
            try:
                file_content = "\n".join(df.astype(str).agg(" ".join, axis=1).tolist())
            except Exception:
                file_content = " ".join(df.astype(str).values.flatten().astype(str).tolist())
        return pd.DataFrame({"content": [file_content]})

    # Sanitize column names and ensure uniqueness
    new_cols = []
    seen = {}
    for i, c in enumerate(cols_list):
        c2 = re.sub(r"[^0-9a-zA-Z_]", "_", c).strip("_").lower()
        if not c2:
            c2 = f"col_{i+1}"
        base = c2
        suffix = 1
        while c2 in seen:
            suffix += 1
            c2 = f"{base}_{suffix}"
        seen[c2] = True
        new_cols.append(c2)

    df.columns = new_cols
    return df


def _allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route("/upload", methods=["GET", "POST"])
@require_role(["admin", "operator"])
def upload_csv():
    try:
        postgres_dbs = load_pg_databases()

        if request.method == "POST":
            selected_db = request.form.get("pg_database")
            files = request.files.getlist("files")

            if not selected_db:
                flash("Please select a target PostgreSQL database.", "warning")
                return redirect(url_for("upload_csv"))

            if not files or len(files) == 0 or files[0].filename == "":
                flash("Please choose at least one file to upload.", "warning")
                return redirect(url_for("upload_csv"))

            # Generate unique session ID for tracking progress
            import uuid
            session_id = str(uuid.uuid4())
            
            # Initialize progress tracking
            upload_progress[session_id] = {
                'total': len(files),
                'completed': 0,
                'current_file': '',
                'status': 'processing',
                'results': []
            }

            try:
                engine = get_pg_engine(selected_db)
                successful_uploads = 0
                failed_uploads = 0

                for idx, file in enumerate(files):
                    if file.filename == "":
                        continue

                    if not _allowed_file(file.filename):
                        upload_progress[session_id]['results'].append({
                            'file': file.filename,
                            'status': 'failed',
                            'message': 'File type not allowed. Only CSV, XLS, XLSX, TXT are supported.'
                        })
                        failed_uploads += 1
                        continue

                    filename = secure_filename(file.filename)
                    upload_progress[session_id]['current_file'] = filename

                    try:
                        # Read file based on extension (read bytes first to avoid stream/seek issues)
                        import io
                        file_ext = filename.rsplit(".", 1)[1].lower()
                        file_bytes = file.read()
                        df = None
                        text_content = None

                        if file_ext == 'csv':
                            try:
                                text_content = file_bytes.decode('utf-8')
                                df = pd.read_csv(io.StringIO(text_content))
                            except Exception:
                                # fallback to bytes-based read
                                df = pd.read_csv(io.BytesIO(file_bytes))

                        elif file_ext in ['xlsx', 'xls']:
                            df = pd.read_excel(io.BytesIO(file_bytes))

                        elif file_ext == 'txt':
                            # decode to text and try several delimiter strategies
                            try:
                                text_content = file_bytes.decode('utf-8')
                            except Exception:
                                text_content = file_bytes.decode('latin-1', errors='replace')

                            # Try tab, then comma, then whitespace, then pandas auto
                            try:
                                df = pd.read_csv(io.StringIO(text_content), sep='\t')
                                if len(df.columns) == 1:
                                    df = pd.read_csv(io.StringIO(text_content), sep=',')
                                if len(df.columns) == 1:
                                    df = pd.read_csv(io.StringIO(text_content), sep=r'\s+', engine='python')
                            except Exception:
                                try:
                                    df = pd.read_csv(io.StringIO(text_content))
                                except Exception:
                                    # as last resort, store whole content as single column
                                    df = pd.DataFrame({'content': [text_content]})

                        else:
                            raise ValueError(f"Unsupported file type: {file_ext}")

                        # Normalize dataframe columns and fallback if headers malformed
                        df = _normalize_dataframe(df, file_content=text_content)

                        # Clean table name from file name (without extension)
                        table_name = os.path.splitext(filename)[0]
                        table_name = ''.join(c for c in table_name if c.isalnum() or c in '_-')
                        table_name = table_name.lower()  # PostgreSQL convention

                        # Load into public schema, replacing any existing table of same name
                        df.to_sql(table_name, engine, schema="public", if_exists="replace", index=False)

                        upload_progress[session_id]['results'].append({
                            'file': filename,
                            'status': 'success',
                            'message': f"Successfully loaded to table 'public.{table_name}' ({len(df)} rows)"
                        })
                        successful_uploads += 1

                    except Exception as e:
                        import traceback
                        error_details = traceback.format_exc()
                        print(f"[UPLOAD ERROR] File: {filename}")
                        print(f"[UPLOAD ERROR] {error_details}")
                        
                        upload_progress[session_id]['results'].append({
                            'file': filename,
                            'status': 'failed',
                            'message': f"Error: {str(e)}"
                        })
                        failed_uploads += 1

                    # Update progress
                    upload_progress[session_id]['completed'] = idx + 1

                # Mark as complete
                upload_progress[session_id]['status'] = 'complete'
                upload_progress[session_id]['current_file'] = ''

                # Flash summary message
                if successful_uploads > 0 and failed_uploads == 0:
                    flash(f"✅ Successfully uploaded {successful_uploads} file(s) to database '{selected_db}'.", "success")
                elif successful_uploads > 0 and failed_uploads > 0:
                    flash(f"⚠️ Uploaded {successful_uploads} file(s), {failed_uploads} failed. Check details below.", "warning")
                else:
                    flash(f"❌ All {failed_uploads} file(s) failed to upload.", "danger")

                return render_template("upload.html", 
                                     postgres_dbs=postgres_dbs, 
                                     role=session.get("role"),
                                     upload_results=upload_progress[session_id]['results'])

            except Exception as e:
                upload_progress[session_id]['status'] = 'error'
                upload_progress[session_id]['current_file'] = ''
                flash(f"Upload process failed: {e}", "danger")
                return redirect(url_for("upload_csv"))

        return render_template("upload.html", postgres_dbs=postgres_dbs, role=session.get("role"))
    except Exception as e:
        flash(f"Upload failed: {e}", "danger")
        return redirect(url_for("index"))

@app.route("/api/upload-progress/<session_id>")
@require_role(["admin", "operator"])
def get_upload_progress(session_id):
    """API endpoint to check upload progress"""
    if session_id in upload_progress:
        return jsonify(upload_progress[session_id])
    else:
        return jsonify({'status': 'not_found'}), 404

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
        flash(f"Schedule not found", "danger")
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
            flash(f"Schedule updated for {server_name}", "success")
            return redirect(url_for("view_schedules"))
        except Exception as e:
            flash(f"Failed to update schedule: {e}", "danger")

    return render_template("edit_schedule.html", job=job)

@app.route("/delete-schedule/<server_name>/<job_type>", methods=["POST"])
@require_role(["admin", "operator"])
def delete_schedule_route(server_name, job_type):
    """Delete a schedule"""
    try:
        delete_schedule(server_name, job_type)
        flash(f"Schedule deleted for {server_name}", "success")
    except Exception as e:
        flash(f"Failed to delete schedule: {e}", "danger")
    return redirect(url_for("view_schedules"))

@app.route("/api/verify-schedules")
@require_role(["admin", "operator", "viewer"])
def verify_schedules_api():
    """
    Verify that all schedules from database are loaded and running.
    Useful after server restart to confirm schedule restoration.
    """
    try:
        from scheduler_utils import get_schedules, scheduled_jobs
        import schedule as sched
        
        # Get schedules from database
        db_schedules = get_schedules()
        
        # Get currently loaded jobs from schedule library
        active_jobs = []
        for job in sched.jobs:
            active_jobs.append({
                'tags': list(job.tags) if hasattr(job, 'tags') else [],
                'next_run': str(job.next_run) if hasattr(job, 'next_run') else None
            })
        
        # Get in-memory job metadata
        memory_jobs = scheduled_jobs
        
        return jsonify({
            'database_schedules': db_schedules,
            'active_scheduler_jobs': active_jobs,
            'memory_metadata': memory_jobs,
            'status': 'ok',
            'message': f'{len(db_schedules)} schedules in database, {len(active_jobs)} active jobs in scheduler'
        })
    except Exception as e:
        return jsonify({'error': str(e), 'status': 'error'}), 500

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
        flash(f"Error comparing table {table}: {e}", "danger")
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
        flash(f"Error getting top changed tables: {e}", "danger")
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
        flash(f"Error loading alerts: {e}", "danger")
        return redirect(url_for("index"))




@app.route("/logs")
@require_role(["admin", "operator", "viewer"])
def view_logs():
    """View and analyze log files with pagination"""
    log_file = 'load_postgres.log'
    if not os.path.exists(log_file):
        flash(f"Log file '{log_file}' not found", "danger")
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
        flash(f"Log file '{log_file}' not found", "danger")
        return redirect(url_for("view_logs"))
    
    try:
        analyzer = LogAnalyzer(log_file)
        output_file = f"alerts_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        analyzer.generate_html_report(output_file)
        flash(f"HTML report generated: {output_file}", "success")
    except Exception as e:
        flash(f"Error generating report: {e}", "danger")
    
    return redirect(url_for("view_logs"))

@app.route("/logs/download")
@require_role(["admin", "operator", "viewer"])
def download_logs():
    """Download raw log file"""
    log_file = 'load_postgres.log'
    if not os.path.exists(log_file):
        flash(f"Log file '{log_file}' not found", "danger")
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
        flash(f"Error getting server metrics: {e}", "danger")
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
        flash(f"Error getting database metrics: {e}", "danger")
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
    """Show sync summary page with individual server comparisons"""
    try:
        all_comparisons = get_all_server_comparisons()
        return render_template("sync_summary.html", 
                             servers=all_comparisons['servers'],
                             total_servers=all_comparisons['total_servers'],
                             role=session.get("role"))
    except Exception as e:
        flash(f"Error getting sync summary: {e}", "danger")
        return redirect(url_for("index"))


@app.route("/sync-summary/quick.json")
@require_role(["admin", "operator", "viewer"])
def sync_summary_quick_json():
    """Return a lightweight cached summary suitable for fast page loads"""
    try:
        all_comparisons = get_all_server_comparisons()
        # Build lightweight payload: server_name and low-cost totals only
        servers = []
        for s in all_comparisons.get('servers', []):
            servers.append({
                'server_name': s.get('server_name'),
                'comparison': s.get('comparison', {}),
                'sql_server': {
                    'total_rows': s.get('sql_server', {}).get('total_rows', 0)
                },
                'postgresql': {
                    'total_rows': s.get('postgresql', {}).get('total_rows', 0)
                }
            })
        return jsonify({'servers': servers, 'total_servers': all_comparisons.get('total_servers', 0)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route("/sync-summary/<server_name>")
@require_role(["admin", "operator", "viewer"])
def sync_summary_detail(server_name):
    """Show detailed comparison for a specific server"""
    try:
        comparison_data = get_individual_server_comparison(server_name)
        if 'error' in comparison_data:
            flash(f"Error: {comparison_data['error']}", "danger")
            return redirect(url_for("sync_summary"))
        
        # Get detailed table comparison
        table_comparison = get_detailed_table_comparison(server_name)
        
        # Get server info from config
        config = load_config()
        sqlservers = config.get("sqlservers", {})
        server_config = sqlservers.get(server_name, {})
        
        server_info = {
            'server_name': server_name,
            'host': server_config.get('server', 'localhost'),
            'port': server_config.get('port', 1433),
            'target_postgres_db': server_config.get('target_postgres_db', 'unknown')
        }
        
        return render_template("sync_summary_detail.html", 
                             server_info=server_info,
                             comparison=comparison_data['comparison'],
                             table_comparison=table_comparison,
                             role=session.get("role"))
    except Exception as e:
        flash(f"Error getting detailed comparison for {server_name}: {e}", "danger")
        return redirect(url_for("sync_summary"))

@app.route("/sync-summary.json")
@require_role(["admin", "operator", "viewer"])
def sync_summary_json():
    """Return sync summary as JSON"""
    try:
        comparison_data = get_sync_comparison()
        payload = json.dumps(comparison_data, indent=2)
        return Response(payload, mimetype='application/json', headers={
            'Content-Disposition': 'attachment; filename=sync_summary.json'
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ------------------ ADVANCED ANALYTICS ROUTES ------------------

@app.route("/advanced-analytics")
@require_role(["admin", "operator", "viewer"])
def advanced_analytics():
    """Advanced Analytics Dashboard - Real-time performance metrics and trends"""
    try:
        metrics = get_advanced_analytics_metrics()
        return render_template("advanced_analytics.html", metrics=metrics, role=session.get("role"))
    except Exception as e:
        flash(f"Error loading analytics: {e}", "danger")
        return redirect(url_for("index"))

@app.route("/advanced-analytics/api/metrics")
@require_role(["admin", "operator", "viewer"])
def advanced_analytics_api_metrics():
    """API endpoint for real-time metrics updates"""
    try:
        metrics = get_advanced_analytics_metrics()
        return jsonify(metrics)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/advanced-analytics/api/activity")
@require_role(["admin", "operator", "viewer"])
def advanced_analytics_api_activity():
    """API endpoint for recent activity feed"""
    try:
        activities = get_recent_activity_feed()
        return jsonify(activities)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def get_advanced_analytics_metrics():
    """Aggregate advanced analytics metrics from database"""
    from datetime import datetime, timedelta
    
    metrics = {
        'total_syncs_today': 0,
        'successful_syncs': 0,
        'failed_syncs': 0,
        'success_rate': 0,
        'active_syncs': 0,
        'active_servers': 0,
        'total_servers': 0,  # NEW: Total unique servers
        'avg_sync_time': '0s',
        'performance_labels': [],
        'performance_data': [],
        'top_servers_labels': [],
        'top_servers_data': [],
        'duration_distribution': [0, 0, 0, 0, 0],
        'server_statuses': [],
        'recent_activities': []
    }
    
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", 5432)),
            database=os.getenv("POSTGRES_DB", "test1"),
            user=os.getenv("POSTGRES_USER", "migration_user"),
            password=os.getenv("POSTGRES_PASSWORD", "StrongPassword123")
        )
        cursor = conn.cursor()
        
        # Get today's date
        today = datetime.now().date()
        
        # Total syncs today - count only 'success' status (not 'started')
        cursor.execute("""
            SELECT COUNT(*) FROM metrics_sync_tables.sync_history 
            WHERE DATE(sync_time) = %s AND status IN ('success', 'failed', 'error')
        """, (today,))
        metrics['total_syncs_today'] = cursor.fetchone()[0] or 0
        
        # Successful syncs today
        cursor.execute("""
            SELECT COUNT(*) FROM metrics_sync_tables.sync_history 
            WHERE DATE(sync_time) = %s AND status = 'success'
        """, (today,))
        metrics['successful_syncs'] = cursor.fetchone()[0] or 0
        
        # Failed syncs today
        cursor.execute("""
            SELECT COUNT(*) FROM metrics_sync_tables.sync_history 
            WHERE DATE(sync_time) = %s AND status IN ('failed', 'error')
        """, (today,))
        metrics['failed_syncs'] = cursor.fetchone()[0] or 0
        
        # Success rate
        if metrics['total_syncs_today'] > 0:
            metrics['success_rate'] = round((metrics['successful_syncs'] / metrics['total_syncs_today']) * 100, 1)
        else:
            metrics['success_rate'] = 100
        
        # Active syncs - changed to 'started' since that's what your system uses
        cursor.execute("""
            SELECT COUNT(DISTINCT server_name) FROM metrics_sync_tables.sync_history 
            WHERE status = 'started'
        """)
        metrics['active_syncs'] = cursor.fetchone()[0] or 0
        
        # Active servers
        cursor.execute("""
            SELECT COUNT(DISTINCT server_name) FROM metrics_sync_tables.sync_history 
            WHERE status = 'started'
        """)
        metrics['active_servers'] = cursor.fetchone()[0] or 0
        
        # Total unique servers (all time)
        cursor.execute("""
            SELECT COUNT(DISTINCT server_name) FROM metrics_sync_tables.sync_history
        """)
        metrics['total_servers'] = cursor.fetchone()[0] or 0
        
        # Average sync time (simplified - sync_history doesn't have duration data)
        # We'll just show count of syncs in last 24 hours
        metrics['avg_sync_time'] = 'N/A'
        
        # Performance data (last 24 hours, hourly) - simplified to just show sync counts
        cursor.execute("""
            SELECT 
                TO_CHAR(DATE_TRUNC('hour', sync_time), 'HH24:MI') as hour,
                COUNT(*) as sync_count
            FROM metrics_sync_tables.sync_history 
            WHERE sync_time >= NOW() - INTERVAL '24 hours' 
            AND status = 'success'
            GROUP BY DATE_TRUNC('hour', sync_time)
            ORDER BY DATE_TRUNC('hour', sync_time)
        """)
        perf_data = cursor.fetchall()
        metrics['performance_labels'] = [row[0] for row in perf_data] if perf_data else ['00:00']
        metrics['performance_data'] = [row[1] for row in perf_data] if perf_data else [0]
        
        # Top servers by sync count (last 7 days) - show top 50 for production scale
        cursor.execute("""
            SELECT server_name, COUNT(*) as sync_count
            FROM metrics_sync_tables.sync_history 
            WHERE sync_time >= NOW() - INTERVAL '7 days'
            AND status = 'success'
            GROUP BY server_name
            ORDER BY sync_count DESC
            LIMIT 50
        """)
        top_servers = cursor.fetchall()
        metrics['top_servers_labels'] = [row[0] for row in top_servers] if top_servers else ['No Data']
        metrics['top_servers_data'] = [row[1] for row in top_servers] if top_servers else [0]
        
        # Duration distribution - skip for now since we don't have duration data
        # Just return default empty distribution
        metrics['duration_distribution'] = [0, 0, 0, 0, 0]
        
        # Server statuses - OPTIMIZED for production (100+ servers)
        # Single query with all needed data - avoids N+1 query problem
        cursor.execute("""
            WITH latest_sync AS (
                SELECT DISTINCT ON (server_name)
                    server_name,
                    status,
                    sync_time
                FROM metrics_sync_tables.sync_history 
                ORDER BY server_name, sync_time DESC
            ),
            success_rates AS (
                SELECT 
                    server_name,
                    COUNT(CASE WHEN status = 'success' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) as success_rate
                FROM metrics_sync_tables.sync_history 
                WHERE sync_time >= NOW() - INTERVAL '7 days'
                AND status IN ('success', 'failed', 'error')
                GROUP BY server_name
            ),
            today_counts AS (
                SELECT 
                    server_name,
                    COUNT(*) as sync_count
                FROM metrics_sync_tables.sync_history 
                WHERE DATE(sync_time) = %s
                AND status = 'success'
                GROUP BY server_name
            )
            SELECT 
                ls.server_name,
                ls.status,
                ls.sync_time,
                COALESCE(sr.success_rate, 0) as success_rate_7d,
                COALESCE(tc.sync_count, 0) as tables_synced
            FROM latest_sync ls
            LEFT JOIN success_rates sr ON ls.server_name = sr.server_name
            LEFT JOIN today_counts tc ON ls.server_name = tc.server_name
            ORDER BY ls.sync_time DESC
        """, (today,))
        server_rows = cursor.fetchall()
        
        for row in server_rows:
            server_name, status, last_sync, success_rate_7d, tables_count = row
            
            # Determine status color
            status_class = 'online' if status == 'success' else ('syncing' if status == 'started' else 'offline')
            
            metrics['server_statuses'].append({
                'name': server_name,
                'status': status_class,
                'last_sync': last_sync.strftime('%Y-%m-%d %H:%M:%S') if last_sync else 'Never',
                'duration': 'N/A',  # We don't have duration data
                'success_rate_7d': round(success_rate_7d, 1),
                'tables_synced': tables_count
            })
        
        # Recent activities (last 100 for production scale)
        cursor.execute("""
            SELECT server_name, status, sync_time, details
            FROM metrics_sync_tables.sync_history 
            ORDER BY sync_time DESC
            LIMIT 100
        """)
        activities = cursor.fetchall()
        
        for activity in activities:
            server_name, status, sync_time, details = activity
            
            # Skip 'started' status entries for activity feed
            if status == 'started':
                continue
            
            if status == 'success':
                icon = 'check_circle'
                icon_color = 'green'
                message = f"Successfully synced {server_name}"
            elif status == 'failed' or status == 'error':
                icon = 'error'
                icon_color = 'red'
                message = f"Failed to sync {server_name}"
                if details:
                    message += f": {details[:50]}..."
            else:
                icon = 'info'
                icon_color = 'gray'
                message = f"{server_name} - {status}"
            
            # Calculate time ago
            time_diff = datetime.now() - sync_time
            if time_diff.seconds < 60:
                time_ago = f"{time_diff.seconds}s ago"
            elif time_diff.seconds < 3600:
                time_ago = f"{time_diff.seconds // 60}m ago"
            elif time_diff.days == 0:
                time_ago = f"{time_diff.seconds // 3600}h ago"
            else:
                time_ago = f"{time_diff.days}d ago"
            
            metrics['recent_activities'].append({
                'icon': icon,
                'icon_color': icon_color,
                'message': message,
                'timestamp': time_ago
            })
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"Error getting advanced analytics metrics: {e}")
    
    return metrics

def get_recent_activity_feed():
    """Get recent activity feed for AJAX updates"""
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", 5432)),
            database=os.getenv("POSTGRES_DB", "test1"),
            user=os.getenv("POSTGRES_USER", "migration_user"),
            password=os.getenv("POSTGRES_PASSWORD", "StrongPassword123")
        )
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT server_name, status, sync_time, details
            FROM metrics_sync_tables.sync_history 
            ORDER BY sync_time DESC
            LIMIT 100
        """)
        activities = cursor.fetchall()
        
        result = []
        for activity in activities:
            server_name, status, sync_time, details = activity
            
            # Skip 'started' status entries
            if status == 'started':
                continue
            
            if status == 'success':
                icon = 'check_circle'
                icon_color = 'green'
                message = f"Successfully synced {server_name}"
            elif status == 'failed' or status == 'error':
                icon = 'error'
                icon_color = 'red'
                message = f"Failed to sync {server_name}"
                if details:
                    message += f": {details[:50]}..."
            else:
                icon = 'info'
                icon_color = 'gray'
                message = f"{server_name} - {status}"
            
            time_diff = datetime.now() - sync_time
            if time_diff.seconds < 60:
                time_ago = f"{time_diff.seconds}s ago"
            elif time_diff.seconds < 3600:
                time_ago = f"{time_diff.seconds // 60}m ago"
            elif time_diff.days == 0:
                time_ago = f"{time_diff.seconds // 3600}h ago"
            else:
                time_ago = f"{time_diff.days}d ago"
            
            result.append({
                'icon': icon,
                'icon_color': icon_color,
                'message': message,
                'timestamp': time_ago
            })
        
        cursor.close()
        conn.close()
        
        return result
    except Exception as e:
        logging.error(f"Error getting activity feed: {e}")
        return []

# ------------------ SYNC HISTORY ROUTES ------------------

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
        flash(f"Error loading sync history: {e}", "danger")
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
                flash("Resume requested. The next incremental run will continue from last PK.", "success")

        return render_template("resume_sync.html", server=server, db=db, table=table, info=info, preview=preview, role=session.get("role"))
    except Exception as e:
        flash(f"Error preparing resume: {e}", "danger")
        return redirect(url_for("index"))


@app.route("/schema-changes/<server>/<db>")
@require_role(["admin", "operator", "viewer"])
def schema_changes(server, db):
    try:
        # Parse from log file; optionally filter client-side in template
        events = parse_schema_changes_from_log()
        return render_template("schema_changes.html", server=server, db=db, events=events, role=session.get("role"))
    except Exception as e:
        flash(f"Error loading schema changes: {e}", "danger")
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
        flash(f"Error loading explorer: {e}", "danger")
        return redirect(url_for("index"))

# ------------------ MAIN ------------------
if __name__ == "__main__":
    app.logger.info("="*60)
    app.logger.info("[STARTING] ACTIN SYNC APPLICATION")
    app.logger.info("="*60)
    app.logger.info(f"[DATE] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    app.logger.info(f"[PYTHON] {sys.version.split()[0]}")
    app.logger.info(f"[FLASK] Debug Mode: ON")
    app.logger.info(f"[LOGGING] Level: INFO")
    app.logger.info("="*60)
    
    # Configure werkzeug to be more verbose
    logging.getLogger('werkzeug').setLevel(logging.DEBUG)
    
    try:
        # Show startup status
        app.logger.info("[INIT] Initializing Flask application...")
        app.logger.info("[CONFIG] Loading configuration...")
        app.logger.info("[AUTH] Authentication system ready")
        # Optionally create default admin based on environment variable
        try:
            # Always ensure default admin exists on startup when running app.py
            from auth import init_admin_user
            default_pw = os.environ.get('DEFAULT_ADMIN_PASSWORD', 'admin123')
            init_admin_user(create_if_missing=True, default_password=default_pw)
            app.logger.info('Ensured default admin exists (created if missing)')
        except Exception as e:
            # Non-fatal: keep startup going even if admin creation fails
            app.logger.exception(f'Default admin creation skipped or failed: {e}')
        app.logger.info("[DATABASE] Database connections configured")
        app.logger.info("[SCHEDULER] Scheduler system active")
        
        # Initialize daily email summary scheduler
        try:
            from daily_summary_scheduler import init_daily_summary
            daily_scheduler = init_daily_summary(app)
            app.logger.info("[EMAIL] Daily summary scheduler initialized (sends at 23:59)")
        except Exception as e:
            app.logger.warning(f"[EMAIL] Failed to initialize daily summary scheduler: {e}")
        
        # Add diagnostic route for SQL Server named instances
        @app.route('/api/diagnose-sql-server/<server_name>', methods=['GET'])
        @require_role(["admin"])
        def diagnose_sql_server(server_name):
            """
            Diagnostic endpoint to help troubleshoot SQL Server connection issues,
            particularly for named instances with port detection problems.
            """
            try:
                from hybrid_sync import get_sql_server_instance_info
                
                # Get server config
                config = load_config()
                server_conf = config.get("sqlservers", {}).get(server_name)
                
                if not server_conf:
                    return jsonify({"error": f"Server {server_name} not found"}), 404
                    
                # Get instance info
                info = get_sql_server_instance_info(server_conf["server"])
                
                # Test connection and add result
                success, error = test_sql_connection(server_conf)
                info["connection_test"] = {
                    "success": success,
                    "error": error
                }
                
                return jsonify({
                    "server_name": server_name,
                    "instance_info": info
                })
            except Exception as e:
                return jsonify({"error": str(e)}), 500

        # Add API endpoint for job statuses
        @app.route('/api/job-statuses', methods=['GET'])
        @require_role(["admin", "operator", "viewer"])
        def api_job_statuses():
            """API endpoint to get current job statuses for the view schedules page"""
            try:
                from scheduler_utils import scheduled_jobs
                return jsonify(scheduled_jobs)
            except Exception as e:
                app.logger.error(f"Error fetching job statuses: {e}")
                return jsonify({"error": str(e)}), 500
                
        app.logger.info("[STARTUP] Application startup complete!")
        
        app.logger.info("[READY] Application ready! Access at: http://127.0.0.1:5000")
        app.logger.info("="*60)
        
        # Run with configurable runtime flags from environment for safety
        flask_debug = os.environ.get('FLASK_DEBUG', '0') in ('1', 'true', 'True')
        app_host = os.environ.get('APP_HOST', '127.0.0.1')
        app_port = int(os.environ.get('APP_PORT', '5001'))
        use_debugger = os.environ.get('APP_USE_DEBUGGER', '0') in ('1', 'true', 'True')
        use_reloader = os.environ.get('APP_USE_RELOADER', '0') in ('1', 'true', 'True')

        app.logger.info(f"Starting Flask app host={app_host} port={app_port} debug={flask_debug} debugger={use_debugger}")
        app.run(debug=flask_debug, host=app_host, port=app_port, use_reloader=use_reloader, use_debugger=use_debugger)
    except Exception as e:
        app.logger.error(f"[ERROR] STARTUP ERROR: {e}")
        app.logger.error(f"Failed to start application: {e}")
        sys.exit(1)
