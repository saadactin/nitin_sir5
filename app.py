from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify, Response
import pandas as pd
from werkzeug.utils import secure_filename
import json
import psycopg2
import requests
try:
    from clickhouse_driver import Client as CHClient
except Exception:
    CHClient = None
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
    
    # Load data_sources from Postgres so Add Source entries appear on home page
    data_sources = []
    data_source_statuses = {}
    print(f"[CONSOLE DEBUG] Starting to load data_sources...")
    try:
        from db_utils import load_pg_config
        pg_conf = load_pg_config()
        print(f"[CONSOLE DEBUG] Pg config: db={pg_conf.get('database')}, host={pg_conf.get('host')}")
        conn = psycopg2.connect(
            dbname=pg_conf.get('database', 'metrics_sync_tables'),
            user=pg_conf.get('username'),
            password=pg_conf.get('password'),
            host=pg_conf.get('host'),
            port=int(pg_conf.get('port', 5432))
        )
        print(f"[CONSOLE DEBUG] Connected to PostgreSQL")
        cur = conn.cursor()
        cur.execute("SELECT id, source_name, source_type, server_address, username, target_type, target_database, connection_details FROM data_sources WHERE is_active = true ORDER BY created_at DESC")
        rows = cur.fetchall()
        app.logger.info(f"[DEBUG] Query returned {len(rows)} data_source rows")
        print(f"[CONSOLE DEBUG] Query returned {len(rows)} rows")
        for r in rows:
            ds = {
                'id': r[0],
                'source_name': r[1],
                'source_type': r[2],
                'server_address': r[3],
                'username': r[4],
                'target_type': r[5],
                'target_database': r[6],
                'connection_details': r[7]
            }
            data_sources.append(ds)
            app.logger.info(f"[DEBUG] Loaded data_source: {ds['source_name']} (ID: {ds['id']})")
            print(f"[CONSOLE DEBUG] Loaded source: {ds['source_name']}")
        cur.close()
        conn.close()
    except Exception as e:
        app.logger.exception(f"Could not load data_sources: {e}")
        print(f"[CONSOLE DEBUG] ERROR loading data_sources: {e}")
        import traceback
        traceback.print_exc()

    # Determine status for each data source
    for ds in data_sources:
        try:
            if ds['source_type'] == 'sap_hana':
                # Test HANA connection
                try:
                    import hdbcli.dbapi as hana_dbapi
                    connection_details = json.loads(ds['connection_details']) if isinstance(ds['connection_details'], str) else (ds['connection_details'] or {})
                    
                    # Parse server address
                    if ':' in ds['server_address']:
                        host, port = ds['server_address'].split(':', 1)
                    else:
                        host = ds['server_address']
                        port = connection_details.get('port', '30015')
                    
                    # Try to connect
                    conn = hana_dbapi.connect(
                        address=host,
                        port=int(port),
                        user=ds['username'],
                        password=ds['password'],
                        encrypt=True,
                        sslValidateCertificate=False,
                        timeout=5
                    )
                    conn.close()
                    data_source_statuses[ds['id']] = {'online': True, 'error': None}
                except ImportError:
                    # hdbcli not installed - can't test connection
                    data_source_statuses[ds['id']] = {'online': None, 'error': 'hdbcli library not installed'}
                except Exception as e:
                    data_source_statuses[ds['id']] = {'online': False, 'error': str(e)}
            elif ds['source_type'] == 'sql_server':
                # Build a minimal server_conf similar to YAML config
                server_conf = {
                    'server': ds['server_address'],
                    'username': ds.get('username'),
                    'password': None,  # don't expose password here
                }
                # We can't test without password; mark as unknown unless password present in DB
                # Attempt to fetch password from DB for testing (internal only)
                try:
                    from db_utils import load_pg_config
                    pg_conf = load_pg_config()
                    conn = psycopg2.connect(
                        dbname=pg_conf.get('database', 'metrics_sync_tables'),
                        user=pg_conf.get('username'),
                        password=pg_conf.get('password'),
                        host=pg_conf.get('host'),
                        port=int(pg_conf.get('port', 5432))
                    )
                    cur = conn.cursor()
                    cur.execute("SELECT password FROM data_sources WHERE id = %s", (ds['id'],))
                    pw_row = cur.fetchone()
                    cur.close()
                    conn.close()
                    if pw_row and pw_row[0]:
                        server_conf['password'] = pw_row[0]
                except Exception:
                    pass

                if server_conf.get('password'):
                    success, error = test_sql_connection(server_conf)
                    data_source_statuses[ds['id']] = {'online': success, 'error': error}
                else:
                    data_source_statuses[ds['id']] = {'online': False, 'error': 'Password not available for connection test'}
            elif ds['source_type'] == 'api' or ds['source_type'] == 'rest_api':
                # Test API connection
                try:
                    import requests
                    api_url = ds['server_address']
                    connection_details = ds.get('connection_details') or {}
                    
                    # Check if it's an SSE stream
                    is_sse = connection_details.get('is_sse', False)
                    
                    if is_sse:
                        # For SSE, just mark as "streaming" without full test
                        data_source_statuses[ds['id']] = {'online': True, 'error': None, 'status': 'Streaming'}
                    else:
                        headers = {}
                        auth_type = connection_details.get('auth_type', 'none')
                        
                        # Handle OAuth authentication
                        if auth_type == 'oauth':
                            oauth_token_url = connection_details.get('oauth_token_url', '')
                            oauth_username = connection_details.get('oauth_username', '')
                            oauth_password = connection_details.get('oauth_password', '')
                            
                            if oauth_token_url and oauth_username and oauth_password:
                                try:
                                    app.logger.info(f"Status check OAuth: Requesting token from {oauth_token_url}")
                                    token_response = requests.post(
                                        oauth_token_url,
                                        json={"username": oauth_username, "password": oauth_password},
                                        timeout=10
                                    )
                                    
                                    if token_response.status_code == 200:
                                        token_data = token_response.json()
                                        oauth_token = token_data.get('token') or token_data.get('access_token') or token_data.get('oauth_token')
                                        
                                        if not oauth_token:
                                            # Try to find any string field that looks like a token
                                            for key, value in token_data.items():
                                                if isinstance(value, str) and len(value) > 20:
                                                    oauth_token = value
                                                    break
                                        
                                        if oauth_token:
                                            headers["Authorization"] = f"Bearer {oauth_token}"
                                            app.logger.info(f"Status check: OAuth token obtained successfully")
                                        else:
                                            data_source_statuses[ds['id']] = {'online': False, 'error': 'No token found in OAuth response'}
                                            continue
                                    else:
                                        data_source_statuses[ds['id']] = {'online': False, 'error': f'OAuth token request failed: HTTP {token_response.status_code}'}
                                        continue
                                except Exception as e:
                                    app.logger.exception(f"OAuth token request failed in status check: {e}")
                                    data_source_statuses[ds['id']] = {'online': False, 'error': f'OAuth token request failed: {str(e)}'}
                                    continue
                            else:
                                data_source_statuses[ds['id']] = {'online': False, 'error': 'OAuth credentials missing'}
                                continue
                        elif auth_type == 'bearer':
                            token = connection_details.get('auth_token', '')
                            headers['Authorization'] = f'Bearer {token}'
                        elif auth_type == 'apikey':
                            key_name = connection_details.get('apikey_header', 'X-API-Key')
                            key_value = connection_details.get('auth_token', '')
                            headers[key_name] = key_value
                        
                        # Make the status check request
                        response = requests.get(api_url, headers=headers, timeout=5)
                        if response.status_code == 200:
                            data_source_statuses[ds['id']] = {'online': True, 'error': None}
                        else:
                            data_source_statuses[ds['id']] = {'online': False, 'error': f'HTTP {response.status_code}'}
                except Exception as e:
                    data_source_statuses[ds['id']] = {'online': False, 'error': str(e)}
            else:
                data_source_statuses[ds['id']] = {'online': False, 'error': 'Unknown source type'}
        except Exception as e:
            app.logger.exception(f"Error checking status for source {ds.get('source_name')}: {e}")
            data_source_statuses[ds['id']] = {'online': False, 'error': str(e)}

    role = session.get("role")
    app.logger.info(f"[INFO] Loaded {len(sqlservers)} SQL servers and {len(data_sources)} data sources for display")
    print(f"[CONSOLE DEBUG] About to render: sqlservers={len(sqlservers)}, data_sources={len(data_sources)}")
    print(f"[CONSOLE DEBUG] data_sources content: {[ds.get('source_name') for ds in data_sources]}")
    return render_template("sync_servers.html", sqlservers=sqlservers, server_statuses=server_statuses, data_sources=data_sources, data_source_statuses=data_source_statuses, role=role)


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


@app.route('/sync_api_source/<int:source_id>', methods=['GET', 'POST'])
@require_role(["admin", "operator"])
def sync_api_source(source_id):
    """Manually trigger sync for a REST API source"""
    app.logger.info(f"[API-SYNC] Request to sync API source ID: {source_id}")
    
    try:
        # Load source details from database
        from db_utils import load_pg_config
        pg_conf = load_pg_config()
        conn = psycopg2.connect(
            dbname=pg_conf.get('database', 'metrics_sync_tables'),
            user=pg_conf.get('username'),
            password=pg_conf.get('password'),
            host=pg_conf.get('host'),
            port=int(pg_conf.get('port', 5432))
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT source_name, source_type, server_address, target_type, 
                   target_database, connection_details 
            FROM data_sources 
            WHERE id = %s AND is_active = true
        """, (source_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        if not row:
            app.logger.warning(f"[API-SYNC] API source {source_id} not found or inactive")
            flash(f"API source not found or inactive", "danger")
            return redirect(url_for("index"))
        
        source_name = row[0]
        source_type = row[1]
        api_url = row[2]
        target_type = row[3]
        target_database = row[4]
        connection_details = json.loads(row[5]) if row[5] else {}
        
        # Only sync REST APIs to ClickHouse
        if source_type != 'rest_api':
            flash(f"Source '{source_name}' is not a REST API source", "danger")
            return redirect(url_for("index"))
        
        if target_type.lower() != 'clickhouse':
            flash(f"Source '{source_name}' target is not ClickHouse", "danger")
            return redirect(url_for("index"))
        
        # Extract connection details
        auth_type = connection_details.get('auth_type', 'none')
        auth_token = connection_details.get('auth_token', '')
        basic_username = connection_details.get('basic_username', '')
        basic_password = connection_details.get('basic_password', '')
        apikey_header = connection_details.get('apikey_header', 'X-API-Key')
        custom_headers_str = connection_details.get('custom_headers', '')
        request_method = connection_details.get('request_method', 'GET')
        data_path = connection_details.get('data_path', '')
        # Get target_table from connection_details, or generate with "crm_" prefix
        clean_source_name = source_name.lower().replace(' ', '_').replace('-', '_')
        target_table = connection_details.get('target_table', f"crm_{clean_source_name}")
        is_sse = connection_details.get('is_sse', False)
        
        # Parse custom headers
        custom_headers = {}
        if custom_headers_str:
            try:
                custom_headers = json.loads(custom_headers_str)
            except:
                pass
        
        # Start sync in background thread
        import threading
        from api_sync import sync_api_to_clickhouse_once, sync_api_to_clickhouse
        
        def background_sync():
            try:
                app.logger.info(f"Starting sync for API source: {source_name}")
                
                # Check if this is a polling API (continuous checks) or SSE stream or one-time
                polling_mode = connection_details.get('polling_mode', False)
                poll_interval = connection_details.get('poll_interval', 5)
                id_column = connection_details.get('id_column', 'id')
                upsert_mode = connection_details.get('upsert_mode', False)
                
                if polling_mode and upsert_mode:
                    # UPSERT mode: Update existing records + insert new ones
                    app.logger.info(f"Starting UPSERT mode for '{source_name}' (every {poll_interval}s)")
                    from api_upsert import upsert_api_to_clickhouse
                    upsert_api_to_clickhouse(
                        api_url=api_url,
                        target_database=target_database,
                        target_table=target_table,
                        auth_type=auth_type,
                        auth_token=auth_token,
                        basic_username=basic_username,
                        basic_password=basic_password,
                        apikey_header=apikey_header,
                        custom_headers=custom_headers,
                        request_method=request_method,
                        data_path=data_path,
                        poll_interval=poll_interval,
                        id_column=id_column,
                        auto_create_table=True,
                        oauth_token_url=connection_details.get('oauth_token_url', ''),
                        oauth_username=connection_details.get('oauth_username', ''),
                        oauth_password=connection_details.get('oauth_password', ''),
                        oauth_refresh_interval=connection_details.get('oauth_refresh_interval', 3600)
                    )
                elif polling_mode:
                    # Polling mode: Continuously check API for new data
                    app.logger.info(f"Starting POLLING mode for '{source_name}' (every {poll_interval}s)")
                    from api_polling import poll_api_to_clickhouse
                    poll_api_to_clickhouse(
                        api_url=api_url,
                        target_database=target_database,
                        target_table=target_table,
                        auth_type=auth_type,
                        auth_token=auth_token,
                        basic_username=basic_username,
                        basic_password=basic_password,
                        apikey_header=apikey_header,
                        custom_headers=custom_headers,
                        request_method=request_method,
                        data_path=data_path,
                        poll_interval=poll_interval,
                        id_column=id_column,
                        auto_create_table=True,
                        oauth_token_url=connection_details.get('oauth_token_url', ''),
                        oauth_username=connection_details.get('oauth_username', ''),
                        oauth_password=connection_details.get('oauth_password', ''),
                        oauth_refresh_interval=connection_details.get('oauth_refresh_interval', 3600)
                    )
                elif is_sse:
                    # True SSE streams (text/event-stream)
                    app.logger.info(f"Starting SSE mode for '{source_name}'")
                    sync_api_to_clickhouse(
                        api_url=api_url,
                        target_database=target_database,
                        target_table=target_table,
                        auth_type=auth_type,
                        auth_token=auth_token,
                        basic_username=basic_username,
                        basic_password=basic_password,
                        apikey_header=apikey_header,
                        custom_headers=custom_headers,
                        request_method=request_method,
                        data_path=data_path,
                        is_sse=is_sse,
                        auto_create_table=True
                    )
                else:
                    # Regular REST APIs: sync once
                    app.logger.info(f"Starting ONE-TIME sync for '{source_name}'")
                    result = sync_api_to_clickhouse_once(
                        api_url=api_url,
                        target_database=target_database,
                        target_table=target_table,
                        auth_type=auth_type,
                        auth_token=auth_token,
                        basic_username=basic_username,
                        basic_password=basic_password,
                        apikey_header=apikey_header,
                        custom_headers=custom_headers,
                        request_method=request_method,
                        data_path=data_path,
                        auto_create_table=True
                    )
                    app.logger.info(f"API sync result for '{source_name}': {result}")
            except Exception as e:
                app.logger.error(f"Error syncing API source '{source_name}': {e}")
        
        sync_thread = threading.Thread(target=background_sync, daemon=True)
        sync_thread.start()
        
        flash(f"Sync started for API source '{source_name}'", "success")
        return redirect(url_for("index"))
        
    except Exception as e:
        app.logger.exception(f"Error initiating API source sync: {e}")
        flash(f"Error starting sync: {str(e)}", "danger")
        return redirect(url_for("index"))


@app.route('/source/<int:source_id>/databases', methods=['GET'])
@require_role(["admin", "operator", "viewer"])
def view_source_databases(source_id):
    """Show databases for a configured source (SQL Server or HANA)"""
    try:
        # Load source from DB
        from db_utils import load_pg_config
        pg_conf = load_pg_config()
        conn = psycopg2.connect(
            dbname=pg_conf.get('database', 'metrics_sync_tables'),
            user=pg_conf.get('username'),
            password=pg_conf.get('password'),
            host=pg_conf.get('host'),
            port=int(pg_conf.get('port', 5432))
        )
        cur = conn.cursor()
        cur.execute("SELECT id, source_name, source_type, server_address, username, password, connection_details FROM data_sources WHERE id = %s", (source_id,))
        row = cur.fetchone()
        cur.close(); conn.close()

        if not row:
            flash('Source not found', 'danger')
            return redirect(url_for('index'))

        source = {
            'id': row[0], 'source_name': row[1], 'source_type': row[2], 'server_address': row[3], 'username': row[4], 'password': row[5], 'connection_details': row[6]
        }

        if source['source_type'] == 'sql_server':
            server_conf = {'server': source['server_address'], 'username': source['username'], 'password': source['password']}
            conn_sql = get_sql_connection(server_conf)
            dbs = hs_get_all_databases(conn_sql)
            conn_sql.close()
            return render_template('server_databases.html', server_name=source['source_name'], databases=dbs, role=session.get('role'))

        elif source['source_type'] == 'sap_hana':
            # Attempt HANA listing if hdbcli is available
            try:
                from hdbcli import dbapi as hana_dbapi
                host_port = source['server_address']
                h, p = (host_port.split(':') + [None])[:2]
                port = int(p) if p else 30015
                conn_h = hana_dbapi.connect(address=h, port=port, user=source['username'], password=source['password'])
                # Simple query to get schemas/databases
                cur_h = conn_h.cursor()
                cur_h.execute("SELECT SCHEMA_NAME FROM SYS.SCHEMAS")
                dbs = [r[0] for r in cur_h.fetchall()]
                cur_h.close(); conn_h.close()
                return render_template('server_databases.html', server_name=source['source_name'], databases=dbs, role=session.get('role'))
            except Exception as e:
                flash(f"Could not list HANA databases: {e}", 'danger')
                return redirect(url_for('index'))

        else:
            flash('Unsupported source type for database listing', 'danger')
            return redirect(url_for('index'))

    except Exception as e:
        app.logger.exception(f"Failed to load source databases: {e}")
        flash(f"Failed to load databases: {e}", 'danger')
        return redirect(url_for('index'))


@app.route('/sync_source_background/<int:source_id>', methods=['GET'])
@require_role(["admin", "operator"])
def sync_source_background(source_id):
    """Start background sync for a configured source (SQL Server and REST API supported)"""
    try:
        from db_utils import load_pg_config
        pg_conf = load_pg_config()
        conn = psycopg2.connect(
            dbname=pg_conf.get('database', 'metrics_sync_tables'),
            user=pg_conf.get('username'),
            password=pg_conf.get('password'),
            host=pg_conf.get('host'),
            port=int(pg_conf.get('port', 5432))
        )
        cur = conn.cursor()
        cur.execute("SELECT id, source_name, source_type, server_address, username, password, target_database, connection_details FROM data_sources WHERE id = %s", (source_id,))
        row = cur.fetchone()
        cur.close(); conn.close()

        if not row:
            return jsonify({"success": False, "message": "Source not found"}), 404

        source = {
            'id': row[0], 
            'source_name': row[1], 
            'source_type': row[2], 
            'server_address': row[3], 
            'username': row[4], 
            'password': row[5],
            'target_database': row[6],
            'connection_details': row[7]
        }

        # Handle REST API sources
        if source['source_type'] == 'rest_api':
            from api_sync import sync_api_to_clickhouse_once, sync_api_to_clickhouse
            from api_polling import poll_api_to_clickhouse
            import threading
            import json
            
            connection_details = json.loads(source['connection_details']) if isinstance(source['connection_details'], str) else (source['connection_details'] or {})
            is_sse = connection_details.get('is_sse', False)
            polling_mode = connection_details.get('polling_mode', False)
            poll_interval = connection_details.get('poll_interval', 5)
            id_column = connection_details.get('id_column', 'id')
            upsert_mode = connection_details.get('upsert_mode', False)
            custom_headers_str = connection_details.get('custom_headers', '')
            custom_headers = {}
            if custom_headers_str:
                try:
                    custom_headers = json.loads(custom_headers_str)
                except:
                    pass
            
            def background_sync():
                try:
                    # Determine sync mode: upsert, polling, SSE, or one-time
                    if polling_mode and upsert_mode:
                        # UPSERT mode: Insert new + update existing records
                        app.logger.info(f"Starting UPSERT mode for '{source['source_name']}' (every {poll_interval}s)")
                        from api_upsert import upsert_api_to_clickhouse
                        upsert_api_to_clickhouse(
                            api_url=source['server_address'],
                            target_database=source['target_database'],
                            target_table=connection_details.get('target_table', 'api_data'),
                            auth_type=connection_details.get('auth_type'),
                            auth_token=connection_details.get('auth_token'),
                            basic_username=connection_details.get('basic_username'),
                            basic_password=connection_details.get('basic_password'),
                            apikey_header=connection_details.get('apikey_header'),
                            custom_headers=custom_headers,
                            request_method=connection_details.get('request_method', 'GET'),
                            data_path=connection_details.get('data_path', ''),
                            poll_interval=poll_interval,
                            id_column=id_column,
                            auto_create_table=True,
                            oauth_token_url=connection_details.get('oauth_token_url', ''),
                            oauth_username=connection_details.get('oauth_username', ''),
                            oauth_password=connection_details.get('oauth_password', ''),
                            oauth_refresh_interval=connection_details.get('oauth_refresh_interval', 3600)
                        )
                    elif polling_mode:
                        # Polling mode: Continuously check API for new data
                        app.logger.info(f"Starting POLLING mode for '{source['source_name']}' (every {poll_interval}s)")
                        poll_api_to_clickhouse(
                            api_url=source['server_address'],
                            target_database=source['target_database'],
                            target_table=connection_details.get('target_table', 'api_data'),
                            auth_type=connection_details.get('auth_type'),
                            auth_token=connection_details.get('auth_token'),
                            basic_username=connection_details.get('basic_username'),
                            basic_password=connection_details.get('basic_password'),
                            apikey_header=connection_details.get('apikey_header'),
                            custom_headers=custom_headers,
                            request_method=connection_details.get('request_method', 'GET'),
                            data_path=connection_details.get('data_path', ''),
                            poll_interval=poll_interval,
                            id_column=id_column,
                            auto_create_table=True,
                            oauth_token_url=connection_details.get('oauth_token_url', ''),
                            oauth_username=connection_details.get('oauth_username', ''),
                            oauth_password=connection_details.get('oauth_password', ''),
                            oauth_refresh_interval=connection_details.get('oauth_refresh_interval', 3600)
                        )
                    elif is_sse:
                        # SSE streams need continuous monitoring
                        app.logger.info(f"Starting SSE mode for '{source['source_name']}'")
                        sync_api_to_clickhouse(
                            api_url=source['server_address'],
                            target_database=source['target_database'],
                            target_table=connection_details.get('target_table', 'api_data'),
                            auth_type=connection_details.get('auth_type'),
                            auth_token=connection_details.get('auth_token'),
                            basic_username=connection_details.get('basic_username'),
                            basic_password=connection_details.get('basic_password'),
                            apikey_header=connection_details.get('apikey_header'),
                            custom_headers=custom_headers,
                            request_method=connection_details.get('request_method', 'GET'),
                            data_path=connection_details.get('data_path', ''),
                            is_sse=is_sse,
                            auto_create_table=True
                        )
                    else:
                        # Regular REST APIs: sync once
                        app.logger.info(f"Starting ONE-TIME sync for '{source['source_name']}'")
                        result = sync_api_to_clickhouse_once(
                            api_url=source['server_address'],
                            target_database=source['target_database'],
                            target_table=connection_details.get('target_table', 'api_data'),
                            auth_type=connection_details.get('auth_type'),
                            auth_token=connection_details.get('auth_token'),
                            basic_username=connection_details.get('basic_username'),
                            basic_password=connection_details.get('basic_password'),
                            apikey_header=connection_details.get('apikey_header'),
                            custom_headers=custom_headers,
                            request_method=connection_details.get('request_method', 'GET'),
                            data_path=connection_details.get('data_path', ''),
                            auto_create_table=True
                        )
                        app.logger.info(f"API sync result for '{source['source_name']}': {result}")
                except Exception as e:
                    app.logger.error(f"Error syncing API source '{source['source_name']}': {e}")
            
            # Start sync in background thread
            sync_thread = threading.Thread(target=background_sync, daemon=True)
            sync_thread.start()
            
            sync_mode = "continuous polling" if polling_mode else ("SSE stream" if is_sse else "one-time")
            flash(f"Background sync started for {source['source_name']} ({sync_mode})", 'success')
            return jsonify({
                "success": True,
                "message": f"Sync started for {source['source_name']}",
                "source_id": source_id
            }), 202
        
        # Handle HANA sources - perform incremental sync
        elif source['source_type'] == 'sap_hana':
            from hana_sync import HanaToClickHouseSync
            import threading
            
            connection_details = json.loads(source['connection_details']) if isinstance(source['connection_details'], str) else (source['connection_details'] or {})
            
            # Parse server address (format: host:port)
            if ':' in source['server_address']:
                host, port = source['server_address'].split(':', 1)
            else:
                host = source['server_address']
                port = connection_details.get('port', '30015')
            
            # Prepare configs
            hana_config = {
                'host': host,
                'port': int(port),
                'username': source['username'],
                'password': source['password']
            }
            
            clickhouse_config = {
                'host': os.getenv('CLICKHOUSE_HOST', 'localhost'),
                'port': int(os.getenv('CLICKHOUSE_PORT', '9000')),
                'user': os.getenv('CLICKHOUSE_USER', 'default'),
                'password': os.getenv('CLICKHOUSE_PASSWORD', ''),
                'database': source['target_database'] if source['target_database'] else 'hana_migrated'
            }
            
            def background_incremental_sync():
                try:
                    sync_engine = HanaToClickHouseSync(hana_config, clickhouse_config)
                    
                    if not (sync_engine.connect_hana() and sync_engine.connect_clickhouse()):
                        app.logger.error(f"Failed to connect to HANA or ClickHouse for source {source_id}")
                        return
                    
                    try:
                        # Get all tables that have been synced before (from sync_metadata)
                        database_name = clickhouse_config['database']
                        
                        # Check if sync_metadata table exists
                        try:
                            sync_metadata = sync_engine.ch_client.execute(f"""
                            SELECT source_schema, source_table 
                            FROM {database_name}.sync_metadata 
                            WHERE sync_enabled = 1
                            """)
                        except Exception as e:
                            app.logger.warning(f"No sync_metadata table found or no tables configured for incremental sync: {e}")
                            app.logger.info("Performing initial full sync for all tables in configured schemas")
                            # If no metadata, do initial full sync - get all schemas and tables
                            schemas = sync_engine.get_hana_schemas()
                            sync_metadata = []
                            for schema in schemas[:10]:  # Limit to first 10 schemas
                                tables = sync_engine.get_hana_tables(schema)
                                for table in tables:
                                    sync_metadata.append((schema, table['name']))
                                    # Setup sync metadata for future incremental syncs
                                    sync_engine.setup_incremental_sync(schema, table['name'])
                        
                        results = []
                        for schema, table in sync_metadata:
                            app.logger.info(f"Performing incremental sync for {schema}.{table}")
                            
                            # Try incremental sync first
                            result = sync_engine.perform_incremental_sync(schema, table)
                            
                            if result.get('status') == 'error':
                                # If incremental sync fails (e.g., no timestamp column), do full sync
                                app.logger.info(f"Incremental sync not available for {schema}.{table}, performing full sync")
                                
                                # Get table schema and create if needed
                                columns = sync_engine.get_hana_table_schema(schema, table)
                                if columns:
                                    sync_engine.create_clickhouse_table(schema, table, columns)
                                    result = sync_engine.migrate_table_data(schema, table)
                                else:
                                    app.logger.warning(f"No columns found for {schema}.{table}, skipping")
                                    continue
                            
                            results.append(result)
                            app.logger.info(f"Sync result for {schema}.{table}: {result.get('status', 'unknown')}")
                        
                        app.logger.info(f"HANA incremental sync completed for {len(results)} tables from source {source_id}")
                    finally:
                        sync_engine.close_connections()
                except Exception as e:
                    app.logger.exception(f"Error in HANA incremental sync for source {source_id}: {e}")
            
            # Start sync in background thread
            sync_thread = threading.Thread(target=background_incremental_sync, daemon=True)
            sync_thread.start()
            
            flash(f"Incremental sync started for HANA source '{source['source_name']}'", 'success')
            return jsonify({
                "success": True, 
                "message": f"Incremental sync started for HANA source '{source['source_name']}'",
                "source_id": source_id
            }), 202
        
        # Handle SQL Server sources
        elif source['source_type'] == 'sql_server':
            server_conf = {
                'server': source['server_address'],
                'username': source['username'],
                'password': source['password'],
                'target_postgres_db': None
            }

            # Use a unique name in the sync manager so we don't clash with YAML servers
            server_key = f"source::{source['id']}"
            result = sync_manager.start_sync(server_key, server_conf, app)

            if result['success']:
                flash(f"Background sync started for {source['source_name']}", 'success')
                return jsonify(result), 202
            else:
                flash(f"Failed to start sync for {source['source_name']}: {result.get('message')}", 'warning')
                return jsonify(result), 409
        else:
            return jsonify({"success": False, "message": f"Sync not supported for source type: {source['source_type']}"}), 501

    except Exception as e:
        app.logger.exception(f"Error starting source sync: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@app.route('/edit-source/<int:source_id>', methods=['GET', 'POST'])
@require_role(["admin", "operator"])
def edit_source(source_id):
    """Edit an existing data source (pre-fill forms)"""
    try:
        from db_utils import load_pg_config
        pg_conf = load_pg_config()
        conn = psycopg2.connect(
            dbname=pg_conf.get('database', 'metrics_sync_tables'),
            user=pg_conf.get('username'),
            password=pg_conf.get('password'),
            host=pg_conf.get('host'),
            port=int(pg_conf.get('port', 5432))
        )
        cur = conn.cursor()
        if request.method == 'POST':
            # Update record
            form = request.form
            source_name = form.get('source_name')
            server = form.get('server')
            username = form.get('username')
            password = form.get('password')
            target_type = form.get('target_type')
            target_database = form.get('target_database')
            cur.execute("UPDATE data_sources SET source_name=%s, server_address=%s, username=%s, password=%s, target_type=%s, target_database=%s, updated_at = CURRENT_TIMESTAMP WHERE id=%s",
                        (source_name, server, username, password, target_type, target_database, source_id))
            conn.commit()
            cur.close(); conn.close()
            flash('Source updated', 'success')
            return redirect(url_for('index'))

        cur.execute("SELECT id, source_name, source_type, server_address, username, target_type, target_database, connection_details FROM data_sources WHERE id = %s", (source_id,))
        row = cur.fetchone()
        cur.close(); conn.close()
        if not row:
            flash('Source not found', 'danger')
            return redirect(url_for('index'))

        ds = {'id': row[0], 'source_name': row[1], 'source_type': row[2], 'server_address': row[3], 'username': row[4], 'target_type': row[5], 'target_database': row[6], 'connection_details': row[7]}
        if ds['source_type'] == 'sql_server':
            return render_template('add_sql_source.html', source_name=ds['source_name'], server=ds['server_address'], username=ds['username'], target_type=ds['target_type'], target_database=ds['target_database'])
        else:
            return render_template('add_hana_source.html', source_name=ds['source_name'], host=ds['server_address'].split(':')[0], port=ds['server_address'].split(':')[1] if ':' in ds['server_address'] else '30015', username=ds['username'], target_type=ds['target_type'], target_database=ds['target_database'])

    except Exception as e:
        app.logger.exception(f"Error editing source: {e}")
        flash(f"Error editing source: {e}", 'danger')
        return redirect(url_for('index'))


@app.route('/delete-source/<int:source_id>', methods=['POST'])
@require_role(["admin", "operator"])
def delete_source_route(source_id):
    try:
        from db_utils import load_pg_config
        pg_conf = load_pg_config()
        conn = psycopg2.connect(
            dbname=pg_conf.get('database', 'metrics_sync_tables'),
            user=pg_conf.get('username'),
            password=pg_conf.get('password'),
            host=pg_conf.get('host'),
            port=int(pg_conf.get('port', 5432))
        )
        cur = conn.cursor()
        
        # First, get the source name before deletion
        cur.execute("SELECT source_name FROM data_sources WHERE id = %s", (source_id,))
        result = cur.fetchone()
        source_name = result[0] if result else None
        
        # Delete all related data before deleting the source
        # 1. Delete from schedules table
        cur.execute("DELETE FROM metrics_sync_tables.schedules WHERE server_name = %s", (source_name,))
        app.logger.info(f"[DELETE] Deleted {cur.rowcount} schedule(s) for source {source_name}")
        
        # 2. Delete from sync_history table
        cur.execute("DELETE FROM metrics_sync_tables.sync_history WHERE server_name = %s", (source_name,))
        app.logger.info(f"[DELETE] Deleted {cur.rowcount} sync history record(s) for source {source_name}")
        
        # 3. Delete from sync_database_status table
        cur.execute("DELETE FROM sync_database_status WHERE server_name = %s", (source_name,))
        app.logger.info(f"[DELETE] Deleted {cur.rowcount} database status record(s) for source {source_name}")
        
        # 4. Delete from sync_table_status table
        cur.execute("DELETE FROM sync_table_status WHERE server_name = %s", (source_name,))
        app.logger.info(f"[DELETE] Deleted {cur.rowcount} table status record(s) for source {source_name}")
        
        # 5. Finally, delete the source itself
        cur.execute("DELETE FROM data_sources WHERE id = %s", (source_id,))
        
        conn.commit()
        cur.close(); conn.close()
        flash('Source and all related data deleted successfully', 'success')
        app.logger.info(f"[DELETE] Successfully deleted source {source_name} (ID: {source_id}) and all related data")
    except Exception as e:
        app.logger.exception(f"Failed to delete source: {e}")
        flash(f'Failed to delete source: {e}', 'danger')
    return redirect(url_for('index'))


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


@app.route('/sync_status/source/<int:source_id>', methods=['GET'])
@require_role(["admin", "operator", "viewer"])
def sync_status_source(source_id):
    """Return sync status for a source started via sync_source_background (uses sync_manager key source::<id>)"""
    try:
        server_key = f"source::{source_id}"
        active_sync = sync_manager.get_sync_status(server_key)
        if active_sync:
            sync_status = active_sync["status"]
            is_actually_active = sync_status not in ["completed", "failed"]
            return jsonify({
                "server": server_key,
                "status": active_sync["status"],
                "progress": active_sync.get("progress", 0),
                "message": active_sync.get("message", ""),
                "sync_id": active_sync.get("sync_id"),
                "start_time": active_sync.get("start_time").isoformat() if active_sync.get("start_time") else None,
                "current_database": active_sync.get("current_database"),
                "databases_processed": active_sync.get("databases_processed", 0),
                "total_databases": active_sync.get("total_databases", 0),
                "is_active": is_actually_active
            }), 200

        # No active sync, return last recorded status if available
        last = get_last_sync_for_server(server_key)
        if not last:
            return jsonify({"server": server_key, "status": "none", "is_active": False}), 200

        return jsonify({
            "server": last["server"],
            "status": last["status"],
            "time": last["time"],
            "details": last["details"],
            "is_active": False
        }), 200

    except Exception as e:
        app.logger.exception(f"Error fetching sync status for source {source_id}: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/debug/data_sources', methods=['GET'])
@require_role(["admin"])
def debug_list_data_sources():
    """Debug endpoint: return JSON list of configured data_sources (admin-only).
    Useful to verify the running app can read the same DB where sources are inserted.
    """
    try:
        from db_utils import load_pg_config
        pg_conf = load_pg_config()
        conn = psycopg2.connect(
            dbname=pg_conf.get('database', 'metrics_sync_tables'),
            user=pg_conf.get('username'),
            password=pg_conf.get('password'),
            host=pg_conf.get('host'),
            port=int(pg_conf.get('port', 5432))
        )
        cur = conn.cursor()
        cur.execute("SELECT id, source_name, source_type, server_address, username, target_type, target_database, created_at FROM data_sources ORDER BY created_at DESC")
        rows = cur.fetchall()
        cur.close(); conn.close()

        ds_list = []
        for r in rows:
            ds_list.append({
                'id': r[0],
                'source_name': r[1],
                'source_type': r[2],
                'server_address': r[3],
                'username': r[4],
                'target_type': r[5],
                'target_database': r[6],
                'created_at': r[7].isoformat() if r[7] else None
            })

        return jsonify({'success': True, 'count': len(ds_list), 'data_sources': ds_list}), 200
    except Exception as e:
        app.logger.exception(f"Debug: could not read data_sources: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/debug/insert-sample', methods=['GET'])
@require_role(["admin"])
def debug_insert_sample_source():
    """Debug helper: insert a sample data_source record and redirect to index.
    Use query param `name` to set source_name. Protected to admin only.
    """
    try:
        name = request.args.get('name') or f"debug_sample_{int(datetime.now().timestamp())}"
        from db_utils import load_pg_config
        pg_conf = load_pg_config()
        conn = psycopg2.connect(
            dbname=pg_conf.get('database', 'metrics_sync_tables'),
            user=pg_conf.get('username'),
            password=pg_conf.get('password'),
            host=pg_conf.get('host'),
            port=int(pg_conf.get('port', 5432))
        )
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS data_sources (id SERIAL PRIMARY KEY, source_name VARCHAR(255) UNIQUE NOT NULL, source_type VARCHAR(50) NOT NULL, server_address TEXT NOT NULL, username TEXT NOT NULL, password TEXT NOT NULL, target_type VARCHAR(50) NOT NULL, target_database VARCHAR(255) NOT NULL, is_active BOOLEAN DEFAULT TRUE, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
        cur.execute("INSERT INTO data_sources (source_name, source_type, server_address, username, password, target_type, target_database) VALUES (%s,%s,%s,%s,%s,%s,%s)", (name, 'sql_server', '127.0.0.1', 'sa', 'Password123!', 'postgresql', 'postgres'))
        conn.commit()
        cur.close(); conn.close()
        flash(f"Inserted debug source '{name}'", 'success')
        app.logger.info(f"[DEBUG] Inserted sample data_source: {name}")
        return redirect(url_for('index'))
    except Exception as e:
        app.logger.exception(f"Debug insert failed: {e}")
        flash(f"Debug insert failed: {e}", 'danger')
        return redirect(url_for('index'))


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


def load_ch_databases():
    """Return list of ClickHouse databases, or [] if connection fails or driver missing.

    This function attempts a TCP connection via clickhouse-driver first. If that fails
    (authentication/connection), it falls back to the HTTP interface (/query) and
    requests the databases in JSON format. This makes discovery resilient across
    different ClickHouse deployments.
    """
    def _http_show_databases(host, port, user, password):
        """Use ClickHouse HTTP interface to fetch databases as JSONCompact."""
        try:
            import urllib.request
            import urllib.parse
            import json

            http_port = int(os.environ.get('CLICKHOUSE_HTTP_PORT', 8123)) if not port else (int(port) if str(port).isdigit() else 8123)
            url_host = host
            query = "SHOW DATABASES"
            params = {
                'query': query,
                'format': 'JSONCompact'
            }
            full_url = f"http://{url_host}:{http_port}/?{urllib.parse.urlencode(params)}"

            # Build opener with optional basic auth
            if user:
                password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
                password_mgr.add_password(None, full_url, user, password or '')
                handler = urllib.request.HTTPBasicAuthHandler(password_mgr)
                opener = urllib.request.build_opener(handler)
            else:
                opener = urllib.request.build_opener()

            with opener.open(full_url, timeout=5) as resp:
                body = resp.read()
                try:
                    parsed = json.loads(body.decode('utf-8'))
                except Exception:
                    return []

                # JSONCompact.data is list of lists
                data = parsed.get('data') or []
                dbs = [row[0] for row in data if isinstance(row, (list, tuple)) and len(row) > 0]
                return dbs
        except Exception as e:
            app.logger.debug(f"HTTP ClickHouse discovery failed: {e}")
            return []

    try:
        # If driver is missing, try HTTP only
        if CHClient is None:
            app.logger.warning("clickhouse-driver is not installed; trying HTTP discovery for ClickHouse")
            return _http_show_databases(os.environ.get('CLICKHOUSE_HOST'), os.environ.get('CLICKHOUSE_PORT'), os.environ.get('CLICKHOUSE_USER'), os.environ.get('CLICKHOUSE_PASSWORD', ''))

        # Read env vars; password defaults to empty string (not None)
        ch_host = os.environ.get('CLICKHOUSE_HOST')
        ch_port = os.environ.get('CLICKHOUSE_PORT')
        ch_user = os.environ.get('CLICKHOUSE_USER')
        ch_password = os.environ.get('CLICKHOUSE_PASSWORD', '')

        if not ch_host or not ch_port or not ch_user:
            app.logger.warning('CLICKHOUSE_HOST, CLICKHOUSE_PORT and CLICKHOUSE_USER must be set in environment to discover ClickHouse databases')
            # Try HTTP without full env set — try defaults
            return _http_show_databases(os.environ.get('CLICKHOUSE_HOST', 'localhost'), os.environ.get('CLICKHOUSE_PORT', '8123'), os.environ.get('CLICKHOUSE_USER', ''), os.environ.get('CLICKHOUSE_PASSWORD', ''))

        client = CHClient(host=ch_host, port=int(ch_port), user=ch_user, password=ch_password)
        rows = client.execute('SHOW DATABASES')
        dbs = []
        for r in rows:
            if isinstance(r, (list, tuple)):
                dbs.append(r[0])
            else:
                dbs.append(r)
        return dbs
    except Exception as e:
        app.logger.error(f"WARNING: Could not load ClickHouse DBs: {e}")
        # Try HTTP fallback if TCP failed
        try:
            return _http_show_databases(os.environ.get('CLICKHOUSE_HOST'), os.environ.get('CLICKHOUSE_PORT'), os.environ.get('CLICKHOUSE_USER'), os.environ.get('CLICKHOUSE_PASSWORD', ''))
        except Exception:
            return []


def load_clickhouse_databases():
    """Wrapper for load_ch_databases for consistency"""
    return load_ch_databases()


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


@app.route("/test-hana-connection", methods=["POST"])
@require_role(["admin", "operator"])
def test_hana_connection():
    """Test SAP HANA connection via AJAX if hdbcli is available"""
    try:
        data = request.get_json()
        host = (data.get("host") or "").strip()
        port = (data.get("port") or "").strip() or "30015"
        username = (data.get("username") or "").strip()
        password = (data.get("password") or "").strip()

        if not all([host, port, username, password]):
            return jsonify({"success": False, "message": "All fields are required"}), 400

        # Try importing hdbcli
        try:
            from hdbcli import dbapi as hana_dbapi
        except Exception:
            # Inform frontend that hdbcli is missing
            return jsonify({
                "success": False,
                "message": "hdbcli is not installed on the server. Install the 'hdbcli' package to enable immediate HANA connection testing. Connection will be validated during first sync otherwise."
            }), 400

        # Attempt to connect and fetch database list
        try:
            conn = hana_dbapi.connect(address=host, port=int(port), user=username, password=password)
            cursor = conn.cursor()
            
            # Query to get all schemas/databases
            cursor.execute("SELECT SCHEMA_NAME FROM SYS.SCHEMAS WHERE SCHEMA_NAME NOT IN ('_SYS_BIC', '_SYS_EPM', 'SYS', 'SYSTEM', '_SYS_REPO') ORDER BY SCHEMA_NAME")
            databases = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            conn.close()
            
            return jsonify({
                "success": True, 
                "message": "Connection successful!",
                "databases": databases
            })
        except Exception as e:
            app.logger.exception(f"HANA connection test failed: {e}")
            return jsonify({"success": False, "message": f"Connection failed: {str(e)}"}), 400

    except Exception as e:
        app.logger.exception(f"Error testing HANA connection: {e}")
        return jsonify({"success": False, "message": f"Connection test error: {str(e)}"}), 500


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


# ------------------ Add Source Routes ------------------

@app.route("/add-source")
@require_role(["admin", "operator"])
def add_source_page():
    """Show source type selection page"""
    return render_template("add_source.html")


@app.route("/add-source/sql", methods=["GET", "POST"])
@require_role(["admin", "operator"])
def add_sql_source():
    """Add SQL Server as a data source"""
    if request.method == "POST":
        try:
            source_name = request.form.get("source_name", "").strip()
            server = request.form.get("server", "").strip()
            username = request.form.get("username", "").strip()
            password = request.form.get("password", "").strip()
            target_type = request.form.get("target_type", "").strip()
            target_database = request.form.get("target_database", "").strip()
            
            # Validate required fields
            if not all([source_name, server, username, password, target_type, target_database]):
                flash("All fields are required!", "danger")
                postgres_dbs = load_pg_databases()
                clickhouse_dbs = load_clickhouse_databases()
                return render_template("add_sql_source.html",
                                      postgres_dbs=postgres_dbs,
                                      clickhouse_dbs=clickhouse_dbs,
                                      source_name=source_name,
                                      server=server,
                                      username=username,
                                      target_type=target_type)
            
            # Test SQL Server connection
            server_conf = {
                "server": server,
                "username": username,
                "password": password,
            }
            
            app.logger.info(f"Testing SQL Server connection to {server}")
            success, error = test_sql_connection(server_conf)
            
            if not success:
                diagnostic_info = ""
                if '\\' in server:
                    diagnostic_info = (
                        " For named instances, ensure SQL Browser service is running "
                        "and UDP port 1434 is accessible."
                    )
                flash(f"SQL Server connection failed! {error}{diagnostic_info}", "danger")
                postgres_dbs = load_pg_databases()
                clickhouse_dbs = load_clickhouse_databases()
                return render_template("add_sql_source.html",
                                      postgres_dbs=postgres_dbs,
                                      clickhouse_dbs=clickhouse_dbs,
                                      source_name=source_name,
                                      server=server,
                                      username=username,
                                      target_type=target_type)
            
            # Save source configuration to database
            from db_utils import load_pg_config
            pg_conf = load_pg_config()
            conn = psycopg2.connect(
                dbname=pg_conf.get('database', 'metrics_sync_tables'),
                user=pg_conf.get("username"),
                password=pg_conf.get("password"),
                host=pg_conf.get("host"),
                port=int(pg_conf.get("port", 5432))
            )
            cursor = conn.cursor()
            
            # Create sources table if it doesn't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS data_sources (
                    id SERIAL PRIMARY KEY,
                    source_name VARCHAR(255) UNIQUE NOT NULL,
                    source_type VARCHAR(50) NOT NULL,
                    server_address TEXT NOT NULL,
                    username TEXT NOT NULL,
                    password TEXT NOT NULL,
                    target_type VARCHAR(50) NOT NULL,
                    target_database VARCHAR(255) NOT NULL,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Insert the new source
            cursor.execute("""
                INSERT INTO data_sources 
                (source_name, source_type, server_address, username, password, target_type, target_database)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (source_name, 'sql_server', server, username, password, target_type, target_database))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            flash(f"SQL Server source '{source_name}' added successfully!", "success")
            return redirect(url_for("add_source_page"))
            
        except psycopg2.IntegrityError:
            flash(f"Source '{source_name}' already exists!", "danger")
            postgres_dbs = load_pg_databases()
            clickhouse_dbs = load_clickhouse_databases()
            return render_template("add_sql_source.html",
                                  postgres_dbs=postgres_dbs,
                                  clickhouse_dbs=clickhouse_dbs)
        except Exception as e:
            app.logger.exception(f"Error adding SQL source: {e}")
            flash(f"Error adding SQL source: {str(e)}", "danger")
            postgres_dbs = load_pg_databases()
            clickhouse_dbs = load_clickhouse_databases()
            return render_template("add_sql_source.html",
                                  postgres_dbs=postgres_dbs,
                                  clickhouse_dbs=clickhouse_dbs)
    
    # GET request
    postgres_dbs = load_pg_databases()
    clickhouse_dbs = load_clickhouse_databases()
    return render_template("add_sql_source.html",
                          postgres_dbs=postgres_dbs,
                          clickhouse_dbs=clickhouse_dbs)


@app.route("/add-source/hana", methods=["GET", "POST"])
@require_role(["admin", "operator"])
def add_hana_source():
    """Add SAP HANA as a data source"""
    if request.method == "POST":
        try:
            source_name = request.form.get("source_name", "").strip()
            host = request.form.get("host", "").strip()
            port = request.form.get("port", "30015").strip()
            instance = request.form.get("instance", "").strip()
            username = request.form.get("username", "").strip()
            password = request.form.get("password", "").strip()
            hana_database = request.form.get("hana_database", "").strip()
            target_type = request.form.get("target_type", "").strip()
            target_database = request.form.get("target_database", "").strip()
            
            # Validate required fields
            if not all([source_name, host, port, username, password, target_type, target_database]):
                flash("All required fields must be filled!", "danger")
                postgres_dbs = load_pg_databases()
                clickhouse_dbs = load_clickhouse_databases()
                return render_template("add_hana_source.html",
                                      postgres_dbs=postgres_dbs,
                                      clickhouse_dbs=clickhouse_dbs,
                                      source_name=source_name,
                                      host=host,
                                      port=port,
                                      instance=instance,
                                      username=username,
                                      target_type=target_type)
            
            # Note: HANA connection testing would require hdbcli library
            # For now, we'll save without testing (user can test separately)
            app.logger.info(f"Adding SAP HANA source: {host}:{port}")
            
            # Save source configuration to database
            from db_utils import load_pg_config
            pg_conf = load_pg_config()
            conn = psycopg2.connect(
                dbname=pg_conf.get('database', 'metrics_sync_tables'),
                user=pg_conf.get("username"),
                password=pg_conf.get("password"),
                host=pg_conf.get("host"),
                port=int(pg_conf.get("port", 5432))
            )
            cursor = conn.cursor()
            
            # Create sources table if it doesn't exist (same as SQL Server route)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS data_sources (
                    id SERIAL PRIMARY KEY,
                    source_name VARCHAR(255) UNIQUE NOT NULL,
                    source_type VARCHAR(50) NOT NULL,
                    server_address TEXT NOT NULL,
                    username TEXT NOT NULL,
                    password TEXT NOT NULL,
                    target_type VARCHAR(50) NOT NULL,
                    target_database VARCHAR(255) NOT NULL,
                    connection_details JSONB,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Store HANA-specific details in connection_details JSON
            connection_details = {
                "host": host,
                "port": port,
                "instance": instance,
                "hana_database": hana_database
            }
            
            # Insert the new source
            cursor.execute("""
                INSERT INTO data_sources 
                (source_name, source_type, server_address, username, password, 
                 target_type, target_database, connection_details)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (source_name, 'sap_hana', f"{host}:{port}", username, password, 
                  target_type, target_database, json.dumps(connection_details)))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            flash(f"SAP HANA source '{source_name}' added successfully!", "success")
            return redirect(url_for("add_source_page"))
            
        except psycopg2.IntegrityError:
            flash(f"Source '{source_name}' already exists!", "danger")
            postgres_dbs = load_pg_databases()
            clickhouse_dbs = load_clickhouse_databases()
            return render_template("add_hana_source.html",
                                  postgres_dbs=postgres_dbs,
                                  clickhouse_dbs=clickhouse_dbs)
        except Exception as e:
            app.logger.exception(f"Error adding HANA source: {e}")
            flash(f"Error adding HANA source: {str(e)}", "danger")
            postgres_dbs = load_pg_databases()
            clickhouse_dbs = load_clickhouse_databases()
            return render_template("add_hana_source.html",
                                  postgres_dbs=postgres_dbs,
                                  clickhouse_dbs=clickhouse_dbs)
    
    # GET request
    postgres_dbs = load_pg_databases()
    clickhouse_dbs = load_clickhouse_databases()
    return render_template("add_hana_source.html",
                          postgres_dbs=postgres_dbs,
                          clickhouse_dbs=clickhouse_dbs)


@app.route("/view-hana-tables/<int:source_id>")
@require_role(["admin", "operator", "viewer"])
def view_hana_tables_page(source_id):
    """Render HANA tables browser page"""
    try:
        from db_utils import load_pg_config
        pg_conf = load_pg_config()
        conn = psycopg2.connect(
            dbname=pg_conf.get('database', 'metrics_sync_tables'),
            user=pg_conf.get("username"),
            password=pg_conf.get("password"),
            host=pg_conf.get("host"),
            port=int(pg_conf.get("port", 5432))
        )
        cursor = conn.cursor()
        cursor.execute("SELECT source_name FROM data_sources WHERE id = %s", (source_id,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not row:
            flash("Source not found", "danger")
            return redirect(url_for("index"))
        
        return render_template("view_hana_tables.html", source_id=source_id, source_name=row[0])
    except Exception as e:
        app.logger.exception(f"Error loading HANA tables page: {e}")
        flash(f"Error: {str(e)}", "danger")
        return redirect(url_for("index"))


@app.route("/api/view-hana-tables/<int:source_id>")
@require_role(["admin", "operator", "viewer"])
def view_hana_tables(source_id):
    """Get schemas and tables from HANA source (JSON API)"""
    try:
        from hana_sync import HanaToClickHouseSync
        
        # Get source from database
        from db_utils import load_pg_config
        pg_conf = load_pg_config()
        conn = psycopg2.connect(
            dbname=pg_conf.get('database', 'metrics_sync_tables'),
            user=pg_conf.get("username"),
            password=pg_conf.get("password"),
            host=pg_conf.get("host"),
            port=int(pg_conf.get("port", 5432))
        )
        cursor = conn.cursor()
        cursor.execute("SELECT source_name, server_address, username, password, connection_details, target_database FROM data_sources WHERE id = %s", (source_id,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not row:
            return jsonify({'status': 'error', 'message': 'Source not found'}), 404
        
        source_name, server_address, username, password, connection_details_json, target_database = row
        connection_details = json.loads(connection_details_json) if connection_details_json else {}
        
        # Parse server address (format: host:port)
        if ':' in server_address:
            host, port = server_address.split(':', 1)
        else:
            host = server_address
            port = connection_details.get('port', '30015')
        
        # Prepare HANA config
        hana_config = {
            'host': host,
            'port': int(port),
            'username': username,
            'password': password
        }
        
        # Get ClickHouse config
        clickhouse_config = {
            'host': os.getenv('CLICKHOUSE_HOST', 'localhost'),
            'port': int(os.getenv('CLICKHOUSE_PORT', '9000')),
            'user': os.getenv('CLICKHOUSE_USER', 'default'),
            'password': os.getenv('CLICKHOUSE_PASSWORD', ''),
            'database': target_database if target_database else 'hana_migrated'
        }
        
        # Create sync engine and connect
        sync_engine = HanaToClickHouseSync(hana_config, clickhouse_config)
        
        if not sync_engine.connect_hana():
            return jsonify({'status': 'error', 'message': 'Failed to connect to HANA database'}), 400
        
        try:
            # Get schemas
            schemas = sync_engine.get_hana_schemas()
            tables_by_schema = {}
            
            # Limit to first 20 schemas for performance
            for schema in schemas[:20]:
                tables = sync_engine.get_hana_tables(schema)
                # Add ClickHouse target table names
                for table in tables:
                    ch_table_name = sync_engine.create_clickhouse_table_name(schema, table['name'])
                    table['clickhouse_table'] = f"{clickhouse_config['database']}.{ch_table_name}"
                tables_by_schema[schema] = tables
            
            return jsonify({
                'status': 'success',
                'schemas': schemas,
                'tables_by_schema': tables_by_schema,
                'source_id': source_id,
                'source_name': source_name
            })
        finally:
            sync_engine.close_connections()
            
    except Exception as e:
        app.logger.exception(f"Error loading HANA tables: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route("/sync-hana-source/<int:source_id>", methods=["POST"])
@require_role(["admin", "operator"])
def sync_hana_source(source_id):
    """Trigger HANA to ClickHouse sync"""
    try:
        from hana_sync import HanaToClickHouseSync
        import threading
        
        data = request.get_json() if request.is_json else {}
        tables_to_sync = data.get('tables', [])
        enable_incremental = data.get('enable_incremental', False)
        
        if not tables_to_sync:
            return jsonify({'status': 'error', 'message': 'No tables selected for sync'}), 400
        
        # Get source from database
        from db_utils import load_pg_config
        pg_conf = load_pg_config()
        conn = psycopg2.connect(
            dbname=pg_conf.get('database', 'metrics_sync_tables'),
            user=pg_conf.get("username"),
            password=pg_conf.get("password"),
            host=pg_conf.get("host"),
            port=int(pg_conf.get("port", 5432))
        )
        cursor = conn.cursor()
        cursor.execute("SELECT source_name, server_address, username, password, connection_details, target_database FROM data_sources WHERE id = %s", (source_id,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not row:
            return jsonify({'status': 'error', 'message': 'Source not found'}), 404
        
        source_name, server_address, username, password, connection_details_json, target_database = row
        connection_details = json.loads(connection_details_json) if connection_details_json else {}
        
        # Parse server address
        if ':' in server_address:
            host, port = server_address.split(':', 1)
        else:
            host = server_address
            port = connection_details.get('port', '30015')
        
        # Prepare configs
        hana_config = {
            'host': host,
            'port': int(port),
            'username': username,
            'password': password
        }
        
        clickhouse_config = {
            'host': os.getenv('CLICKHOUSE_HOST', 'localhost'),
            'port': int(os.getenv('CLICKHOUSE_PORT', '9000')),
            'user': os.getenv('CLICKHOUSE_USER', 'default'),
            'password': os.getenv('CLICKHOUSE_PASSWORD', ''),
            'database': target_database if target_database else 'hana_migrated'
        }
        
        # Run sync in background thread
        def background_sync():
            try:
                sync_engine = HanaToClickHouseSync(hana_config, clickhouse_config)
                
                if not (sync_engine.connect_hana() and sync_engine.connect_clickhouse()):
                    app.logger.error("Failed to connect to HANA or ClickHouse")
                    return
                
                try:
                    results = []
                    for table_spec in tables_to_sync:
                        schema = table_spec.get('schema') or table_spec.get('source_schema')
                        table = table_spec.get('table') or table_spec.get('source_table')
                        
                        if not schema or not table:
                            continue
                        
                        app.logger.info(f"Syncing {schema}.{table} from HANA source {source_id}")
                        
                        # Get table schema and create in ClickHouse
                        columns = sync_engine.get_hana_table_schema(schema, table)
                        if not columns:
                            app.logger.warning(f"No columns found for {schema}.{table}")
                            continue
                        
                        sync_engine.create_clickhouse_table(schema, table, columns)
                        
                        # Migrate data
                        result = sync_engine.migrate_table_data(schema, table)
                        results.append(result)
                        
                        # Setup incremental sync if requested
                        if enable_incremental or table_spec.get('enable_incremental', False):
                            sync_engine.setup_incremental_sync(schema, table)
                    
                    app.logger.info(f"HANA sync completed for {len(results)} tables")
                finally:
                    sync_engine.close_connections()
            except Exception as e:
                app.logger.exception(f"Error in HANA background sync: {e}")
        
        # Start background thread
        thread = threading.Thread(target=background_sync, daemon=True)
        thread.start()
        
        return jsonify({
            'status': 'success',
            'message': f'Sync started for {len(tables_to_sync)} tables. Check logs for progress.',
            'tables_count': len(tables_to_sync)
        })
        
    except Exception as e:
        app.logger.exception(f"Error starting HANA sync: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route("/add-source/api", methods=["GET", "POST"])
@require_role(["admin", "operator"])
def add_api_source():
    """Add REST API as a data source"""
    if request.method == "POST":
        try:
            source_name = request.form.get("source_name", "").strip()
            api_url = request.form.get("api_url", "").strip()
            auth_type = request.form.get("auth_type", "none").strip()
            auth_token = request.form.get("auth_token", "").strip()
            basic_username = request.form.get("basic_username", "").strip()
            basic_password = request.form.get("basic_password", "").strip()
            apikey_header = request.form.get("apikey_header", "X-API-Key").strip()
            custom_headers = request.form.get("custom_headers", "").strip()
            request_method = request.form.get("request_method", "GET").strip()
            data_path = request.form.get("data_path", "").strip()
            target_type = request.form.get("target_type", "").strip()
            target_database = request.form.get("target_database", "").strip()
            is_sse = request.form.get("is_sse", "false").strip().lower() == "true"
            
            # OAuth parameters
            oauth_token_url = request.form.get("oauth_token_url", "").strip()
            oauth_username = request.form.get("oauth_username", "").strip()
            oauth_password = request.form.get("oauth_password", "").strip()
            oauth_refresh_interval = int(request.form.get("oauth_refresh_interval", "3600"))
            
            # Auto-generate table name from source name if not provided
            target_table = request.form.get("target_table", "").strip()
            if not target_table:
                # Create table name from source name with "crm_" prefix (e.g., "crm1" -> "crm_crm1")
                clean_source_name = source_name.lower().replace(' ', '_').replace('-', '_')
                target_table = f"crm_{clean_source_name}"
            
            # Validate required fields
            if not all([source_name, api_url, target_type, target_database]):
                flash("Source name, API URL, target type, and database are required!", "danger")
                return render_template("add_api_source.html",
                                      source_name=source_name,
                                      api_url=api_url,
                                      target_type=target_type)
            
            # Validate URL format
            if not api_url.startswith(('http://', 'https://')):
                flash("API URL must start with http:// or https://", "danger")
                return render_template("add_api_source.html",
                                      source_name=source_name,
                                      api_url=api_url,
                                      target_type=target_type)
            
            app.logger.info(f"Adding REST API source: {api_url}")
            
            # Save source configuration to database
            from db_utils import load_pg_config
            pg_conf = load_pg_config()
            conn = psycopg2.connect(
                dbname=pg_conf.get('database', 'metrics_sync_tables'),
                user=pg_conf.get("username"),
                password=pg_conf.get("password"),
                host=pg_conf.get("host"),
                port=int(pg_conf.get("port", 5432))
            )
            cursor = conn.cursor()
            
            # Create sources table if it doesn't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS data_sources (
                    id SERIAL PRIMARY KEY,
                    source_name VARCHAR(255) UNIQUE NOT NULL,
                    source_type VARCHAR(50) NOT NULL,
                    server_address TEXT NOT NULL,
                    username TEXT,
                    password TEXT,
                    target_type VARCHAR(50) NOT NULL,
                    target_database VARCHAR(255) NOT NULL,
                    connection_details JSONB,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Store API-specific details in connection_details JSON
            polling_mode = request.form.get("polling_mode", "false") == "true"
            poll_interval = int(request.form.get("poll_interval", "5"))
            id_column = request.form.get("id_column", "id")
            upsert_mode = request.form.get("upsert_mode", "false") == "true"
            
            connection_details = {
                "api_url": api_url,
                "auth_type": auth_type,
                "auth_token": auth_token if auth_type in ['bearer', 'apikey'] else None,
                "basic_username": basic_username if auth_type == 'basic' else None,
                "basic_password": basic_password if auth_type == 'basic' else None,
                "apikey_header": apikey_header if auth_type == 'apikey' else None,
                "oauth_token_url": oauth_token_url if auth_type == 'oauth' else None,
                "oauth_username": oauth_username if auth_type == 'oauth' else None,
                "oauth_password": oauth_password if auth_type == 'oauth' else None,
                "oauth_refresh_interval": oauth_refresh_interval if auth_type == 'oauth' else 3600,
                "custom_headers": custom_headers,
                "request_method": request_method,
                "data_path": data_path,
                "target_table": target_table,
                "is_sse": is_sse,
                "polling_mode": polling_mode,
                "poll_interval": poll_interval,
                "id_column": id_column,
                "upsert_mode": upsert_mode
            }
            
            # Insert the new source
            cursor.execute("""
                INSERT INTO data_sources 
                (source_name, source_type, server_address, username, password, 
                 target_type, target_database, connection_details)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (source_name, 'rest_api', api_url, None, None, 
                  target_type, target_database, json.dumps(connection_details)))
            
            source_id = cursor.fetchone()[0]
            
            conn.commit()
            cursor.close()
            conn.close()
            
            # Start sync in background thread if target is ClickHouse
            if target_type.lower() == "clickhouse":
                import threading
                from api_sync import sync_api_to_clickhouse_once, sync_api_to_clickhouse
                from api_polling import poll_api_to_clickhouse
                
                def background_sync():
                    try:
                        app.logger.info(f"Starting background sync for API source: {source_name}")
                        
                        # Parse custom headers
                        headers = {}
                        if custom_headers:
                            try:
                                headers = json.loads(custom_headers)
                            except:
                                pass
                        
                        # Determine sync mode: upsert, polling, SSE, or one-time
                        if polling_mode and upsert_mode:
                            # UPSERT mode: Insert new + update existing
                            app.logger.info(f"Starting UPSERT mode for '{source_name}' (every {poll_interval}s)")
                            from api_upsert import upsert_api_to_clickhouse
                            upsert_api_to_clickhouse(
                                api_url=api_url,
                                target_database=target_database,
                                target_table=target_table,
                                auth_type=auth_type,
                                auth_token=auth_token,
                                basic_username=basic_username,
                                basic_password=basic_password,
                                apikey_header=apikey_header,
                                custom_headers=headers,
                                request_method=request_method,
                                data_path=data_path,
                                poll_interval=poll_interval,
                                id_column=id_column,
                                auto_create_table=True,
                                oauth_token_url=oauth_token_url,
                                oauth_username=oauth_username,
                                oauth_password=oauth_password,
                                oauth_refresh_interval=oauth_refresh_interval
                            )
                        elif polling_mode:
                            # Polling mode: Continuously check API for new data
                            app.logger.info(f"Starting POLLING mode for '{source_name}' (every {poll_interval}s)")
                            poll_api_to_clickhouse(
                                api_url=api_url,
                                target_database=target_database,
                                target_table=target_table,
                                auth_type=auth_type,
                                auth_token=auth_token,
                                basic_username=basic_username,
                                basic_password=basic_password,
                                apikey_header=apikey_header,
                                custom_headers=headers,
                                request_method=request_method,
                                data_path=data_path,
                                poll_interval=poll_interval,
                                id_column=id_column,
                                auto_create_table=True,
                                oauth_token_url=oauth_token_url,
                                oauth_username=oauth_username,
                                oauth_password=oauth_password,
                                oauth_refresh_interval=oauth_refresh_interval
                            )
                        elif is_sse:
                            # SSE streams need continuous monitoring
                            app.logger.info(f"Starting SSE mode for '{source_name}'")
                            sync_api_to_clickhouse(
                                api_url=api_url,
                                target_database=target_database,
                                target_table=target_table,
                                auth_type=auth_type,
                                auth_token=auth_token,
                                basic_username=basic_username,
                                basic_password=basic_password,
                                apikey_header=apikey_header,
                                custom_headers=headers,
                                request_method=request_method,
                                data_path=data_path,
                                is_sse=is_sse,
                                auto_create_table=True
                            )
                        else:
                            # Regular REST APIs: sync once immediately
                            app.logger.info(f"Starting ONE-TIME sync for '{source_name}'")
                            result = sync_api_to_clickhouse_once(
                                api_url=api_url,
                                target_database=target_database,
                                target_table=target_table,
                                auth_type=auth_type,
                                auth_token=auth_token,
                                basic_username=basic_username,
                                basic_password=basic_password,
                                apikey_header=apikey_header,
                                custom_headers=headers,
                                request_method=request_method,
                                data_path=data_path,
                                auto_create_table=True
                            )
                            app.logger.info(f"Sync result: {result}")
                    except Exception as e:
                        app.logger.error(f"Background sync error: {e}")
                
                sync_thread = threading.Thread(target=background_sync, daemon=True)
                sync_thread.start()
                
                if polling_mode:
                    flash(f"REST API source '{source_name}' added with continuous polling (every {poll_interval}s)!", "success")
                else:
                    flash(f"REST API source '{source_name}' added and sync started in background!", "success")
            else:
                flash(f"REST API source '{source_name}' added successfully!", "success")
            
            return redirect(url_for("index"))
            
        except psycopg2.IntegrityError:
            flash(f"Source '{source_name}' already exists!", "danger")
            return render_template("add_api_source.html")
        except Exception as e:
            app.logger.exception(f"Error adding API source: {e}")
            flash(f"Error adding API source: {str(e)}", "danger")
            return render_template("add_api_source.html")
    
    # GET request
    return render_template("add_api_source.html")


@app.route("/test_api_connection", methods=["POST"])
@require_role(["admin", "operator"])
def test_api_connection():
    """Test API connection and fetch sample data"""
    import time
    try:
        data = request.get_json()
        api_url = data.get("api_url", "").strip()
        auth_type = data.get("auth_type", "none")
        auth_token = data.get("auth_token", "").strip()
        basic_username = data.get("basic_username", "").strip()
        basic_password = data.get("basic_password", "").strip()
        apikey_header = data.get("apikey_header", "X-API-Key").strip()
        custom_headers_str = data.get("custom_headers", "").strip()
        request_method = data.get("request_method", "GET")
        data_path = data.get("data_path", "").strip()
        target_type = data.get("target_type", "")
        target_database = data.get("target_database", "")
        
        # OAuth parameters
        oauth_token_url = data.get("oauth_token_url", "").strip()
        oauth_username = data.get("oauth_username", "").strip()
        oauth_password = data.get("oauth_password", "").strip()
        
        # Parse custom headers
        headers = {}
        if custom_headers_str:
            try:
                headers = json.loads(custom_headers_str)
            except:
                pass
        
        # Add authentication
        if auth_type == "bearer" and auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"
        elif auth_type == "apikey" and auth_token and apikey_header:
            headers[apikey_header] = auth_token
        elif auth_type == "oauth" and oauth_token_url and oauth_username and oauth_password:
            # For OAuth, get token first
            try:
                app.logger.info(f"Testing OAuth: Requesting token from {oauth_token_url}")
                token_response = requests.post(
                    oauth_token_url,
                    json={"username": oauth_username, "password": oauth_password},
                    timeout=10
                )
                
                if token_response.status_code != 200:
                    return jsonify({
                        "success": False,
                        "error": f"OAuth token request failed: HTTP {token_response.status_code}",
                        "details": token_response.text
                    })
                
                token_data = token_response.json()
                oauth_token = token_data.get('token') or token_data.get('access_token') or token_data.get('oauth_token')
                
                if not oauth_token:
                    # Try to find any string field that looks like a token
                    for key, value in token_data.items():
                        if isinstance(value, str) and len(value) > 20:
                            oauth_token = value
                            break
                
                if oauth_token:
                    headers["Authorization"] = f"Bearer {oauth_token}"
                    app.logger.info(f"OAuth token obtained successfully")
                else:
                    return jsonify({
                        "success": False,
                        "error": "No token found in OAuth response",
                        "response": token_data
                    })
                    
            except Exception as e:
                return jsonify({
                    "success": False,
                    "error": f"OAuth token request failed: {str(e)}"
                })
        
        # Prepare auth for basic authentication
        auth = None
        if auth_type == "basic" and basic_username and basic_password:
            auth = (basic_username, basic_password)
        
        # Check if this might be an SSE endpoint
        is_sse = data.get("is_sse", False)
        
        # Make API request
        start_time = time.time()
        
        if is_sse:
            # For SSE streams, just check if we can connect and read first event
            try:
                if request_method == "GET":
                    response = requests.get(api_url, headers=headers, auth=auth, timeout=5, stream=True)
                else:
                    response = requests.post(api_url, headers=headers, auth=auth, timeout=5, stream=True)
                
                response_time = round(time.time() - start_time, 2)
                
                if response.status_code != 200:
                    return jsonify({
                        "success": False,
                        "error": f"HTTP {response.status_code}: {response.reason}"
                    })
                
                # Try to read first SSE event
                sample_data = None
                for line in response.iter_lines():
                    if line:
                        decoded = line.decode('utf-8')
                        if decoded.startswith('data:'):
                            try:
                                json_str = decoded[5:].strip()
                                sample_data = json.loads(json_str)
                                break
                            except:
                                pass
                
                return jsonify({
                    "success": True,
                    "status_code": response.status_code,
                    "response_time": response_time,
                    "record_count": "Streaming (continuous)",
                    "sample_data": sample_data,
                    "is_sse": True
                })
            except Exception as e:
                return jsonify({
                    "success": False,
                    "error": f"SSE connection failed: {str(e)}"
                })
        else:
            # Regular REST API
            if request_method == "GET":
                response = requests.get(api_url, headers=headers, auth=auth, timeout=10)
            else:
                response = requests.post(api_url, headers=headers, auth=auth, timeout=10)
            
            response_time = round(time.time() - start_time, 2)
            
            # Check if successful
            if response.status_code != 200:
                return jsonify({
                    "success": False,
                    "error": f"HTTP {response.status_code}: {response.reason}"
                })
            
            # Parse JSON response
            try:
                json_data = response.json()
            except:
                return jsonify({
                    "success": False,
                    "error": "Response is not valid JSON"
                })
            
            # Navigate to data path if specified
            records = json_data
            if data_path:
                for key in data_path.split('.'):
                    if isinstance(records, dict) and key in records:
                        records = records[key]
                    else:
                        return jsonify({
                            "success": False,
                            "error": f"Data path '{data_path}' not found in response"
                        })
            
            # Determine record count
            record_count = 0
            sample_data = None
            if isinstance(records, list):
                record_count = len(records)
                sample_data = records[0] if records else None
            elif isinstance(records, dict):
                record_count = 1
                sample_data = records
        
        # Test target database connection
        target_db_status = "Not tested"
        try:
            if target_type.lower() == "postgresql":
                from db_utils import load_pg_config
                pg_conf = load_pg_config()
                test_conn = psycopg2.connect(
                    dbname=target_database,
                    user=pg_conf.get("username"),
                    password=pg_conf.get("password"),
                    host=pg_conf.get("host"),
                    port=int(pg_conf.get("port", 5432))
                )
                test_conn.close()
                target_db_status = "Connected ✓"
            elif target_type.lower() == "clickhouse":
                from clickhouse_driver import Client
                from db_utils import load_clickhouse_config
                ch_conf = load_clickhouse_config()
                client = Client(
                    host=ch_conf.get('host', 'localhost'),
                    port=int(ch_conf.get('port', 9000)),
                    user=ch_conf.get('user', 'default'),
                    password=ch_conf.get('password', ''),
                    database=target_database
                )
                client.execute('SELECT 1')
                target_db_status = "Connected ✓"
        except Exception as e:
            target_db_status = f"Failed: {str(e)}"
        
        return jsonify({
            "success": True,
            "status_code": response.status_code,
            "response_time": response_time,
            "record_count": record_count,
            "sample_data": sample_data,
            "target_db_status": target_db_status
        })
        
    except requests.exceptions.Timeout:
        return jsonify({
            "success": False,
            "error": "Request timed out (10s limit)"
        })
    except requests.exceptions.ConnectionError:
        return jsonify({
            "success": False,
            "error": "Could not connect to API endpoint"
        })
    except Exception as e:
        app.logger.exception(f"Error testing API connection: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        })


@app.route("/api/target/databases")
@require_role(["admin", "operator"])
def get_target_databases():
    """Get list of databases from selected target (PostgreSQL or ClickHouse)"""
    try:
        target_type = request.args.get("target_type", "").strip()
        
        if target_type == "postgresql":
            databases = load_pg_databases()
            return jsonify({"success": True, "databases": databases})
        elif target_type == "clickhouse":
            databases = load_clickhouse_databases()
            return jsonify({"success": True, "databases": databases})
        else:
            return jsonify({"success": False, "message": "Invalid target type"}), 400
            
    except Exception as e:
        app.logger.exception(f"Error fetching target databases: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


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
    postgres_dbs = load_pg_databases()
    clickhouse_dbs = load_ch_databases()

    if request.method == "POST":
        target_engine = request.form.get("target_engine", "postgres")
        selected_db = request.form.get("target_db")
        files = request.files.getlist("files")

        app.logger.info(f"[UPLOAD] POST received: engine={target_engine}, db={selected_db}, files={len(files)}")

        if not selected_db:
            flash("Please select a target database.", "warning")
            app.logger.warning(f"[UPLOAD] No database selected")
            return redirect(url_for("upload_csv"))

        if not files or len(files) == 0 or files[0].filename == "":
            flash("Please choose at least one file to upload.", "warning")
            app.logger.warning(f"[UPLOAD] No files uploaded")
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
            successful_uploads = 0
            failed_uploads = 0

            # Prepare target engine/clients
            pg_engine = None
            ch_client = None
            if target_engine == 'postgres':
                pg_engine = get_pg_engine(selected_db)
            elif target_engine == 'clickhouse':
                if CHClient is None:
                    flash('ClickHouse driver not installed on server. Please install clickhouse-driver.', 'danger')
                    return redirect(url_for('upload_csv'))
                # Read connection info from environment (required)
                ch_host = os.environ.get('CLICKHOUSE_HOST')
                ch_port = os.environ.get('CLICKHOUSE_PORT')
                ch_user = os.environ.get('CLICKHOUSE_USER')
                ch_password = os.environ.get('CLICKHOUSE_PASSWORD', '')
                if not ch_host or not ch_port or not ch_user:
                    flash('Please set CLICKHOUSE_HOST, CLICKHOUSE_PORT and CLICKHOUSE_USER in your environment (.env)', 'danger')
                    return redirect(url_for('upload_csv'))
                ch_client = CHClient(host=ch_host, port=int(ch_port), user=ch_user, password=ch_password)
            else:
                flash(f'Unknown target engine: {target_engine}', 'danger')
                return redirect(url_for('upload_csv'))

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

                    # Load into target engine
                    if target_engine == 'postgres':
                        # Load into public schema, replacing any existing table of same name
                        df.to_sql(table_name, pg_engine, schema="public", if_exists="replace", index=False)
                    else:
                        # ClickHouse: create table with all columns as Nullable(String) and insert rows
                        # Sanitize column names and convert all values to strings
                        ch_db = selected_db
                        ch_table = table_name
                        cols = list(df.columns)
                        # Build CREATE TABLE statement
                        col_defs = ", ".join([f"`{c}` Nullable(String)" for c in cols]) if cols else "`content` Nullable(String)"
                        create_sql = f"CREATE TABLE IF NOT EXISTS `{ch_db}`.`{ch_table}` ({col_defs}) ENGINE = MergeTree() ORDER BY tuple()"
                        ch_client.execute(create_sql)

                        # Prepare rows as tuples of strings
                        if cols:
                            rows_to_insert = [tuple((None if pd.isna(v) else str(v)) for v in row) for row in df.values.tolist()]
                            insert_sql = f"INSERT INTO `{ch_db}`.`{ch_table}` ({', '.join(['`'+c+'`' for c in cols])}) VALUES"
                            # clickhouse-driver accepts list of tuples with execute and INSERT query without VALUES part in some versions
                            ch_client.execute(insert_sql, rows_to_insert)
                        else:
                            # single content column
                            rows_to_insert = [(None if pd.isna(v) else str(v),) for v in df['content'].tolist()]
                            insert_sql = f"INSERT INTO `{ch_db}`.`{ch_table}` (`content`) VALUES"
                            ch_client.execute(insert_sql, rows_to_insert)

                    upload_progress[session_id]['results'].append({
                        'file': filename,
                        'status': 'success',
                        'message': (f"Successfully loaded to table 'public.{table_name}' ({len(df)} rows)" if target_engine == 'postgres' else f"Successfully loaded to ClickHouse '{selected_db}.{table_name}' ({len(df)} rows)" )
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
                                 clickhouse_dbs=clickhouse_dbs,
                                 role=session.get("role"),
                                 upload_results=upload_progress[session_id]['results'])

        except Exception as e:
            upload_progress[session_id]['status'] = 'error'
            upload_progress[session_id]['current_file'] = ''
            flash(f"Upload process failed: {e}", "danger")
            return redirect(url_for("upload_csv"))

    return render_template("upload.html", postgres_dbs=postgres_dbs, clickhouse_dbs=clickhouse_dbs, role=session.get("role"))

@app.route("/api/upload-progress/<session_id>")
@require_role(["admin", "operator"])
def get_upload_progress(session_id):
    """API endpoint to check upload progress"""
    if session_id in upload_progress:
        return jsonify(upload_progress[session_id])
    else:
        return jsonify({'status': 'not_found'}), 404


@app.route('/api/clickhouse-dbs')
@require_role(["admin", "operator"])
def api_clickhouse_dbs():
    """Return ClickHouse databases as JSON for the UI to populate selects."""
    try:
        dbs = load_ch_databases()
        return jsonify({'databases': dbs})
    except Exception as e:
        app.logger.exception(f"Error fetching ClickHouse DBs: {e}")
        return jsonify({'databases': [], 'error': str(e)}), 500

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
        
        # Auto-start polling for all API sources with polling_mode=True
        from flask_auto_start_polling import auto_start_polling_sources
        auto_start_polling_sources(app)
        
        app.run(debug=flask_debug, host=app_host, port=app_port, use_reloader=use_reloader, use_debugger=use_debugger)
    except Exception as e:
        app.logger.error(f"[ERROR] STARTUP ERROR: {e}")
        app.logger.error(f"Failed to start application: {e}")
        sys.exit(1)
