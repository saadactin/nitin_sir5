"""
Server management routes
Handles adding, editing, deleting servers, and viewing server databases
"""
from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from routes.utils import load_config, test_sql_connection, load_pg_databases, load_clickhouse_databases
from manage_server import save_config
from auth import require_role
from hybrid_sync import (
    get_sql_connection, 
    get_all_databases as hs_get_all_databases,
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

# Create blueprint
servers_bp = Blueprint('servers', __name__)


@servers_bp.route("/server/<server_name>")
@require_role(["admin", "operator", "viewer"])
def view_server_databases(server_name):
    """Show databases for a server and allow selecting subset to sync."""
    from flask import current_app as app
    
    try:
        config = load_config()
        server_conf = config["sqlservers"].get(server_name)
        if not server_conf:
            flash("Server not found", "danger")
            return redirect(url_for("main.index"))
        conn = get_sql_connection(server_conf)
        dbs = hs_get_all_databases(conn)
        conn.close()
        return render_template("server_databases.html", server_name=server_name, databases=dbs, role=session.get("role"))
    except Exception as e:
        flash(f"Failed to load databases: {e}", "danger")
        return redirect(url_for("main.index"))


@servers_bp.route("/add-server", methods=["GET", "POST"])
@require_role(["admin", "operator"])
def add_server():
    """Add a new SQL Server"""
    from flask import current_app as app
    
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
            return redirect(url_for("servers.add_server"))
            
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
        return redirect(url_for("main.index"))

    # GET request
    postgres_dbs = load_pg_databases()
    return render_template("add_sources.html", postgres_dbs=postgres_dbs)


@servers_bp.route("/test-connection", methods=["POST"])
@require_role(["admin", "operator"])
def test_connection():
    """Test SQL Server connection via AJAX"""
    from flask import current_app as app
    
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


@servers_bp.route("/test-hana-connection", methods=["POST"])
@require_role(["admin", "operator"])
def test_hana_connection():
    """Test SAP HANA connection via AJAX if hdbcli is available"""
    from flask import current_app as app
    
    try:
        data = request.get_json()
        host = (data.get("host") or "").strip()
        port = (data.get("port") or "").strip()
        if not port:
            # Try to load from .env
            try:
                from db_utils import load_hana_config
                hana_base = load_hana_config()
                port = str(hana_base['port'])
            except ValueError:
                port = None
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
            conn = hana_dbapi.connect(
                address=host,
                port=int(port),
                user=username,
                password=password,
                encrypt=True,
                sslValidateCertificate=False,
                timeout=10,
                communicationTimeout=30000,
                reconnect=True
            )
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


@servers_bp.route("/edit-server/<server_name>", methods=["GET", "POST"])
@require_role(["admin", "operator"])
def edit_server(server_name):
    """Edit an existing server"""
    from flask import current_app as app
    
    config = load_config()
    servers = config.get("sqlservers", {})

    if request.method == "POST":
        server = request.form["server"]
        username = request.form["username"]
        password = request.form["password"]
        pg_database = request.form.get("pg_database")

        if server_name not in servers:
            flash(f"Server {server_name} does not exist!", "error")
            return redirect(url_for("main.index"))
            
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
        return redirect(url_for("main.index"))

    # GET request → pre-fill form
    server_config = servers.get(server_name)
    if not server_config:
        flash(f"Server {server_name} not found!", "error")
        return redirect(url_for("main.index"))

    postgres_dbs = load_pg_databases()
    return render_template(
        "edit_sources.html",   # now points to your edit page
        postgres_dbs=postgres_dbs,
        server_name=server_name,
        server_config=server_config
    )


@servers_bp.route("/delete-server/<server_name>", methods=["POST"])
@require_role(["admin", "operator"])
def delete_server_route(server_name):
    """Delete a SQL Server from config"""
    from manage_server import delete_server  # import here to avoid circular import

    try:
        delete_server(server_name)
        flash(f"Server {server_name} deleted!", "success")
    except Exception as e:
        flash(f"Failed to delete server: {e}", "danger")
    return redirect(url_for("main.index"))


@servers_bp.route("/sync-selected/<server_name>", methods=["POST"])
@require_role(["admin", "operator"])
def sync_selected_databases(server_name):
    """Sync only selected databases for a server (incremental)."""
    from flask import current_app as app
    
    try:
        selected = [d.strip() for d in request.form.getlist("databases") if d.strip()]
        if not selected:
            flash("No databases selected.", "warning")
            return redirect(url_for("servers.view_server_databases", server_name=server_name))

        config = load_config()
        server_conf = config["sqlservers"].get(server_name)
        if not server_conf:
            flash("Server not found", "danger")
            return redirect(url_for("main.index"))

        # Validate requested databases actually exist on the server
        try:
            test_conn = get_sql_connection(server_conf)
            existing_dbs = set(hs_get_all_databases(test_conn))
            test_conn.close()
        except Exception as e:
            flash(f"Could not read databases from server: {e}", "danger")
            return redirect(url_for("servers.view_server_databases", server_name=server_name))

        selected = [d for d in selected if d in existing_dbs]
        if not selected:
            flash("No valid databases selected (not found on server).", "warning")
            return redirect(url_for("servers.view_server_databases", server_name=server_name))

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
        return redirect(url_for("servers.view_server_databases", server_name=server_name))
    except Exception as e:
        flash(f"Failed to sync selected: {e}", "danger")
        return redirect(url_for("servers.view_server_databases", server_name=server_name))


@servers_bp.route('/sync-summary/<server_name>/tables')
@require_role(["admin", "operator", "viewer"])
def server_tables(server_name):
    """Direct route for server tables view for backward compatibility"""
    config = load_config()
    sqlservers = config.get("sqlservers", {})
    
    if server_name not in sqlservers:
        flash(f"Server '{server_name}' not found", "error")
        return redirect(url_for('main.index'))
    
    target_db = sqlservers[server_name].get('target_postgres_db')
    if not target_db:
        flash(f"No target database configured for server '{server_name}'", "error")
        return redirect(url_for('main.index'))
    
    return render_template( 
        'server_tables.html',
        server_name=server_name,
        target_db=target_db
    )

