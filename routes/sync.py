"""
Sync operation routes
Handles sync operations for servers and sources
"""
from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
import json
import psycopg2
import threading
from datetime import datetime
from routes.utils import load_config, load_pg_databases, load_clickhouse_databases
from auth import require_role
from hybrid_sync import process_sql_server_hybrid
from sync_manager import sync_manager
from dashboard import log_sync, get_last_sync_for_server
from utils.email_service import email_service

# Create blueprint
sync_bp = Blueprint('sync', __name__)


@sync_bp.route("/sync/<server_name>")
@require_role(["admin", "operator"])
def sync_server(server_name):
    """Run sync for the selected server"""
    from flask import current_app as app
    
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
    return redirect(url_for("main.index"))


@sync_bp.route('/sync_background/<server_name>', methods=['GET'])
@require_role(["admin", "operator"])
def sync_background(server_name):
    """Start the sync in a background thread using the enhanced sync manager."""
    from flask import current_app as app
    
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


@sync_bp.route('/sync_api_source/<int:source_id>', methods=['GET', 'POST'])
@require_role(["admin", "operator"])
def sync_api_source(source_id):
    """Manually trigger sync for a REST API source"""
    from flask import current_app as app
    
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
                   target_database, connection_details,
                   oauth_refresh_token, oauth_client_id, oauth_client_secret,
                   oauth_access_token, oauth_token_expiry, oauth_api_domain
            FROM data_sources 
            WHERE id = %s AND is_active = true
        """, (source_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        if not row:
            app.logger.warning(f"[API-SYNC] API source {source_id} not found or inactive")
            flash(f"API source not found or inactive", "danger")
            return redirect(url_for("main.index"))
        
        source_name = row[0]
        source_type = row[1]
        api_url = row[2]
        target_type = row[3]
        target_database = row[4]
        connection_details = json.loads(row[5]) if row[5] else {}
        
        # Get OAuth credentials if available (for Zoho OAuth)
        zoho_refresh_token = row[6] if len(row) > 6 else None
        zoho_client_id = row[7] if len(row) > 7 else None
        zoho_client_secret = row[8] if len(row) > 8 else None
        oauth_access_token = row[9] if len(row) > 9 else None
        oauth_token_expiry = row[10] if len(row) > 10 else None
        oauth_api_domain = row[11] if len(row) > 11 else None
        
        # Only sync REST APIs to ClickHouse
        if source_type != 'rest_api':
            flash(f"Source '{source_name}' is not a REST API source", "danger")
            return redirect(url_for("main.index"))
        
        if target_type.lower() != 'clickhouse':
            flash(f"Source '{source_name}' target is not ClickHouse", "danger")
            return redirect(url_for("main.index"))
        
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
                        auto_create_table=True,
                        source_id=source_id if auth_type == 'zoho_oauth' else None,
                        zoho_refresh_token=zoho_refresh_token if auth_type == 'zoho_oauth' else None,
                        zoho_client_id=zoho_client_id if auth_type == 'zoho_oauth' else None,
                        zoho_client_secret=zoho_client_secret if auth_type == 'zoho_oauth' else None,
                        stored_access_token=oauth_access_token if auth_type == 'zoho_oauth' else None,
                        stored_token_expiry=oauth_token_expiry if auth_type == 'zoho_oauth' else None,
                        stored_api_domain=oauth_api_domain if auth_type == 'zoho_oauth' else None
                    )
                    app.logger.info(f"API sync result for '{source_name}': {result}")
            except Exception as e:
                app.logger.error(f"Error syncing API source '{source_name}': {e}")
        
        sync_thread = threading.Thread(target=background_sync, daemon=True)
        sync_thread.start()
        
        flash(f"Sync started for API source '{source_name}'", "success")
        return redirect(url_for("main.index"))
        
    except Exception as e:
        app.logger.exception(f"Error initiating API source sync: {e}")
        flash(f"Error starting sync: {str(e)}", "danger")
        return redirect(url_for("main.index"))


@sync_bp.route('/sync_status/<server_name>', methods=['GET'])
@require_role(["admin", "operator", "viewer"])
def sync_status(server_name):
    """Return the most recent sync status for a server as JSON."""
    from flask import current_app as app
    
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


@sync_bp.route('/sync_status/all', methods=['GET'])
@require_role(["admin", "operator", "viewer"])
def all_sync_status():
    """Return status of all active syncs"""
    from flask import current_app as app
    
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


@sync_bp.route('/sync_status/source/<int:source_id>', methods=['GET'])
@require_role(["admin", "operator", "viewer"])
def sync_status_source(source_id):
    """Return sync status for a source started via sync_source_background"""
    from flask import current_app as app
    
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


@sync_bp.route('/sync_stop/<server_name>', methods=['POST'])
@require_role(["admin", "operator"])
def stop_sync(server_name):
    """Stop a running sync for the specified server"""
    from flask import current_app as app
    
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

