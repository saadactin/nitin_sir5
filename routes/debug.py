"""
Debug routes
"""
from flask import Blueprint, redirect, url_for, flash, jsonify, request
from datetime import datetime
import psycopg2
import os
from auth import require_role

# Create blueprint
debug_bp = Blueprint('debug', __name__)


@debug_bp.route('/debug/data_sources', methods=['GET'])
@require_role(["admin"])
def debug_list_data_sources():
    """Debug endpoint: return JSON list of configured data_sources (admin-only)."""
    from flask import current_app as app
    
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


@debug_bp.route('/debug/insert-sample', methods=['GET'])
@require_role(["admin"])
def debug_insert_sample_source():
    """Debug helper: insert a sample data_source record and redirect to index."""
    from flask import current_app as app
    
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
        # Debug endpoint - use environment variables instead of hardcoded values
        debug_server = os.environ.get('DEBUG_SERVER', '127.0.0.1')
        debug_username = os.environ.get('DEBUG_USERNAME', 'sa')
        debug_password = os.environ.get('DEBUG_PASSWORD', '')
        debug_target_db = os.environ.get('DEBUG_TARGET_DB', 'postgres')
        
        if not debug_password:
            flash("DEBUG_PASSWORD environment variable not set. Cannot create debug source.", "warning")
            return redirect(url_for('main.index'))
        
        cur.execute("INSERT INTO data_sources (source_name, source_type, server_address, username, password, target_type, target_database) VALUES (%s,%s,%s,%s,%s,%s,%s)", (name, 'sql_server', debug_server, debug_username, debug_password, 'postgresql', debug_target_db))
        conn.commit()
        cur.close(); conn.close()
        flash(f"Inserted debug source '{name}'", 'success')
        app.logger.info(f"[DEBUG] Inserted sample data_source: {name}")
        return redirect(url_for('main.index'))
    except Exception as e:
        app.logger.exception(f"Debug insert failed: {e}")
        flash(f"Debug insert failed: {e}", 'danger')
        return redirect(url_for('main.index'))

