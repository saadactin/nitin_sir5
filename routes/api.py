"""
API endpoint routes
"""
from flask import Blueprint, jsonify
from auth import require_role
from routes.utils import load_config, test_sql_connection
from scheduler_utils import scheduled_jobs

# Create blueprint
api_bp = Blueprint('api', __name__)


@api_bp.route("/api/target/databases")
@require_role(["admin", "operator", "viewer"])
def get_target_databases():
    """API endpoint to get available target databases"""
    from routes.utils import load_pg_databases, load_clickhouse_databases
    
    try:
        postgres_dbs = load_pg_databases()
        clickhouse_dbs = load_clickhouse_databases()
        
        return jsonify({
            "postgres": postgres_dbs,
            "clickhouse": clickhouse_dbs
        })
    except Exception as e:
        from flask import current_app as app
        app.logger.exception(f"Error loading target databases: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.route('/api/diagnose-sql-server/<server_name>', methods=['GET'])
@require_role(["admin"])
def diagnose_sql_server(server_name):
    """Diagnostic endpoint to help troubleshoot SQL Server connection issues"""
    from flask import current_app as app
    
    try:
        from hybrid_sync import get_sql_server_instance_info
        from routes.utils import test_sql_connection
        
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


@api_bp.route('/api/job-statuses', methods=['GET'])
@require_role(["admin", "operator", "viewer"])
def api_job_statuses():
    """API endpoint to get current job statuses for the view schedules page"""
    from flask import current_app as app
    
    try:
        return jsonify(scheduled_jobs)
    except Exception as e:
        app.logger.error(f"Error fetching job statuses: {e}")
        return jsonify({"error": str(e)}), 500

