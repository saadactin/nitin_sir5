"""
Health Check Endpoint
Add this to app.py for production monitoring
"""

from flask import jsonify
from datetime import datetime
from db_utils import get_pg_connection

@app.route('/health')
def health_check():
    """Health check endpoint for monitoring"""
    health_status = {
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'version': '1.0.0'
    }
    
    # Check database connection
    try:
        conn = get_pg_connection()
        conn.close()
        health_status['database'] = 'connected'
    except Exception as e:
        health_status['status'] = 'unhealthy'
        health_status['database'] = f'error: {str(e)}'
        return jsonify(health_status), 503
    
    # Check ClickHouse (optional)
    try:
        from db_utils import load_clickhouse_config
        import clickhouse_connect
        config = load_clickhouse_config()
        if config and config.get('host'):
            client = clickhouse_connect.get_client(
                host=config.get('host'),
                port=int(config.get('port', 9000)),
                username=config.get('user', 'default'),
                password=config.get('password', '')
            )
            client.command('SELECT 1')
            health_status['clickhouse'] = 'connected'
    except Exception:
        health_status['clickhouse'] = 'not_configured'
    
    status_code = 200 if health_status['status'] == 'healthy' else 503
    return jsonify(health_status), status_code

