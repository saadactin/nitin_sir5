"""
Production Readiness Improvements
These are the critical improvements needed for production deployment.
"""

# ============================================================================
# 1. HEALTH CHECK ENDPOINT
# ============================================================================

def add_health_check_endpoint(app):
    """Add health check endpoint for load balancers and monitoring"""
    from flask import jsonify
    from datetime import datetime
    from db_utils import get_pg_connection, return_pg_connection
    
    @app.route("/health")
    @app.route("/healthz")  # Kubernetes standard
    def health_check():
        """Health check endpoint for load balancers and orchestrators"""
        health_status = {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "version": "1.0.0",
            "checks": {}
        }
        overall_healthy = True
        
        # Check PostgreSQL connection
        try:
            conn = get_pg_connection()
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
            return_pg_connection(conn)
            health_status["checks"]["database"] = "healthy"
        except Exception as e:
            health_status["checks"]["database"] = f"unhealthy: {str(e)}"
            overall_healthy = False
        
        # Check connection pools
        try:
            from connection_pool import ConnectionPoolManager
            stats = ConnectionPoolManager.get_pool_stats()
            if stats.get('initialized'):
                health_status["checks"]["connection_pools"] = "healthy"
            else:
                health_status["checks"]["connection_pools"] = "not_initialized"
        except Exception as e:
            health_status["checks"]["connection_pools"] = f"error: {str(e)}"
        
        # Check ClickHouse (optional)
        try:
            from db_utils import load_clickhouse_config
            config = load_clickhouse_config()
            if config:
                health_status["checks"]["clickhouse"] = "configured"
        except Exception:
            health_status["checks"]["clickhouse"] = "not_configured"
        
        status_code = 200 if overall_healthy else 503
        if not overall_healthy:
            health_status["status"] = "unhealthy"
        
        return jsonify(health_status), status_code
    
    @app.route("/ready")
    def readiness_check():
        """Readiness check - more thorough than health check"""
        from db_utils import get_pg_connection, return_pg_connection
        
        try:
            # Check database is not just connected, but responsive
            conn = get_pg_connection()
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM information_schema.tables LIMIT 1")
            cur.fetchone()
            cur.close()
            return_pg_connection(conn)
            
            return jsonify({
                "status": "ready",
                "timestamp": datetime.now().isoformat()
            }), 200
        except Exception as e:
            return jsonify({
                "status": "not_ready",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }), 503


# ============================================================================
# 2. ENVIRONMENT VARIABLE VALIDATION
# ============================================================================

def validate_environment():
    """Validate all required environment variables are set"""
    import os
    from typing import List, Tuple
    
    required_vars = {
        "POSTGRES_HOST": "PostgreSQL host address",
        "POSTGRES_DB": "PostgreSQL database name",
        "POSTGRES_USER": "PostgreSQL username",
        "POSTGRES_PASSWORD": "PostgreSQL password",
    }
    
    optional_vars = {
        "CLICKHOUSE_HOST": "ClickHouse host (optional)",
        "SECRET_KEY": "Flask secret key (optional, has fallback)",
        "ADMIN_EMAILS": "Admin email addresses (optional)",
    }
    
    missing_required = []
    warnings = []
    
    # Check required variables
    for var, description in required_vars.items():
        value = os.environ.get(var)
        if not value:
            missing_required.append((var, description))
    
    # Check optional but recommended
    for var, description in optional_vars.items():
        value = os.environ.get(var)
        if not value:
            warnings.append((var, description))
    
    return missing_required, warnings


def validate_env_on_startup(app):
    """Validate environment on application startup"""
    missing, warnings = validate_environment()
    
    if missing:
        error_msg = "CRITICAL: Missing required environment variables:\n"
        for var, desc in missing:
            error_msg += f"  - {var}: {desc}\n"
        app.logger.error(error_msg)
        raise RuntimeError("Missing required environment variables. Check logs for details.")
    
    if warnings:
        warning_msg = "WARNING: Recommended environment variables not set:\n"
        for var, desc in warnings:
            warning_msg += f"  - {var}: {desc}\n"
        app.logger.warning(warning_msg)


# ============================================================================
# 3. PRODUCTION WSGI SERVER CONFIGURATION
# ============================================================================

# Gunicorn configuration (for Linux)
GUNICORN_CONFIG = """
# gunicorn_config.py
import multiprocessing

bind = "0.0.0.0:5002"
workers = multiprocessing.cpu_count() * 2 + 1
worker_class = "sync"
worker_connections = 1000
timeout = 120
keepalive = 5
max_requests = 1000
max_requests_jitter = 50
preload_app = True
accesslog = "-"
errorlog = "-"
loglevel = "info"
"""

# Waitress configuration (for Windows)
WAITRESS_CONFIG = """
# For Windows, use Waitress
# Install: pip install waitress
# Run: waitress-serve --host=0.0.0.0 --port=5002 app:app
"""


# ============================================================================
# 4. STRUCTURED LOGGING
# ============================================================================

def setup_structured_logging(app):
    """Set up structured JSON logging for production"""
    import json
    import logging
    from datetime import datetime
    
    class JSONFormatter(logging.Formatter):
        def format(self, record):
            log_data = {
                "timestamp": datetime.utcnow().isoformat(),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
                "module": record.module,
                "function": record.funcName,
                "line": record.lineno,
            }
            
            # Add exception info if present
            if record.exc_info:
                log_data["exception"] = self.formatException(record.exc_info)
            
            # Add extra fields
            if hasattr(record, "request_id"):
                log_data["request_id"] = record.request_id
            if hasattr(record, "user_id"):
                log_data["user_id"] = record.user_id
            
            return json.dumps(log_data)
    
    # Apply JSON formatter in production
    if os.environ.get("FLASK_ENV") == "production":
        for handler in app.logger.handlers:
            handler.setFormatter(JSONFormatter())


# ============================================================================
# 5. METRICS ENDPOINT (Prometheus format)
# ============================================================================

def add_metrics_endpoint(app):
    """Add Prometheus metrics endpoint"""
    from flask import Response
    from connection_pool import ConnectionPoolManager
    
    @app.route("/metrics")
    def metrics():
        """Prometheus metrics endpoint"""
        metrics_lines = []
        
        # Connection pool metrics
        try:
            pool_stats = ConnectionPoolManager.get_pool_stats()
            if pool_stats.get('initialized'):
                metrics_lines.append(f"connection_pool_initialized 1")
                metrics_lines.append(f"connection_pool_min_connections {pool_stats.get('min_connections', 0)}")
                metrics_lines.append(f"connection_pool_max_connections {pool_stats.get('max_connections', 0)}")
        except:
            pass
        
        # Application metrics (add your own)
        metrics_lines.append("# HELP app_requests_total Total number of requests")
        metrics_lines.append("# TYPE app_requests_total counter")
        metrics_lines.append("app_requests_total 0")
        
        return Response("\n".join(metrics_lines), mimetype="text/plain")


# ============================================================================
# 6. REQUEST ID TRACKING
# ============================================================================

def add_request_id_middleware(app):
    """Add request ID tracking for distributed tracing"""
    import uuid
    from flask import g, request
    
    @app.before_request
    def set_request_id():
        """Set request ID for tracing"""
        g.request_id = request.headers.get('X-Request-ID') or str(uuid.uuid4())
        
        # Add to logger context
        import logging
        logger = logging.getLogger()
        old_factory = logging.getLogRecordFactory()
        
        def record_factory(*args, **kwargs):
            record = old_factory(*args, **kwargs)
            record.request_id = g.request_id
            return record
        
        logging.setLogRecordFactory(record_factory)
    
    @app.after_request
    def add_request_id_header(response):
        """Add request ID to response headers"""
        if hasattr(g, 'request_id'):
            response.headers['X-Request-ID'] = g.request_id
        return response


# ============================================================================
# USAGE INSTRUCTIONS
# ============================================================================

"""
To use these improvements in app.py:

1. Add health check:
   from production_improvements import add_health_check_endpoint
   add_health_check_endpoint(app)

2. Validate environment on startup:
   from production_improvements import validate_env_on_startup
   validate_env_on_startup(app)

3. Add metrics endpoint:
   from production_improvements import add_metrics_endpoint
   add_metrics_endpoint(app)

4. Add request ID tracking:
   from production_improvements import add_request_id_middleware
   add_request_id_middleware(app)

5. Set up structured logging:
   from production_improvements import setup_structured_logging
   setup_structured_logging(app)
"""

