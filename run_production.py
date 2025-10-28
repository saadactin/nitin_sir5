"""
Production WSGI Server Runner for ACTIN Data Sync Application

This script runs the Flask application using Waitress, a production-ready
pure-Python WSGI server that works excellently on Windows.

Usage:
    python run_production.py

Environment Variables:
    APP_HOST - Host to bind to (default: 0.0.0.0 for all interfaces)
    APP_PORT - Port to listen on (default: 5001)
    WAITRESS_THREADS - Number of threads (default: 4)
    WAITRESS_CHANNEL_TIMEOUT - Channel timeout in seconds (default: 60)
    FLASK_DEBUG - Set to 0 for production (default: 0)
"""

import os
import sys
import logging
from datetime import datetime

# Configure logging before importing app
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('production.log')
    ]
)

logger = logging.getLogger(__name__)

def validate_environment():
    """Validate required environment variables and configuration"""
    issues = []
    
    # Check for SECRET_KEY
    if not os.environ.get('SECRET_KEY'):
        issues.append("⚠️  SECRET_KEY not set - using default (CHANGE IN PRODUCTION!)")
    
    # Ensure production mode
    if os.environ.get('FLASK_DEBUG', '0') not in ('0', 'false', 'False'):
        issues.append("⚠️  FLASK_DEBUG should be 0 in production")
    
    # Check database configuration
    required_vars = ['PG_HOST', 'PG_PORT', 'PG_USER', 'PG_PASSWORD', 'PG_DATABASE']
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    
    if missing_vars:
        issues.append(f"⚠️  Missing database config: {', '.join(missing_vars)}")
    
    return issues

def run_production_server():
    """Run the application with Waitress production server"""
    
    print("="*70)
    print("🚀 ACTIN DATA SYNC - PRODUCTION SERVER")
    print("="*70)
    print(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🐍 Python: {sys.version.split()[0]}")
    print(f"🔧 WSGI Server: Waitress (Production-Ready)")
    print("="*70)
    
    # Validate environment
    issues = validate_environment()
    if issues:
        print("\n⚠️  CONFIGURATION WARNINGS:")
        for issue in issues:
            print(f"   {issue}")
        print()
    
    # Get configuration from environment
    host = os.environ.get('APP_HOST', '0.0.0.0')  # 0.0.0.0 allows external connections
    port = int(os.environ.get('APP_PORT', '5001'))
    threads = int(os.environ.get('WAITRESS_THREADS', '4'))
    channel_timeout = int(os.environ.get('WAITRESS_CHANNEL_TIMEOUT', '60'))
    
    # Ensure production mode
    os.environ['FLASK_DEBUG'] = '0'
    
    print(f"🌐 Host: {host}")
    print(f"🔌 Port: {port}")
    print(f"🧵 Threads: {threads}")
    print(f"⏱️  Channel Timeout: {channel_timeout}s")
    print("="*70)
    
    try:
        # Import app AFTER setting environment variables
        logger.info("Loading Flask application...")
        from app import app
        
        logger.info("Initializing default admin user...")
        from auth import init_admin_user
        default_pw = os.environ.get('DEFAULT_ADMIN_PASSWORD', 'admin123')
        init_admin_user(create_if_missing=True, default_password=default_pw)
        logger.info("✅ Default admin user ready")
        
        # Initialize daily email summary scheduler
        try:
            from daily_summary_scheduler import init_daily_summary
            daily_scheduler = init_daily_summary(app)
            logger.info("✅ Daily summary scheduler initialized")
        except Exception as e:
            logger.warning(f"⚠️  Daily summary scheduler not initialized: {e}")
        
        print("\n✅ APPLICATION READY")
        print(f"🌍 Access the application at: http://{host if host != '0.0.0.0' else 'localhost'}:{port}")
        print(f"📊 Dashboard: http://localhost:{port}/")
        print(f"🔐 Login: http://localhost:{port}/login")
        print("\n💡 Press Ctrl+C to stop the server")
        print("="*70)
        print()
        
        # Import and run Waitress
        from waitress import serve
        
        # Serve the application
        serve(
            app,
            host=host,
            port=port,
            threads=threads,
            channel_timeout=channel_timeout,
            # connection_limit defaults to 100, which is good for most cases
            # url_scheme='http',  # Explicitly set to HTTP (no HTTPS)
            # asyncore_use_poll=True,  # Better performance on Windows
            _quiet=False  # Show request logs
        )
        
    except ImportError as e:
        logger.error(f"❌ Missing dependency: {e}")
        logger.error("💡 Run: pip install waitress")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n")
        print("="*70)
        print("🛑 SHUTTING DOWN SERVER")
        print("="*70)
        
        # Send shutdown email if configured
        try:
            from app import send_shutdown_email
            send_shutdown_email("Production server stopped by administrator")
            logger.info("📧 Shutdown notification sent")
        except Exception as e:
            logger.warning(f"Could not send shutdown email: {e}")
        
        print("✅ Server stopped gracefully")
        print("="*70)
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ FATAL ERROR: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    run_production_server()
