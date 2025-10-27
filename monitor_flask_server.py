"""
Flask Server Monitor
Monitors the Flask application and sends email alerts when the server goes down.
Run this script separately to monitor the Flask server process.
"""

import os
import sys
import time
import requests
import logging
from datetime import datetime
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
FLASK_URL = "http://127.0.0.1:5001"
CHECK_INTERVAL_SECONDS = 60  # Check every 60 seconds
ALERT_COOLDOWN_SECONDS = 300  # Don't send another alert for 5 minutes

# Track when last alert was sent
last_alert_time = None
server_was_up = True

def check_server_health():
    """Check if Flask server is responding"""
    try:
        response = requests.get(f"{FLASK_URL}/login", timeout=5)
        # Any response (even redirect) means server is up
        return response.status_code in [200, 302, 401, 403]
    except requests.exceptions.RequestException:
        return False

def send_alert_email():
    """Send email alert that Flask server is down"""
    global last_alert_time
    
    try:
        # Load environment and email service
        from dotenv import load_dotenv
        load_dotenv()
        
        from utils.email_service import email_service
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        error_msg = f"""Flask Application Server has stopped responding.

Timestamp: {timestamp}
Server URL: {FLASK_URL}
Monitor Process: monitor_flask_server.py

The Flask server is either:
- Crashed or stopped
- Not responding to HTTP requests
- Process was terminated

Action Required: Restart the Flask application server.

To restart:
1. Open PowerShell in project directory
2. Run: .\\myenv1\\Scripts\\python.exe app.py
"""
        
        result = email_service.notify_server_down(
            server_name="Flask Application Server",
            error_message=error_msg
        )
        
        if result.success:
            logger.info(f"✓ Alert email sent successfully to {len(result.recipients)} recipients")
            last_alert_time = time.time()
        else:
            logger.error(f"✗ Failed to send alert email: {result.error}")
            
    except Exception as e:
        logger.error(f"Exception while sending alert email: {e}")

def monitor_loop():
    """Main monitoring loop"""
    global server_was_up, last_alert_time
    
    logger.info(f"Starting Flask Server Monitor")
    logger.info(f"Monitoring URL: {FLASK_URL}")
    logger.info(f"Check interval: {CHECK_INTERVAL_SECONDS} seconds")
    logger.info(f"Alert cooldown: {ALERT_COOLDOWN_SECONDS} seconds")
    logger.info("-" * 50)
    
    while True:
        try:
            server_is_up = check_server_health()
            current_time = time.time()
            
            if server_is_up:
                if not server_was_up:
                    logger.info("✓ Flask server is back online")
                    server_was_up = True
                else:
                    logger.debug(f"✓ Server check OK at {datetime.now().strftime('%H:%M:%S')}")
            else:
                logger.warning("✗ Flask server is DOWN")
                
                # Send alert if server was previously up and cooldown period has passed
                should_alert = (
                    server_was_up or 
                    (last_alert_time is None) or 
                    (current_time - last_alert_time > ALERT_COOLDOWN_SECONDS)
                )
                
                if should_alert:
                    logger.error("!!! SENDING ALERT EMAIL - Flask server is down !!!")
                    send_alert_email()
                    server_was_up = False
                else:
                    remaining_cooldown = int(ALERT_COOLDOWN_SECONDS - (current_time - last_alert_time))
                    logger.info(f"Alert cooldown active. Next alert possible in {remaining_cooldown} seconds")
            
            time.sleep(CHECK_INTERVAL_SECONDS)
            
        except KeyboardInterrupt:
            logger.info("\nMonitor stopped by user")
            break
        except Exception as e:
            logger.error(f"Error in monitor loop: {e}")
            time.sleep(CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    try:
        # Initial check
        logger.info("Performing initial server check...")
        if check_server_health():
            logger.info("✓ Flask server is running")
        else:
            logger.warning("✗ Flask server is not responding. Monitoring will start...")
            server_was_up = False
        
        # Start monitoring
        monitor_loop()
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
