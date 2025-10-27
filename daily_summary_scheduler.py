"""
Daily Email Summary Scheduler
Automatically sends daily sync summary emails
"""
import logging
import schedule
import time
from datetime import datetime, timedelta
from threading import Thread
from pathlib import Path
import sys

# Add utils to path
sys.path.insert(0, str(Path(__file__).parent / "utils"))
from email_service import email_service

logger = logging.getLogger(__name__)

class DailySummaryScheduler:
    """Schedules and sends daily sync summary emails"""
    
    def __init__(self, flask_app=None):
        self.flask_app = flask_app
        self.is_running = False
        self.scheduler_thread = None
        
    def send_daily_summary(self):
        """Collect and send daily sync summary"""
        try:
            logger.info("Generating daily sync summary...")
            
            # Get today's date
            today = datetime.now().strftime("%Y-%m-%d")
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            
            # Import dashboard functions
            if self.flask_app:
                with self.flask_app.app_context():
                    from dashboard import get_all_sync_history
                    
                    # Get sync history for today
                    history = get_all_sync_history()
                    
                    # Filter today's syncs
                    today_syncs = [s for s in history if s.get('timestamp', '').startswith(today) or s.get('timestamp', '').startswith(yesterday)]
                    
                    # Calculate statistics
                    total_syncs = len(today_syncs)
                    successful_syncs = len([s for s in today_syncs if s.get('status') == 'success'])
                    failed_syncs = len([s for s in today_syncs if s.get('status') in ['failed', 'error']])
                    
                    # Group by server
                    server_stats = {}
                    for sync in today_syncs:
                        server = sync.get('server_name', 'Unknown')
                        if server not in server_stats:
                            server_stats[server] = {'syncs': 0, 'success': 0, 'failed': 0}
                        server_stats[server]['syncs'] += 1
                        if sync.get('status') == 'success':
                            server_stats[server]['success'] += 1
                        else:
                            server_stats[server]['failed'] += 1
                    
                    # Prepare server summary
                    servers_summary = []
                    for server, stats in server_stats.items():
                        servers_summary.append({
                            'server': server,
                            'syncs': stats['syncs'],
                            'status': 'success' if stats['failed'] == 0 else 'failed'
                        })
                    
                    # Get top errors
                    error_syncs = [s for s in today_syncs if s.get('status') in ['failed', 'error']]
                    top_errors = [f"{s.get('server_name', 'Unknown')}: {s.get('details', 'Unknown error')[:100]}" 
                                  for s in error_syncs[:5]]
                    
                    # Estimate total rows (this would need actual tracking in production)
                    total_rows = 0  # You can enhance this by tracking actual row counts
                    
                    # Send email if there were any syncs
                    if total_syncs > 0:
                        result = email_service.notify_daily_sync_summary(
                            date=today,
                            total_syncs=total_syncs,
                            successful_syncs=successful_syncs,
                            failed_syncs=failed_syncs,
                            total_rows_synced=total_rows,
                            servers_summary=servers_summary,
                            top_errors=top_errors
                        )
                        
                        if result.success:
                            logger.info(f"Daily summary email sent successfully for {today}")
                        else:
                            logger.error(f"Failed to send daily summary email: {result.error}")
                    else:
                        logger.info(f"No syncs found for {today}, skipping daily summary email")
                        
        except Exception as e:
            logger.exception(f"Error generating daily summary: {e}")
    
    def start(self):
        """Start the daily summary scheduler"""
        if self.is_running:
            logger.warning("Daily summary scheduler is already running")
            return
        
        self.is_running = True
        
        # Schedule daily summary at 11:59 PM
        schedule.every().day.at("23:59").do(self.send_daily_summary)
        
        logger.info("Daily summary scheduler started (will send at 23:59 daily)")
        
        # Run scheduler in background thread
        def run_scheduler():
            while self.is_running:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        
        self.scheduler_thread = Thread(target=run_scheduler, daemon=True, name="daily-summary-scheduler")
        self.scheduler_thread.start()
    
    def stop(self):
        """Stop the daily summary scheduler"""
        self.is_running = False
        schedule.clear()
        logger.info("Daily summary scheduler stopped")
    
    def send_now(self):
        """Send daily summary immediately (for testing)"""
        logger.info("Sending daily summary now...")
        self.send_daily_summary()

# Global instance
daily_summary_scheduler = None

def init_daily_summary(flask_app):
    """Initialize daily summary scheduler with Flask app"""
    global daily_summary_scheduler
    if daily_summary_scheduler is None:
        daily_summary_scheduler = DailySummaryScheduler(flask_app)
        daily_summary_scheduler.start()
    return daily_summary_scheduler
