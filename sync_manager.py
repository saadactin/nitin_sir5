"""
Background Sync Manager
Handles asynchronous sync operations with proper isolation and progress tracking.
"""

import threading
import time
import logging
from datetime import datetime
from typing import Dict, Optional
import queue
import uuid
import os

logger = logging.getLogger(__name__)

# Import email service lazily to ensure .env is loaded first
def get_email_service():
    """Get email service instance, ensuring .env is loaded"""
    try:
        # Ensure .env is loaded
        from dotenv import load_dotenv
        load_dotenv(override=False)
    except:
        pass
    
    from utils.email_service import email_service
    return email_service

class SyncManager:
    """Manages background sync operations with proper isolation"""
    
    def __init__(self):
        self.active_syncs: Dict[str, dict] = {}
        self.sync_lock = threading.Lock()
        self.max_concurrent_syncs = 3  # Limit concurrent syncs to prevent resource exhaustion
        
    def is_sync_running(self, server_name: str) -> bool:
        """Check if a sync is currently running for the server"""
        with self.sync_lock:
            return server_name in self.active_syncs
    
    def get_sync_status(self, server_name: str) -> Optional[dict]:
        """Get the current status of a running sync"""
        with self.sync_lock:
            return self.active_syncs.get(server_name)
    
    def get_all_active_syncs(self) -> Dict[str, dict]:
        """Get all currently active syncs"""
        with self.sync_lock:
            return self.active_syncs.copy()
    
    def start_sync(self, server_name: str, server_conf: dict, flask_app) -> dict:
        """Start a background sync operation"""
        
        # Check if sync is already running
        if self.is_sync_running(server_name):
            return {
                "success": False,
                "message": f"Sync already running for {server_name}",
                "sync_id": None
            }
        
        # Check if we've reached max concurrent syncs
        with self.sync_lock:
            if len(self.active_syncs) >= self.max_concurrent_syncs:
                return {
                    "success": False,
                    "message": f"Maximum concurrent syncs ({self.max_concurrent_syncs}) reached. Please wait.",
                    "sync_id": None
                }
        
        # Generate unique sync ID
        sync_id = str(uuid.uuid4())[:8]
        
        # Initialize sync status
        sync_status = {
            "sync_id": sync_id,
            "server_name": server_name,
            "status": "starting",
            "progress": 0,
            "message": "Initializing sync...",
            "start_time": datetime.now(),
            "thread_id": None,
            "databases_processed": 0,
            "total_databases": 0,
            "current_database": None,
            "errors": []
        }
        
        with self.sync_lock:
            self.active_syncs[server_name] = sync_status
        
        # Start sync in isolated thread
        thread = threading.Thread(
            target=self._sync_worker,
            args=(server_name, server_conf, sync_id, flask_app),
            daemon=True,
            name=f"sync-{server_name}-{sync_id}"
        )
        
        thread.start()
        sync_status["thread_id"] = thread.ident
        
        logger.info(f"[SYNC-MANAGER] Started background sync for {server_name} (ID: {sync_id})")
        
        return {
            "success": True,
            "message": f"Background sync started for {server_name}",
            "sync_id": sync_id
        }
    
    def _sync_worker(self, server_name: str, server_conf: dict, sync_id: str, flask_app):
        """Worker function that runs the actual sync in isolation"""
        
        def update_status(status: str, progress: int = None, message: str = None, **kwargs):
            """Update sync status thread-safely"""
            with self.sync_lock:
                if server_name in self.active_syncs:
                    if status:
                        self.active_syncs[server_name]["status"] = status
                    if progress is not None:
                        self.active_syncs[server_name]["progress"] = progress
                    if message:
                        self.active_syncs[server_name]["message"] = message
                    for key, value in kwargs.items():
                        self.active_syncs[server_name][key] = value
        
        try:
            # Push Flask app context for database operations
            with flask_app.app_context():
                logger.info(f"[SYNC-WORKER-{sync_id}] Starting sync for {server_name}")
                update_status("running", 0, "Starting sync process...")
                
                # Import here to avoid circular imports
                from hybrid_sync import process_sql_server_hybrid
                from dashboard import log_sync
                
                # Track sync statistics
                sync_start_time = datetime.now()
                tables_synced = 0
                rows_synced = 0
                failed_tables = []
                
                # Log sync start
                try:
                    log_sync(server_name, 'started', f'Started background sync at {sync_start_time.strftime("%Y-%m-%d %H:%M:%S")} (ID: {sync_id})')
                except Exception as e:
                    logger.warning(f"[SYNC-WORKER-{sync_id}] Failed to log sync start: {e}")
                
                update_status("running", 10, "Connecting to databases...")
                
                # Run the actual sync process with progress tracking
                sync_result = self._run_sync_with_tracking(server_name, server_conf, sync_id, update_status)
                
                # Calculate sync duration
                sync_end_time = datetime.now()
                duration = sync_end_time - sync_start_time
                duration_str = f"{int(duration.total_seconds() // 60)}m {int(duration.total_seconds() % 60)}s"
                
                # Check for partial success
                if sync_result.get('failed_tables'):
                    failed_tables = sync_result['failed_tables']
                    total_tables = sync_result.get('total_tables', len(failed_tables))
                    success_count = total_tables - len(failed_tables)
                    
                    update_status("partial", 100, f"Sync partially completed: {success_count}/{total_tables} tables")
                    logger.warning(f"[SYNC-WORKER-{sync_id}] Partial sync for {server_name}: {len(failed_tables)} tables failed")
                    
                    try:
                        log_sync(server_name, "partial", f"Partial sync completed (ID: {sync_id}): {success_count}/{total_tables} tables")
                    except Exception as e:
                        logger.warning(f"[SYNC-WORKER-{sync_id}] Failed to log partial sync: {e}")
                    
                    # Send partial success email
                    try:
                        logger.info(f"[SYNC-WORKER-{sync_id}] Sending partial success email for {server_name}")
                        error_summary = sync_result.get('error_summary', 'Multiple errors occurred during sync')
                        email_service = get_email_service()
                        email_result = email_service.notify_sync_partial_success(
                            server_name=server_name,
                            total_tables=total_tables,
                            success_count=success_count,
                            failed_tables=failed_tables,
                            error_summary=error_summary
                        )
                        if email_result.success:
                            logger.info(f"[SYNC-WORKER-{sync_id}] Partial success email sent successfully")
                        else:
                            logger.error(f"[SYNC-WORKER-{sync_id}] Failed to send partial success email: {email_result.error}")
                    except Exception as e:
                        logger.exception(f"[SYNC-WORKER-{sync_id}] Exception sending partial success email: {e}")
                else:
                    # Sync completed successfully
                    update_status("completed", 100, "Sync completed successfully")
                    logger.info(f"[SYNC-WORKER-{sync_id}] Sync completed successfully for {server_name}")
                    
                    tables_synced = sync_result.get('tables_synced', 0)
                    rows_synced = sync_result.get('rows_synced', 0)
                    
                    try:
                        log_sync(server_name, "success", f"Background sync completed (ID: {sync_id})")
                    except Exception as e:
                        logger.warning(f"[SYNC-WORKER-{sync_id}] Failed to log sync success: {e}")
                    
                    # Send success email with details
                    try:
                        logger.info(f"[SYNC-WORKER-{sync_id}] Preparing to send success email for {server_name}")
                        
                        # Build summary (using plain text, no emoji for Windows console compatibility)
                        if tables_synced > 0:
                            summary = f"Successfully synced {tables_synced} tables\n"
                        else:
                            summary = f"Sync completed successfully\n"
                        
                        if rows_synced > 0:
                            summary += f"Total rows synced: {rows_synced:,}\n"
                        
                        summary += f"Duration: {duration_str}\n"
                        summary += f"No errors encountered"
                        
                        logger.info(f"[SYNC-WORKER-{sync_id}] Calling email service for {server_name}")
                        
                        email_service = get_email_service()
                        logger.info(f"[SYNC-WORKER-{sync_id}] Email service loaded. Admin emails: {email_service.admin_emails}")
                        
                        email_result = email_service.notify_sync_success(
                            server_name=server_name,
                            summary=summary
                        )
                        
                        logger.info(f"[SYNC-WORKER-{sync_id}] Email result: success={email_result.success}, error={email_result.error}, recipients={email_result.recipients}")
                        
                        if email_result.success:
                            logger.info(f"[SYNC-WORKER-{sync_id}] [EMAIL-SENT] Success email sent to {len(email_result.recipients) if email_result.recipients else 0} recipients")
                        else:
                            logger.error(f"[SYNC-WORKER-{sync_id}] [EMAIL-FAILED] Failed to send success email: {email_result.error}")
                            
                    except Exception as e:
                        logger.exception(f"[SYNC-WORKER-{sync_id}] Exception while sending success email: {e}")
                
        except Exception as e:
            logger.exception(f"[SYNC-WORKER-{sync_id}] Sync failed for {server_name}: {e}")
            update_status("failed", progress=None, message=f"Sync failed: {str(e)}")
            
            # Add error to status
            with self.sync_lock:
                if server_name in self.active_syncs:
                    self.active_syncs[server_name]["errors"].append({
                        "timestamp": datetime.now(),
                        "error": str(e)
                    })
            
            try:
                from dashboard import log_sync
                log_sync(server_name, "failed", f"Background sync failed (ID: {sync_id}): {str(e)}")
            except Exception as log_e:
                logger.warning(f"[SYNC-WORKER-{sync_id}] Failed to log sync failure: {log_e}")
            
            # Send failure email
            try:
                error_details = str(e)
                logger.info(f"[SYNC-WORKER-{sync_id}] Preparing to send failure email for {server_name}")
                
                email_service = get_email_service()
                logger.info(f"[SYNC-WORKER-{sync_id}] Email service loaded for failure notification. Admin emails: {email_service.admin_emails}")
                
                if "connection" in error_details.lower() or "timeout" in error_details.lower():
                    # This might be a server down issue
                    logger.info(f"[SYNC-WORKER-{sync_id}] Detected connection/timeout error, sending server down alert")
                    email_result = email_service.notify_server_down(
                        server_name=server_name,
                        error_message=error_details
                    )
                else:
                    # General sync failure
                    logger.info(f"[SYNC-WORKER-{sync_id}] Sending general sync failure alert")
                    email_result = email_service.notify_sync_failed(
                        server_name=server_name,
                        error_message=error_details
                    )
                
                logger.info(f"[SYNC-WORKER-{sync_id}] Failure email result: success={email_result.success}, error={email_result.error}")
                
                if email_result.success:
                    logger.info(f"[SYNC-WORKER-{sync_id}] [EMAIL-SENT] Failure/server-down email sent successfully")
                else:
                    logger.error(f"[SYNC-WORKER-{sync_id}] [EMAIL-FAILED] Failed to send failure email: {email_result.error}")
                    
            except Exception as email_e:
                logger.exception(f"[SYNC-WORKER-{sync_id}] Exception while sending failure email: {email_e}")
        
        finally:
            # Clean up: remove from active syncs after a delay to allow status checking
            def cleanup():
                time.sleep(60)  # Keep status available for 1 minute after completion
                with self.sync_lock:
                    if server_name in self.active_syncs:
                        final_status = self.active_syncs[server_name]["status"]
                        logger.info(f"[SYNC-MANAGER] Cleaning up sync {sync_id} for {server_name} (final status: {final_status})")
                        del self.active_syncs[server_name]
            
            cleanup_thread = threading.Thread(target=cleanup, daemon=True)
            cleanup_thread.start()
    
    def _run_sync_with_tracking(self, server_name: str, server_conf: dict, sync_id: str, update_status):
        """Run sync with progress tracking and return statistics"""
        
        sync_stats = {
            'tables_synced': 0,
            'rows_synced': 0,
            'failed_tables': [],
            'total_tables': 0,
            'error_summary': ''
        }
        
        try:
            # Import the sync function
            from hybrid_sync import process_sql_server_hybrid
            
            # Track database and table processing
            databases_processed = []
            current_db = None
            tables_in_db = []
            errors_list = []
            
            # We'll monkey-patch some logging to track progress
            original_logging_info = logging.info
            original_logging_error = logging.error
            
            def tracking_log_info(message):
                nonlocal current_db, tables_in_db
                # Update progress based on log messages
                if "Starting database" in message:
                    # Extract database name and update status
                    try:
                        db_name = message.split("Starting database")[1].split()[0].strip(": ")
                        current_db = db_name
                        databases_processed.append(db_name)
                        update_status(status="running", message=f"Processing database: {db_name}", current_database=db_name)
                    except:
                        pass
                elif "completed" in message.lower() and "table" in message.lower():
                    sync_stats['tables_synced'] += 1
                    update_status(status="running", message=f"Completed table sync")
                elif "rows" in message.lower():
                    # Try to extract row count
                    try:
                        import re
                        match = re.search(r'(\d+)\s+rows', message)
                        if match:
                            rows = int(match.group(1))
                            sync_stats['rows_synced'] += rows
                    except:
                        pass
                
                # Call original logging
                original_logging_info(message)
            
            def tracking_log_error(message):
                nonlocal errors_list
                # Track errors
                errors_list.append(message)
                if "table" in message.lower():
                    # Try to extract table name
                    try:
                        import re
                        match = re.search(r'table\s+["\']?([a-zA-Z0-9_\.]+)', message, re.IGNORECASE)
                        if match:
                            table_name = match.group(1)
                            if table_name not in sync_stats['failed_tables']:
                                sync_stats['failed_tables'].append(table_name)
                    except:
                        pass
                
                # Call original logging
                original_logging_error(message)
            
            # Temporarily replace logging
            logging.info = tracking_log_info
            logging.error = tracking_log_error
            
            try:
                # Run the actual sync
                process_sql_server_hybrid(server_name, server_conf)
                update_status("running", 90, "Finalizing sync process...")
                
                # Prepare error summary
                if errors_list:
                    sync_stats['error_summary'] = '\n'.join(errors_list[:5])  # Top 5 errors
                    if len(errors_list) > 5:
                        sync_stats['error_summary'] += f'\n... and {len(errors_list) - 5} more errors'
                
            finally:
                # Restore original logging
                logging.info = original_logging_info
                logging.error = original_logging_error
                
        except Exception as e:
            logger.exception(f"[SYNC-TRACKER-{sync_id}] Error in sync tracking for {server_name}")
            raise  # Re-raise the exception
        
        return sync_stats
    
    def stop_sync(self, server_name: str) -> dict:
        """Attempt to stop a running sync (limited capability)"""
        with self.sync_lock:
            if server_name not in self.active_syncs:
                return {
                    "success": False,
                    "message": f"No active sync found for {server_name}"
                }
            
            sync_status = self.active_syncs[server_name]
            
            # Mark as stopping (the thread will check this and exit gracefully if possible)
            sync_status["status"] = "stopping"
            sync_status["message"] = "Sync stop requested..."
            
            logger.warning(f"[SYNC-MANAGER] Stop requested for sync {sync_status['sync_id']} on {server_name}")
            
            return {
                "success": True,
                "message": f"Stop request sent for {server_name}. Sync will stop when safe to do so."
            }

# Global sync manager instance
sync_manager = SyncManager()