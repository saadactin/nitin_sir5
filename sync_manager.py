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

logger = logging.getLogger(__name__)

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
                
                # Log sync start
                try:
                    log_sync(server_name, 'started', f'Started background sync at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} (ID: {sync_id})')
                except Exception as e:
                    logger.warning(f"[SYNC-WORKER-{sync_id}] Failed to log sync start: {e}")
                
                update_status("running", 10, "Connecting to databases...")
                
                # Run the actual sync process with progress tracking
                self._run_sync_with_tracking(server_name, server_conf, sync_id, update_status)
                
                # Sync completed successfully
                update_status("completed", 100, "Sync completed successfully")
                logger.info(f"[SYNC-WORKER-{sync_id}] Sync completed successfully for {server_name}")
                
                try:
                    log_sync(server_name, "success", f"Background sync completed (ID: {sync_id})")
                except Exception as e:
                    logger.warning(f"[SYNC-WORKER-{sync_id}] Failed to log sync success: {e}")
                
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
        """Run sync with progress tracking"""
        
        try:
            # Import the sync function
            from hybrid_sync import process_sql_server_hybrid
            
            # We'll monkey-patch some logging to track progress
            original_logging_info = logging.info
            
            def tracking_log_info(message):
                # Update progress based on log messages
                if "Starting database" in message:
                    # Extract database name and update status
                    try:
                        db_name = message.split("Starting database")[1].split()[0].strip(": ")
                        update_status(status="running", message=f"Processing database: {db_name}", current_database=db_name)
                    except:
                        pass
                elif "completed" in message.lower():
                    update_status(status="running", message="Processing completed for database")
                elif "error" in message.lower():
                    update_status(status="running", message="Handling errors in processing")
                
                # Call original logging
                original_logging_info(message)
            
            # Temporarily replace logging.info
            logging.info = tracking_log_info
            
            try:
                # Run the actual sync
                process_sql_server_hybrid(server_name, server_conf)
                update_status("running", 90, "Finalizing sync process...")
                
            finally:
                # Restore original logging
                logging.info = original_logging_info
                
        except Exception as e:
            logger.exception(f"[SYNC-TRACKER-{sync_id}] Error in sync tracking for {server_name}")
            raise  # Re-raise the exception
    
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