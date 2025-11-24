"""
Unified Sync Logger - Handles logging for all sync operations
Logs HANA, API, SQL Server syncs with proper formatting and retention
Also writes to sync_history table for dashboard display
"""
import logging
import os
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
import json

# Log file path
LOG_DIR = os.path.dirname(os.path.abspath(__file__))
SYNC_LOG_FILE = os.path.join(LOG_DIR, 'sync_operations.log')

# Configure unified sync logger
sync_logger = logging.getLogger('sync_operations')
sync_logger.setLevel(logging.INFO)

# Remove existing handlers to avoid duplicates
sync_logger.handlers.clear()

# Create rotating file handler (max 10MB per file, keep 5 backups)
file_handler = RotatingFileHandler(
    SYNC_LOG_FILE,
    maxBytes=10 * 1024 * 1024,  # 10MB
    backupCount=5,
    encoding='utf-8'
)

# Set format: TIMESTAMP - LEVEL - SOURCE_TYPE - SOURCE_NAME - MESSAGE
formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

file_handler.setFormatter(formatter)
sync_logger.addHandler(file_handler)

# Also add console handler for development
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
console_handler.setLevel(logging.WARNING)  # Only show warnings/errors in console
sync_logger.addHandler(console_handler)


class SyncLogger:
    """Wrapper class for structured sync logging"""
    
    @staticmethod
    def _write_to_sync_history(source_name, status, details):
        """Write sync entry to sync_history table for dashboard"""
        try:
            from db_utils import get_pg_connection
            conn = get_pg_connection()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO metrics_sync_tables.sync_history (server_name, sync_time, status, details)
                VALUES (%s, NOW(), %s, %s)
            """, (source_name, status, details or "-"))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            sync_logger.warning(f"Failed to write to sync_history: {e}")
    
    @staticmethod
    def log_sync_start(source_type, source_name, source_id=None, details=None):
        """Log sync operation start"""
        details_str = f" | Details: {json.dumps(details)}" if details else ""
        id_str = f" | Source ID: {source_id}" if source_id else ""
        sync_logger.info(
            f"SYNC_START | Source Type: {source_type} | Source Name: {source_name}{id_str}{details_str}"
        )
        # Write to sync_history table
        history_details = f"{source_type} sync started"
        if details:
            if isinstance(details, dict):
                if 'tables_count' in details:
                    history_details += f" ({details['tables_count']} tables)"
                if 'target_db' in details:
                    history_details += f" → {details['target_db']}"
            else:
                history_details += f": {str(details)}"
        SyncLogger._write_to_sync_history(source_name, "started", history_details)
    
    @staticmethod
    def log_sync_complete(source_type, source_name, source_id=None, 
                         records_synced=0, duration=0, target_db=None, target_table=None, details=None):
        """Log successful sync completion"""
        details_str = f" | Details: {json.dumps(details)}" if details else ""
        id_str = f" | Source ID: {source_id}" if source_id else ""
        target_str = f" | Target: {target_db}.{target_table}" if target_db and target_table else ""
        if target_db:
            target_str = f" | Target DB: {target_db}{target_str}"
        
        sync_logger.info(
            f"SYNC_COMPLETE | Source Type: {source_type} | Source Name: {source_name}{id_str} | "
            f"Records: {records_synced} | Duration: {duration:.2f}s{target_str}{details_str}"
        )
        # Write to sync_history table
        history_details = f"{source_type} sync completed"
        if records_synced > 0:
            history_details += f": {records_synced:,} records"
        if details:
            if isinstance(details, dict):
                if 'tables_synced' in details:
                    history_details += f", {details['tables_synced']} tables"
                if 'target_table' in details:
                    history_details += f" → {details.get('target_db', target_db or '')}.{details['target_table']}"
            else:
                history_details += f" | {str(details)}"
        if target_db:
            history_details += f" → {target_db}"
        if target_table:
            history_details += f".{target_table}"
        SyncLogger._write_to_sync_history(source_name, "success", history_details)
    
    @staticmethod
    def log_sync_failed(source_type, source_name, source_id=None, error_msg=None, 
                       records_partial=0, duration=0, target_db=None, details=None):
        """Log sync failure"""
        details_str = f" | Details: {json.dumps(details)}" if details else ""
        id_str = f" | Source ID: {source_id}" if source_id else ""
        partial_str = f" | Partial Records: {records_partial}" if records_partial > 0 else ""
        target_str = f" | Target DB: {target_db}" if target_db else ""
        error_str = f" | Error: {error_msg}" if error_msg else ""
        
        sync_logger.error(
            f"SYNC_FAILED | Source Type: {source_type} | Source Name: {source_name}{id_str} | "
            f"Duration: {duration:.2f}s{partial_str}{target_str}{error_str}{details_str}"
        )
        # Write to sync_history table
        history_details = f"{source_type} sync failed"
        if error_msg:
            history_details += f": {error_msg[:200]}"  # Limit error message length
        if records_partial > 0:
            history_details += f" (partial: {records_partial} records)"
        if target_db:
            history_details += f" → {target_db}"
        SyncLogger._write_to_sync_history(source_name, "failed", history_details)
    
    @staticmethod
    def log_sync_partial(source_type, source_name, source_id=None,
                        records_synced=0, records_failed=0, duration=0, 
                        target_db=None, target_table=None, failed_tables=None, details=None):
        """Log partially failed sync"""
        details_str = f" | Details: {json.dumps(details)}" if details else ""
        id_str = f" | Source ID: {source_id}" if source_id else ""
        target_str = f" | Target: {target_db}.{target_table}" if target_db and target_table else ""
        if target_db:
            target_str = f" | Target DB: {target_db}{target_str}"
        failed_tables_str = f" | Failed Tables: {', '.join(failed_tables)}" if failed_tables else ""
        
        sync_logger.warning(
            f"SYNC_PARTIAL | Source Type: {source_type} | Source Name: {source_name}{id_str} | "
            f"Synced: {records_synced} | Failed: {records_failed} | Duration: {duration:.2f}s"
            f"{target_str}{failed_tables_str}{details_str}"
        )
        # Write to sync_history table
        history_details = f"{source_type} sync partial: {records_synced} synced, {records_failed} failed"
        if details:
            if isinstance(details, dict):
                if 'total_tables' in details:
                    history_details += f" ({details.get('successful', 0)}/{details['total_tables']} tables)"
                if 'failed' in details:
                    history_details += f" | {details['failed']} failed"
        if failed_tables:
            history_details += f" | Failed: {', '.join(failed_tables[:3])}"  # Limit to first 3
            if len(failed_tables) > 3:
                history_details += f" (+{len(failed_tables) - 3} more)"
        if target_db:
            history_details += f" → {target_db}"
        SyncLogger._write_to_sync_history(source_name, "partial", history_details)
    
    @staticmethod
    def log_table_sync(source_type, source_name, source_id=None, 
                      schema=None, table=None, records=0, status='success', duration=0, error=None):
        """Log individual table sync"""
        id_str = f" | Source ID: {source_id}" if source_id else ""
        table_str = f" | Table: {schema}.{table}" if schema and table else ""
        error_str = f" | Error: {error}" if error else ""
        
        if status == 'success':
            sync_logger.info(
                f"TABLE_SYNC | Source Type: {source_type} | Source Name: {source_name}{id_str}{table_str} | "
                f"Records: {records} | Status: {status} | Duration: {duration:.2f}s"
            )
        elif status == 'failed':
            sync_logger.error(
                f"TABLE_SYNC | Source Type: {source_type} | Source Name: {source_name}{id_str}{table_str} | "
                f"Records: {records} | Status: {status} | Duration: {duration:.2f}s{error_str}"
            )
        else:
            sync_logger.warning(
                f"TABLE_SYNC | Source Type: {source_type} | Source Name: {source_name}{id_str}{table_str} | "
                f"Records: {records} | Status: {status} | Duration: {duration:.2f}s{error_str}"
            )
    
    @staticmethod
    def log_connection_test(source_type, source_name, success=True, error_msg=None):
        """Log connection test"""
        if success:
            sync_logger.info(
                f"CONNECTION_TEST | Source Type: {source_type} | Source Name: {source_name} | Status: SUCCESS"
            )
        else:
            sync_logger.warning(
                f"CONNECTION_TEST | Source Type: {source_type} | Source Name: {source_name} | "
                f"Status: FAILED | Error: {error_msg}"
            )
    
    @staticmethod
    def log_progress(source_type, source_name, source_id=None, 
                    progress_percent=0, current=0, total=0, message=None):
        """Log sync progress"""
        id_str = f" | Source ID: {source_id}" if source_id else ""
        msg_str = f" | Message: {message}" if message else ""
        sync_logger.info(
            f"SYNC_PROGRESS | Source Type: {source_type} | Source Name: {source_name}{id_str} | "
            f"Progress: {progress_percent:.1f}% | {current}/{total}{msg_str}"
        )


def cleanup_old_logs(days=7):
    """Remove log entries older than specified days"""
    try:
        if not os.path.exists(SYNC_LOG_FILE):
            return
        
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # Read all lines
        with open(SYNC_LOG_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Filter lines within retention period
        valid_lines = []
        for line in lines:
            try:
                # Extract timestamp from line (format: YYYY-MM-DD HH:MM:SS)
                if len(line) >= 19:
                    timestamp_str = line[:19]
                    log_date = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                    if log_date >= cutoff_date:
                        valid_lines.append(line)
            except (ValueError, IndexError):
                # Keep lines that don't match format (header, etc.)
                valid_lines.append(line)
        
        # Write back only valid lines
        with open(SYNC_LOG_FILE, 'w', encoding='utf-8') as f:
            f.writelines(valid_lines)
        
        sync_logger.info(f"Log cleanup completed. Retained {len(valid_lines)} lines from last {days} days.")
        
    except Exception as e:
        sync_logger.error(f"Error cleaning up old logs: {e}")

