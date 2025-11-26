"""
Sync Integrity Wrapper - Adds validation and integrity checks to existing sync functions
This module wraps existing sync functions to add Phase 1 features without breaking existing code
"""

import logging
import uuid
from typing import Dict, List, Optional, Callable, Any
from datetime import datetime
from data_integrity import (
    safe_sync_operation,
    RetryConfig,
    transaction_manager,
    ValidationResult
)
from validation_manager import validation_manager
from clickhouse_validator import ClickHouseValidator

logger = logging.getLogger(__name__)


def with_integrity_checks(
    sync_function: Callable,
    source_name: str,
    table_name: str,
    target_database: str,
    enable_validation: bool = True,
    enable_retry: bool = True,
    enable_transaction: bool = True,
    retry_config: RetryConfig = None
):
    """
    Wrapper that adds integrity checks to sync functions.
    
    Args:
        sync_function: The sync function to wrap
        source_name: Name of the data source
        table_name: Name of the table being synced
        target_database: Target database name
        enable_validation: Enable post-sync validation
        enable_retry: Enable retry on failure
        enable_transaction: Enable transaction management
        retry_config: Retry configuration
    
    Returns:
        Wrapped function with integrity checks
    """
    def wrapper(*args, **kwargs):
        # Generate transaction ID
        transaction_id = f"{source_name}_{table_name}_{uuid.uuid4().hex[:8]}"
        
        # Store original data for validation
        original_data = None
        if enable_validation and 'data' in kwargs:
            original_data = kwargs.get('data')
        elif enable_validation and len(args) > 0:
            # Try to extract data from first argument if it's a list/dict
            if isinstance(args[0], (list, dict)):
                original_data = args[0]
        
        # Configure retry if enabled
        if enable_retry and retry_config is None:
            retry_config = RetryConfig(
                max_attempts=3,
                initial_delay=1.0,
                max_delay=60.0
            )
        
        # Execute with integrity checks
        if enable_transaction or enable_retry:
            success, result, error = safe_sync_operation(
                operation_name=f"{source_name}.{table_name}",
                sync_function=sync_function,
                transaction_id=transaction_id,
                retry_config=retry_config if enable_retry else None,
                *args,
                **kwargs
            )
        else:
            # Execute without wrapper
            try:
                result = sync_function(*args, **kwargs)
                success = result.get('success', True) if isinstance(result, dict) else True
                error = None if success else "Unknown error"
            except Exception as e:
                success = False
                result = None
                error = str(e)
        
        # Perform validation if enabled and sync was successful
        if enable_validation and success and original_data:
            try:
                logger.info(f"Validating sync result for {source_name}.{table_name}")
                
                # Convert data to list of dicts if needed
                if isinstance(original_data, dict):
                    data_list = [original_data]
                elif isinstance(original_data, list):
                    data_list = original_data
                else:
                    data_list = []
                
                # Perform validation
                validation_result = validation_manager.validate_after_sync(
                    source_name=source_name,
                    table_name=table_name,
                    source_data=data_list,
                    target_database=target_database,
                    sync_id=transaction_id
                )
                
                # Log validation result
                if validation_result.success:
                    logger.info(f"✅ Validation passed for {source_name}.{table_name}")
                else:
                    logger.warning(f"⚠️ Validation failed for {source_name}.{table_name}: "
                                 f"{validation_result.missing_rows} missing, "
                                 f"{validation_result.extra_rows} extra rows")
                    
                    # Add validation errors to result
                    if isinstance(result, dict):
                        result['validation_errors'] = validation_result.errors
                        result['validation_warnings'] = validation_result.warnings
                        result['validation_result'] = {
                            'success': validation_result.success,
                            'missing_rows': validation_result.missing_rows,
                            'extra_rows': validation_result.extra_rows,
                            'mismatched_rows': validation_result.mismatched_rows
                        }
            except Exception as e:
                logger.error(f"Error during validation: {e}", exc_info=True)
                # Don't fail the sync if validation fails
        
        # Return result
        if isinstance(result, dict):
            result['transaction_id'] = transaction_id
            if not success:
                result['error'] = error
        
        return result
    
    return wrapper


def validate_api_sync_result(
    api_url: str,
    target_database: str,
    target_table: str,
    source_data: List[Dict],
    records_synced: int
) -> ValidationResult:
    """
    Validate API sync result.
    This can be called after sync_api_to_clickhouse_once completes.
    """
    try:
        logger.info(f"Validating API sync result for {target_database}.{target_table}")
        
        # Use validation manager
        result = validation_manager.validate_after_sync(
            source_name=api_url,
            table_name=target_table,
            source_data=source_data[:records_synced] if source_data else [],
            target_database=target_database
        )
        
        return result
    except Exception as e:
        logger.error(f"Error validating API sync: {e}")
        return ValidationResult(
            success=False,
            source_rows=len(source_data) if source_data else 0,
            target_rows=0,
            missing_rows=0,
            extra_rows=0,
            mismatched_rows=0,
            checksum_match=False,
            errors=[f"Validation error: {str(e)}"]
        )


def validate_hana_sync_result(
    hana_source: str,
    schema_name: str,
    table_name: str,
    target_database: str,
    source_data: List[Dict],
    records_synced: int
) -> ValidationResult:
    """
    Validate HANA sync result.
    This can be called after HANA table sync completes.
    """
    try:
        logger.info(f"Validating HANA sync result for {schema_name}.{table_name}")
        
        # Use validation manager
        result = validation_manager.validate_after_sync(
            source_name=hana_source,
            table_name=f"{schema_name}_{table_name}",
            source_data=source_data[:records_synced] if source_data else [],
            target_database=target_database
        )
        
        return result
    except Exception as e:
        logger.error(f"Error validating HANA sync: {e}")
        return ValidationResult(
            success=False,
            source_rows=len(source_data) if source_data else 0,
            target_rows=0,
            missing_rows=0,
            extra_rows=0,
            mismatched_rows=0,
            checksum_match=False,
            errors=[f"Validation error: {str(e)}"]
        )


def check_sync_gaps(
    source_name: str,
    table_name: str,
    expected_interval_minutes: int = 60
) -> List[Dict]:
    """
    Check for sync gaps in history.
    This should be called periodically to detect missing syncs.
    """
    try:
        from dashboard import get_last_10_syncs
        from db_utils import get_pg_connection, return_pg_connection
        
        # Get sync history for this source/table
        conn = get_pg_connection()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT sync_time, status, details
                FROM metrics_sync_tables.sync_history
                WHERE server_name = %s
                AND details LIKE %s
                ORDER BY sync_time DESC
                LIMIT 100
            """, (source_name, f"%{table_name}%"))
            
            sync_history = []
            for row in cur.fetchall():
                sync_history.append({
                    'sync_time': row[0],
                    'status': row[1],
                    'details': row[2]
                })
            
            cur.close()
        finally:
            return_pg_connection(conn)
        
        # Detect gaps
        gaps = validation_manager.detect_and_save_gaps(
            source_name=source_name,
            table_name=table_name,
            sync_history=sync_history,
            expected_interval_minutes=expected_interval_minutes
        )
        
        return gaps
    except Exception as e:
        logger.error(f"Error checking sync gaps: {e}")
        return []


def get_validation_summary(source_name: str = None, days: int = 7) -> Dict:
    """
    Get validation summary for recent syncs.
    """
    try:
        from db_utils import get_pg_connection, return_pg_connection
        
        conn = get_pg_connection()
        try:
            cur = conn.cursor()
            query = f"""
                SELECT 
                    COUNT(*) as total_validations,
                    COUNT(CASE WHEN success = TRUE THEN 1 END) as successful,
                    COUNT(CASE WHEN success = FALSE THEN 1 END) as failed,
                    SUM(missing_rows) as total_missing,
                    SUM(extra_rows) as total_extra,
                    SUM(mismatched_rows) as total_mismatched
                FROM metrics_sync_tables.validation_results
                WHERE validation_time >= NOW() - INTERVAL '{days} days'
            """
            params = []
            
            if source_name:
                query += " AND source_name = %s"
                params.append(source_name)
            
            cur.execute(query, params)
            row = cur.fetchone()
            cur.close()
            
            if row:
                return {
                    'total_validations': row[0] or 0,
                    'successful': row[1] or 0,
                    'failed': row[2] or 0,
                    'success_rate': (row[1] / row[0] * 100) if row[0] > 0 else 0,
                    'total_missing_rows': row[3] or 0,
                    'total_extra_rows': row[4] or 0,
                    'total_mismatched_rows': row[5] or 0
                }
            else:
                return {
                    'total_validations': 0,
                    'successful': 0,
                    'failed': 0,
                    'success_rate': 0,
                    'total_missing_rows': 0,
                    'total_extra_rows': 0,
                    'total_mismatched_rows': 0
                }
        finally:
            return_pg_connection(conn)
    except Exception as e:
        logger.error(f"Error getting validation summary: {e}")
        return {}

