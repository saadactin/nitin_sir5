"""
Data Integrity Module - Phase 1: Critical for 100% Accuracy
Implements:
1. Row-level checksum validation
2. Full table comparison utility
3. Gap detection and recovery
4. Enhanced error handling with retry
5. Transaction integrity
"""

import hashlib
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
from functools import wraps
import traceback

logger = logging.getLogger(__name__)


# ==================== Data Classes ====================

@dataclass
class ValidationResult:
    """Result of a validation operation"""
    success: bool
    source_rows: int
    target_rows: int
    missing_rows: int
    extra_rows: int
    mismatched_rows: int
    checksum_match: bool
    source_checksum: Optional[str] = None
    target_checksum: Optional[str] = None
    errors: List[str] = None
    warnings: List[str] = None
    details: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.warnings is None:
            self.warnings = []
        if self.details is None:
            self.details = {}


@dataclass
class GapInfo:
    """Information about a sync gap"""
    source_name: str
    table_name: str
    gap_start: datetime
    gap_end: datetime
    expected_syncs: int
    actual_syncs: int
    missing_syncs: int
    severity: str  # 'low', 'medium', 'high', 'critical'


@dataclass
class RetryConfig:
    """Configuration for retry mechanism"""
    max_attempts: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    retryable_exceptions: Tuple = (ConnectionError, TimeoutError, IOError)
    jitter: bool = True


# ==================== Checksum Functions ====================

def compute_row_checksum(row: Dict, exclude_columns: List[str] = None) -> str:
    """
    Compute checksum for a single row.
    
    Args:
        row: Dictionary representing a row
        exclude_columns: Columns to exclude from checksum (e.g., timestamps)
    
    Returns:
        SHA256 hash of the row data
    """
    if exclude_columns is None:
        exclude_columns = []
    
    # Create a copy and exclude specified columns
    row_copy = {k: v for k, v in row.items() if k not in exclude_columns}
    
    # Sort keys for consistent hashing
    sorted_items = sorted(row_copy.items())
    
    # Convert to JSON string for hashing
    row_str = json.dumps(sorted_items, default=str, sort_keys=True)
    
    # Compute SHA256 hash
    return hashlib.sha256(row_str.encode('utf-8')).hexdigest()


def compute_table_checksum(rows: List[Dict], exclude_columns: List[str] = None) -> str:
    """
    Compute checksum for entire table.
    
    Args:
        rows: List of dictionaries representing rows
        exclude_columns: Columns to exclude from checksum
    
    Returns:
        SHA256 hash of all rows combined
    """
    if not rows:
        return hashlib.sha256(b'').hexdigest()
    
    # Compute checksum for each row
    row_checksums = []
    for row in rows:
        row_checksum = compute_row_checksum(row, exclude_columns)
        row_checksums.append(row_checksum)
    
    # Sort row checksums for consistent ordering
    row_checksums.sort()
    
    # Combine all checksums
    combined = ''.join(row_checksums)
    
    # Compute final hash
    return hashlib.sha256(combined.encode('utf-8')).hexdigest()


def compute_batch_checksum(rows: List[Dict], batch_size: int = 1000, exclude_columns: List[str] = None) -> str:
    """
    Compute checksum for large tables in batches.
    
    Args:
        rows: List of dictionaries representing rows
        batch_size: Number of rows to process per batch
        exclude_columns: Columns to exclude from checksum
    
    Returns:
        SHA256 hash of all rows
    """
    if not rows:
        return hashlib.sha256(b'').hexdigest()
    
    batch_checksums = []
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        batch_checksum = compute_table_checksum(batch, exclude_columns)
        batch_checksums.append(batch_checksum)
    
    # Combine batch checksums
    combined = ''.join(batch_checksums)
    return hashlib.sha256(combined.encode('utf-8')).hexdigest()


# ==================== Validation Functions ====================

def validate_row_counts(source_count: int, target_count: int, tolerance: float = 0.0) -> Tuple[bool, str]:
    """
    Validate row counts match.
    
    Args:
        source_count: Number of rows in source
        target_count: Number of rows in target
        tolerance: Allowed difference as percentage (0.0 = exact match)
    
    Returns:
        Tuple of (is_valid, message)
    """
    if source_count == target_count:
        return True, "Row counts match exactly"
    
    diff = abs(source_count - target_count)
    if tolerance > 0:
        percentage_diff = (diff / source_count * 100) if source_count > 0 else 100
        if percentage_diff <= tolerance:
            return True, f"Row counts within tolerance ({percentage_diff:.2f}% difference)"
    
    return False, f"Row count mismatch: source={source_count}, target={target_count}, diff={diff}"


def compare_tables_full(
    source_rows: List[Dict],
    target_rows: List[Dict],
    key_columns: List[str] = None,
    exclude_columns: List[str] = None
) -> ValidationResult:
    """
    Perform full table comparison with row-level checksums.
    
    Args:
        source_rows: List of source rows (as dictionaries)
        target_rows: List of target rows (as dictionaries)
        key_columns: Columns to use as primary key for matching
        exclude_columns: Columns to exclude from comparison
    
    Returns:
        ValidationResult with detailed comparison results
    """
    result = ValidationResult(
        success=True,
        source_rows=len(source_rows),
        target_rows=len(target_rows),
        missing_rows=0,
        extra_rows=0,
        mismatched_rows=0,
        checksum_match=False
    )
    
    try:
        # Compute table-level checksums
        source_checksum = compute_table_checksum(source_rows, exclude_columns)
        target_checksum = compute_table_checksum(target_rows, exclude_columns)
        
        result.source_checksum = source_checksum
        result.target_checksum = target_checksum
        result.checksum_match = (source_checksum == target_checksum)
        
        if result.checksum_match:
            result.success = True
            result.details['message'] = "Tables match exactly (checksum verified)"
            return result
        
        # If checksums don't match, do detailed comparison
        if key_columns:
            # Use key columns for matching
            source_dict = {}
            for row in source_rows:
                key = tuple(row.get(col) for col in key_columns)
                source_dict[key] = row
            
            target_dict = {}
            for row in target_rows:
                key = tuple(row.get(col) for col in key_columns)
                target_dict[key] = row
            
            # Find missing rows (in source but not in target)
            missing_keys = set(source_dict.keys()) - set(target_dict.keys())
            result.missing_rows = len(missing_keys)
            
            # Find extra rows (in target but not in source)
            extra_keys = set(target_dict.keys()) - set(source_dict.keys())
            result.extra_rows = len(extra_keys)
            
            # Find mismatched rows (same key but different data)
            common_keys = set(source_dict.keys()) & set(target_dict.keys())
            mismatched = 0
            for key in common_keys:
                source_row = source_dict[key]
                target_row = target_dict[key]
                source_chk = compute_row_checksum(source_row, exclude_columns)
                target_chk = compute_row_checksum(target_row, exclude_columns)
                if source_chk != target_chk:
                    mismatched += 1
            
            result.mismatched_rows = mismatched
            
        else:
            # No key columns - use row checksums for comparison
            source_checksums = {compute_row_checksum(row, exclude_columns): row for row in source_rows}
            target_checksums = {compute_row_checksum(row, exclude_columns): row for row in target_rows}
            
            result.missing_rows = len(set(source_checksums.keys()) - set(target_checksums.keys()))
            result.extra_rows = len(set(target_checksums.keys()) - set(source_checksums.keys()))
            result.mismatched_rows = 0  # Can't detect mismatches without keys
        
        # Determine overall success
        result.success = (
            result.missing_rows == 0 and
            result.extra_rows == 0 and
            result.mismatched_rows == 0
        )
        
        if not result.success:
            result.errors.append(
                f"Data mismatch: {result.missing_rows} missing, "
                f"{result.extra_rows} extra, {result.mismatched_rows} mismatched rows"
            )
        
    except Exception as e:
        result.success = False
        result.errors.append(f"Error during comparison: {str(e)}")
        logger.error(f"Table comparison error: {e}", exc_info=True)
    
    return result


# ==================== Gap Detection ====================

def detect_sync_gaps(
    source_name: str,
    table_name: str,
    sync_history: List[Dict],
    expected_interval_minutes: int = 60,
    lookback_hours: int = 24
) -> List[GapInfo]:
    """
    Detect gaps in sync history.
    
    Args:
        source_name: Name of the data source
        table_name: Name of the table
        sync_history: List of sync records with 'sync_time' field
        expected_interval_minutes: Expected time between syncs in minutes
        lookback_hours: How many hours to look back
    
    Returns:
        List of GapInfo objects representing detected gaps
    """
    gaps = []
    
    if not sync_history:
        return gaps
    
    # Sort sync history by time
    sorted_history = sorted(sync_history, key=lambda x: x.get('sync_time', datetime.min))
    
    # Check for gaps between syncs
    for i in range(len(sorted_history) - 1):
        current_sync = sorted_history[i]
        next_sync = sorted_history[i + 1]
        
        current_time = current_sync.get('sync_time')
        next_time = next_sync.get('sync_time')
        
        if not current_time or not next_time:
            continue
        
        # Convert to datetime if needed
        if isinstance(current_time, str):
            current_time = datetime.fromisoformat(current_time.replace('Z', '+00:00'))
        if isinstance(next_time, str):
            next_time = datetime.fromisoformat(next_time.replace('Z', '+00:00'))
        
        time_diff = (next_time - current_time).total_seconds() / 60  # minutes
        
        # If gap is larger than expected interval, it's a gap
        if time_diff > expected_interval_minutes * 1.5:  # 50% tolerance
            expected_syncs = int(time_diff / expected_interval_minutes)
            actual_syncs = 1  # Only one sync in this period
            missing_syncs = expected_syncs - actual_syncs
            
            # Determine severity
            if missing_syncs >= 10:
                severity = 'critical'
            elif missing_syncs >= 5:
                severity = 'high'
            elif missing_syncs >= 2:
                severity = 'medium'
            else:
                severity = 'low'
            
            gap = GapInfo(
                source_name=source_name,
                table_name=table_name,
                gap_start=current_time,
                gap_end=next_time,
                expected_syncs=expected_syncs,
                actual_syncs=actual_syncs,
                missing_syncs=missing_syncs,
                severity=severity
            )
            gaps.append(gap)
    
    return gaps


def recover_from_gap(
    gap: GapInfo,
    sync_function: callable,
    *args,
    **kwargs
) -> Tuple[bool, str]:
    """
    Attempt to recover from a sync gap by backfilling missing data.
    
    Args:
        gap: GapInfo object describing the gap
        sync_function: Function to call for syncing
        *args, **kwargs: Arguments to pass to sync function
    
    Returns:
        Tuple of (success, message)
    """
    try:
        logger.info(f"Recovering gap for {gap.source_name}.{gap.table_name} "
                   f"from {gap.gap_start} to {gap.gap_end}")
        
        # Call sync function with gap time range
        result = sync_function(*args, **kwargs, 
                              start_time=gap.gap_start,
                              end_time=gap.gap_end)
        
        if result:
            return True, f"Successfully recovered gap: {gap.missing_syncs} syncs backfilled"
        else:
            return False, f"Failed to recover gap"
    
    except Exception as e:
        logger.error(f"Error recovering gap: {e}", exc_info=True)
        return False, f"Error during gap recovery: {str(e)}"


# ==================== Retry Mechanism ====================

def retry_with_backoff(
    func: callable,
    config: RetryConfig = None,
    *args,
    **kwargs
) -> Any:
    """
    Execute a function with exponential backoff retry.
    
    Args:
        func: Function to execute
        config: RetryConfig object
        *args, **kwargs: Arguments to pass to function
    
    Returns:
        Result of function call
    
    Raises:
        Last exception if all retries fail
    """
    if config is None:
        config = RetryConfig()
    
    last_exception = None
    
    for attempt in range(1, config.max_attempts + 1):
        try:
            return func(*args, **kwargs)
        
        except Exception as e:
            last_exception = e
            
            # Check if exception is retryable
            if not isinstance(e, config.retryable_exceptions):
                logger.error(f"Non-retryable exception: {e}")
                raise
            
            # Don't retry on last attempt
            if attempt >= config.max_attempts:
                logger.error(f"Max retries ({config.max_attempts}) exceeded. Last error: {e}")
                raise
            
            # Calculate delay
            delay = min(
                config.initial_delay * (config.exponential_base ** (attempt - 1)),
                config.max_delay
            )
            
            # Add jitter if enabled
            if config.jitter:
                import random
                delay = delay * (0.5 + random.random() * 0.5)
            
            logger.warning(f"Attempt {attempt}/{config.max_attempts} failed: {e}. Retrying in {delay:.2f}s...")
            time.sleep(delay)
    
    # Should never reach here, but just in case
    raise last_exception


def retryable(config: RetryConfig = None):
    """
    Decorator for making functions retryable.
    
    Usage:
        @retryable(RetryConfig(max_attempts=5))
        def my_function():
            ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            return retry_with_backoff(func, config, *args, **kwargs)
        return wrapper
    return decorator


# ==================== Transaction Integrity ====================

class TransactionManager:
    """Manages transaction integrity for sync operations"""
    
    def __init__(self):
        self.active_transactions = {}
        self.transaction_log = []
    
    def start_transaction(self, transaction_id: str, description: str = None):
        """Start a new transaction"""
        transaction = {
            'id': transaction_id,
            'description': description,
            'start_time': datetime.now(),
            'status': 'active',
            'operations': []
        }
        self.active_transactions[transaction_id] = transaction
        logger.info(f"Transaction {transaction_id} started: {description}")
        return transaction
    
    def log_operation(self, transaction_id: str, operation: str, details: Dict = None):
        """Log an operation within a transaction"""
        if transaction_id in self.active_transactions:
            op = {
                'operation': operation,
                'timestamp': datetime.now(),
                'details': details or {}
            }
            self.active_transactions[transaction_id]['operations'].append(op)
            logger.debug(f"Transaction {transaction_id}: {operation}")
    
    def commit_transaction(self, transaction_id: str) -> bool:
        """Commit a transaction"""
        if transaction_id not in self.active_transactions:
            logger.error(f"Transaction {transaction_id} not found")
            return False
        
        transaction = self.active_transactions[transaction_id]
        transaction['end_time'] = datetime.now()
        transaction['status'] = 'committed'
        transaction['duration'] = (transaction['end_time'] - transaction['start_time']).total_seconds()
        
        self.transaction_log.append(transaction)
        del self.active_transactions[transaction_id]
        
        logger.info(f"Transaction {transaction_id} committed in {transaction['duration']:.2f}s")
        return True
    
    def rollback_transaction(self, transaction_id: str, reason: str = None) -> bool:
        """Rollback a transaction"""
        if transaction_id not in self.active_transactions:
            logger.error(f"Transaction {transaction_id} not found")
            return False
        
        transaction = self.active_transactions[transaction_id]
        transaction['end_time'] = datetime.now()
        transaction['status'] = 'rolled_back'
        transaction['reason'] = reason
        transaction['duration'] = (transaction['end_time'] - transaction['start_time']).total_seconds()
        
        self.transaction_log.append(transaction)
        del self.active_transactions[transaction_id]
        
        logger.warning(f"Transaction {transaction_id} rolled back: {reason}")
        return True
    
    def get_transaction_status(self, transaction_id: str) -> Optional[Dict]:
        """Get status of a transaction"""
        if transaction_id in self.active_transactions:
            return self.active_transactions[transaction_id]
        
        # Check log
        for txn in reversed(self.transaction_log):
            if txn['id'] == transaction_id:
                return txn
        
        return None


# Global transaction manager instance
transaction_manager = TransactionManager()


# ==================== Integration Helpers ====================

def validate_sync_result(
    source_name: str,
    table_name: str,
    source_rows: List[Dict],
    target_rows: List[Dict],
    key_columns: List[str] = None
) -> ValidationResult:
    """
    Validate sync result by comparing source and target data.
    
    This is the main entry point for validation after a sync operation.
    """
    logger.info(f"Validating sync result for {source_name}.{table_name}")
    
    # Perform full comparison
    result = compare_tables_full(
        source_rows=source_rows,
        target_rows=target_rows,
        key_columns=key_columns
    )
    
    # Log results
    if result.success:
        logger.info(f"Validation passed for {source_name}.{table_name}: "
                   f"{result.source_rows} rows, checksum match")
    else:
        logger.warning(f"Validation failed for {source_name}.{table_name}: "
                      f"{result.missing_rows} missing, {result.extra_rows} extra, "
                      f"{result.mismatched_rows} mismatched")
    
    return result


def safe_sync_operation(
    operation_name: str,
    sync_function: callable,
    transaction_id: str = None,
    retry_config: RetryConfig = None,
    *args,
    **kwargs
) -> Tuple[bool, Any, Optional[str]]:
    """
    Execute a sync operation with full integrity checks.
    
    Args:
        operation_name: Name of the operation (for logging)
        sync_function: Function to execute
        transaction_id: Optional transaction ID
        retry_config: Retry configuration
        *args, **kwargs: Arguments for sync function
    
    Returns:
        Tuple of (success, result, error_message)
    """
    if transaction_id is None:
        transaction_id = f"{operation_name}_{int(time.time())}"
    
    # Start transaction
    transaction_manager.start_transaction(transaction_id, operation_name)
    
    try:
        # Execute with retry
        if retry_config:
            result = retry_with_backoff(sync_function, retry_config, *args, **kwargs)
        else:
            result = sync_function(*args, **kwargs)
        
        # Commit transaction
        transaction_manager.commit_transaction(transaction_id)
        
        return True, result, None
    
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Sync operation {operation_name} failed: {error_msg}", exc_info=True)
        
        # Rollback transaction
        transaction_manager.rollback_transaction(transaction_id, error_msg)
        
        return False, None, error_msg

