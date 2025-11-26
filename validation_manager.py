"""
Validation Manager - Integrates data integrity checks into sync operations
"""

import logging
import json
from datetime import datetime
from typing import Dict, List, Optional
from db_utils import get_pg_connection, return_pg_connection
from data_integrity import (
    ValidationResult,
    safe_sync_operation,
    RetryConfig,
    transaction_manager,
    detect_sync_gaps,
    GapInfo
)
from dataclasses import asdict
from clickhouse_validator import ClickHouseValidator

logger = logging.getLogger(__name__)


class ValidationManager:
    """Manages validation operations and stores results"""
    
    def __init__(self):
        self.validator = None
    
    def save_validation_result(self, result: ValidationResult, source_name: str, table_name: str, sync_id: str = None):
        """Save validation result to database"""
        try:
            conn = get_pg_connection()
            try:
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO metrics_sync_tables.validation_results
                    (source_name, table_name, validation_time, success, source_rows, target_rows,
                     missing_rows, extra_rows, mismatched_rows, checksum_match, source_checksum,
                     target_checksum, errors, warnings, details, sync_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    source_name,
                    table_name,
                    datetime.now(),
                    result.success,
                    result.source_rows,
                    result.target_rows,
                    result.missing_rows,
                    result.extra_rows,
                    result.mismatched_rows,
                    result.checksum_match,
                    result.source_checksum,
                    result.target_checksum,
                    json.dumps(result.errors) if result.errors else None,
                    json.dumps(result.warnings) if result.warnings else None,
                    json.dumps(result.details) if result.details else None,
                    sync_id
                ))
                conn.commit()
                cur.close()
                logger.info(f"Validation result saved for {source_name}.{table_name}")
            finally:
                return_pg_connection(conn)
        except Exception as e:
            logger.error(f"Error saving validation result: {e}")
    
    def validate_after_sync(
        self,
        source_name: str,
        table_name: str,
        source_data: List[Dict],
        target_database: str,
        key_columns: List[str] = None,
        sync_id: str = None
    ) -> ValidationResult:
        """Validate data after sync operation"""
        try:
            # Initialize validator
            validator = ClickHouseValidator(database=target_database)
            if not validator.connect():
                result = ValidationResult(
                    success=False,
                    source_rows=len(source_data) if source_data else 0,
                    target_rows=0,
                    missing_rows=0,
                    extra_rows=0,
                    mismatched_rows=0,
                    checksum_match=False
                )
                result.errors.append("Failed to connect to ClickHouse for validation")
                return result
            
            # Validate table data
            result = validator.validate_table_data(
                table_name=table_name,
                source_data=source_data,
                key_columns=key_columns
            )
            
            # Save result
            self.save_validation_result(result, source_name, table_name, sync_id)
            
            validator.close()
            return result
            
        except Exception as e:
            logger.error(f"Error during validation: {e}")
            result = ValidationResult(
                success=False,
                source_rows=len(source_data) if source_data else 0,
                target_rows=0,
                missing_rows=0,
                extra_rows=0,
                mismatched_rows=0,
                checksum_match=False
            )
            result.errors.append(f"Validation error: {str(e)}")
            return result
    
    def detect_and_save_gaps(
        self,
        source_name: str,
        table_name: str,
        sync_history: List[Dict],
        expected_interval_minutes: int = 60
    ) -> List[Dict]:
        """Detect sync gaps and save to database"""
        try:
            gaps = detect_sync_gaps(
                source_name=source_name,
                table_name=table_name,
                sync_history=sync_history,
                expected_interval_minutes=expected_interval_minutes
            )
            
            # Save gaps to database
            if gaps:
                conn = get_pg_connection()
                try:
                    cur = conn.cursor()
                    for gap in gaps:
                        cur.execute("""
                            INSERT INTO metrics_sync_tables.sync_gaps
                            (source_name, table_name, gap_start, gap_end, expected_syncs,
                             actual_syncs, missing_syncs, severity)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT DO NOTHING
                        """, (
                            gap.source_name,
                            gap.table_name,
                            gap.gap_start,
                            gap.gap_end,
                            gap.expected_syncs,
                            gap.actual_syncs,
                            gap.missing_syncs,
                            gap.severity
                        ))
                    conn.commit()
                    cur.close()
                    logger.info(f"Detected and saved {len(gaps)} gaps for {source_name}.{table_name}")
                finally:
                    return_pg_connection(conn)
            
            return [asdict(gap) for gap in gaps]
            
        except Exception as e:
            logger.error(f"Error detecting gaps: {e}")
            return []
    
    def get_validation_history(
        self,
        source_name: str = None,
        table_name: str = None,
        limit: int = 100
    ) -> List[Dict]:
        """Get validation history from database"""
        try:
            conn = get_pg_connection()
            try:
                cur = conn.cursor()
                query = """
                    SELECT id, source_name, table_name, validation_time, success,
                           source_rows, target_rows, missing_rows, extra_rows,
                           mismatched_rows, checksum_match, errors, warnings, sync_id
                    FROM metrics_sync_tables.validation_results
                    WHERE 1=1
                """
                params = []
                
                if source_name:
                    query += " AND source_name = %s"
                    params.append(source_name)
                
                if table_name:
                    query += " AND table_name = %s"
                    params.append(table_name)
                
                query += " ORDER BY validation_time DESC LIMIT %s"
                params.append(limit)
                
                cur.execute(query, params)
                rows = cur.fetchall()
                cur.close()
                
                results = []
                for row in rows:
                    results.append({
                        'id': row[0],
                        'source_name': row[1],
                        'table_name': row[2],
                        'validation_time': row[3].isoformat() if row[3] else None,
                        'success': row[4],
                        'source_rows': row[5],
                        'target_rows': row[6],
                        'missing_rows': row[7],
                        'extra_rows': row[8],
                        'mismatched_rows': row[9],
                        'checksum_match': row[10],
                        'errors': json.loads(row[11]) if row[11] else [],
                        'warnings': json.loads(row[12]) if row[12] else [],
                        'sync_id': row[13]
                    })
                
                return results
            finally:
                return_pg_connection(conn)
        except Exception as e:
            logger.error(f"Error getting validation history: {e}")
            return []
    
    def get_active_gaps(
        self,
        source_name: str = None,
        severity: str = None
    ) -> List[Dict]:
        """Get active (unrecovered) sync gaps"""
        try:
            conn = get_pg_connection()
            try:
                cur = conn.cursor()
                query = """
                    SELECT id, source_name, table_name, gap_start, gap_end,
                           expected_syncs, actual_syncs, missing_syncs, severity,
                           detected_at
                    FROM metrics_sync_tables.sync_gaps
                    WHERE recovered = FALSE
                """
                params = []
                
                if source_name:
                    query += " AND source_name = %s"
                    params.append(source_name)
                
                if severity:
                    query += " AND severity = %s"
                    params.append(severity)
                
                query += " ORDER BY severity DESC, detected_at DESC"
                
                cur.execute(query, params)
                rows = cur.fetchall()
                cur.close()
                
                results = []
                for row in rows:
                    results.append({
                        'id': row[0],
                        'source_name': row[1],
                        'table_name': row[2],
                        'gap_start': row[3].isoformat() if row[3] else None,
                        'gap_end': row[4].isoformat() if row[4] else None,
                        'expected_syncs': row[5],
                        'actual_syncs': row[6],
                        'missing_syncs': row[7],
                        'severity': row[8],
                        'detected_at': row[9].isoformat() if row[9] else None
                    })
                
                return results
            finally:
                return_pg_connection(conn)
        except Exception as e:
            logger.error(f"Error getting active gaps: {e}")
            return []


# Global validation manager instance
validation_manager = ValidationManager()

