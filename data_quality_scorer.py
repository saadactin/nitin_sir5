"""
Data Quality Scoring Module - Phase 2
Calculates quality scores for synced data
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime, date
from dataclasses import dataclass
from db_utils import get_pg_connection, return_pg_connection
import json

logger = logging.getLogger(__name__)


@dataclass
class QualityScore:
    """Data quality score for a table"""
    overall_score: float  # 0-100
    completeness_score: float  # 0-100
    accuracy_score: float  # 0-100
    consistency_score: float  # 0-100
    timeliness_score: float  # 0-100
    uniqueness_score: float  # 0-100
    total_rows: int
    null_count: int
    duplicate_count: int
    error_count: int
    details: Dict


class DataQualityScorer:
    """Calculates and stores data quality scores"""
    
    def calculate_quality_score(
        self,
        source_name: str,
        table_name: str,
        data: List[Dict],
        target_data: List[Dict] = None
    ) -> QualityScore:
        """
        Calculate comprehensive data quality score.
        
        Args:
            source_name: Name of the data source
            table_name: Name of the table
            data: Source data to analyze
            target_data: Target data for comparison (optional)
        
        Returns:
            QualityScore object
        """
        if not data:
            return QualityScore(
                overall_score=0.0,
                completeness_score=0.0,
                accuracy_score=0.0,
                consistency_score=0.0,
                timeliness_score=0.0,
                uniqueness_score=0.0,
                total_rows=0,
                null_count=0,
                duplicate_count=0,
                error_count=0,
                details={'error': 'No data to analyze'}
            )
        
        total_rows = len(data)
        
        # 1. Completeness Score (null values)
        null_count = 0
        total_fields = 0
        for row in data:
            for key, value in row.items():
                total_fields += 1
                if value is None or value == '':
                    null_count += 1
        
        completeness_score = max(0, 100 - (null_count / total_fields * 100)) if total_fields > 0 else 100
        
        # 2. Uniqueness Score (duplicates)
        from data_integrity import compute_row_checksum
        row_hashes = [compute_row_checksum(row) for row in data]
        unique_hashes = set(row_hashes)
        duplicate_count = total_rows - len(unique_hashes)
        uniqueness_score = max(0, 100 - (duplicate_count / total_rows * 100)) if total_rows > 0 else 100
        
        # 3. Accuracy Score (if target data provided)
        accuracy_score = 100.0
        if target_data:
            from data_integrity import compare_tables_full
            validation_result = compare_tables_full(data, target_data)
            if validation_result.success:
                accuracy_score = 100.0
            else:
                # Penalize for mismatches
                mismatch_penalty = (
                    validation_result.missing_rows +
                    validation_result.extra_rows +
                    validation_result.mismatched_rows
                ) / total_rows * 100
                accuracy_score = max(0, 100 - mismatch_penalty)
        
        # 4. Consistency Score (data type consistency, format consistency)
        consistency_score = self._calculate_consistency_score(data)
        
        # 5. Timeliness Score (based on sync frequency and freshness)
        timeliness_score = self._calculate_timeliness_score(source_name, table_name)
        
        # Overall score (weighted average)
        overall_score = (
            completeness_score * 0.25 +
            uniqueness_score * 0.20 +
            accuracy_score * 0.25 +
            consistency_score * 0.15 +
            timeliness_score * 0.15
        )
        
        # Error count (rows with validation errors)
        error_count = 0
        for row in data:
            # Check for common data quality issues
            if self._has_data_quality_issues(row):
                error_count += 1
        
        details = {
            'completeness': {
                'null_count': null_count,
                'total_fields': total_fields,
                'null_percentage': (null_count / total_fields * 100) if total_fields > 0 else 0
            },
            'uniqueness': {
                'duplicate_count': duplicate_count,
                'unique_rows': len(unique_hashes),
                'duplicate_percentage': (duplicate_count / total_rows * 100) if total_rows > 0 else 0
            },
            'consistency': {
                'type_consistency': consistency_score
            }
        }
        
        return QualityScore(
            overall_score=round(overall_score, 2),
            completeness_score=round(completeness_score, 2),
            accuracy_score=round(accuracy_score, 2),
            consistency_score=round(consistency_score, 2),
            timeliness_score=round(timeliness_score, 2),
            uniqueness_score=round(uniqueness_score, 2),
            total_rows=total_rows,
            null_count=null_count,
            duplicate_count=duplicate_count,
            error_count=error_count,
            details=details
        )
    
    def _calculate_consistency_score(self, data: List[Dict]) -> float:
        """Calculate consistency score based on data type and format consistency"""
        if not data:
            return 100.0
        
        # Check type consistency for each column
        type_consistency = 0.0
        columns_checked = 0
        
        # Get all columns from first row
        first_row = data[0]
        for col_name in first_row.keys():
            # Skip metadata columns
            if col_name.startswith('_'):
                continue
            
            # Check if all values in this column have consistent types
            first_value = first_row[col_name]
            first_type = type(first_value).__name__
            
            consistent = True
            for row in data[1:]:
                if col_name in row:
                    value = row[col_name]
                    value_type = type(value).__name__
                    # Allow None values
                    if value is not None and first_value is not None:
                        if value_type != first_type:
                            consistent = False
                            break
            
            if consistent:
                type_consistency += 1.0
            columns_checked += 1
        
        return (type_consistency / columns_checked * 100) if columns_checked > 0 else 100.0
    
    def _calculate_timeliness_score(self, source_name: str, table_name: str) -> float:
        """Calculate timeliness score based on sync frequency"""
        try:
            from db_utils import get_pg_connection, return_pg_connection
            
            conn = get_pg_connection()
            try:
                cur = conn.cursor()
                cur.execute("""
                    SELECT MAX(sync_time) as last_sync
                    FROM metrics_sync_tables.sync_history
                    WHERE server_name = %s
                    AND details LIKE %s
                """, (source_name, f"%{table_name}%"))
                
                row = cur.fetchone()
                cur.close()
                
                if row and row[0]:
                    last_sync = row[0]
                    if isinstance(last_sync, str):
                        from dateutil import parser
                        last_sync = parser.parse(last_sync)
                    
                    hours_since_sync = (datetime.now() - last_sync).total_seconds() / 3600
                    
                    # Score decreases as time since last sync increases
                    # 100% if synced within last hour, 0% if more than 24 hours
                    if hours_since_sync <= 1:
                        return 100.0
                    elif hours_since_sync <= 24:
                        return max(0, 100 - (hours_since_sync - 1) * 4.35)  # Linear decrease
                    else:
                        return 0.0
                
                return 50.0  # Default if no sync history
            finally:
                return_pg_connection(conn)
        except Exception as e:
            logger.warning(f"Error calculating timeliness score: {e}")
            return 50.0
    
    def _has_data_quality_issues(self, row: Dict) -> bool:
        """Check if a row has data quality issues"""
        # Check for common issues
        for key, value in row.items():
            # Skip metadata columns
            if key.startswith('_'):
                continue
            
            # Check for invalid values
            if isinstance(value, str):
                # Check for suspicious patterns
                if value.strip() == '':
                    continue  # Empty strings are handled by completeness
                if len(value) > 10000:  # Suspiciously long strings
                    return True
            elif isinstance(value, (int, float)):
                # Check for extreme values
                if abs(value) > 1e15:
                    return True
        
        return False
    
    def save_quality_score(
        self,
        source_name: str,
        table_name: str,
        score: QualityScore,
        score_date: date = None
    ):
        """Save quality score to database"""
        if score_date is None:
            score_date = date.today()
        
        try:
            conn = get_pg_connection()
            try:
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO metrics_sync_tables.data_quality_scores
                    (source_name, table_name, score_date, overall_score,
                     completeness_score, accuracy_score, consistency_score,
                     timeliness_score, uniqueness_score, total_rows, null_count,
                     duplicate_count, error_count, details)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source_name, table_name, score_date)
                    DO UPDATE SET
                        overall_score = EXCLUDED.overall_score,
                        completeness_score = EXCLUDED.completeness_score,
                        accuracy_score = EXCLUDED.accuracy_score,
                        consistency_score = EXCLUDED.consistency_score,
                        timeliness_score = EXCLUDED.timeliness_score,
                        uniqueness_score = EXCLUDED.uniqueness_score,
                        total_rows = EXCLUDED.total_rows,
                        null_count = EXCLUDED.null_count,
                        duplicate_count = EXCLUDED.duplicate_count,
                        error_count = EXCLUDED.error_count,
                        details = EXCLUDED.details,
                        created_at = NOW()
                """, (
                    source_name,
                    table_name,
                    score_date,
                    score.overall_score,
                    score.completeness_score,
                    score.accuracy_score,
                    score.consistency_score,
                    score.timeliness_score,
                    score.uniqueness_score,
                    score.total_rows,
                    score.null_count,
                    score.duplicate_count,
                    score.error_count,
                    json.dumps(score.details)
                ))
                conn.commit()
                cur.close()
                logger.info(f"Saved quality score for {source_name}.{table_name}: {score.overall_score:.2f}")
            finally:
                return_pg_connection(conn)
        except Exception as e:
            logger.error(f"Error saving quality score: {e}")
    
    def get_quality_scores(
        self,
        source_name: str = None,
        table_name: str = None,
        days: int = 30
    ) -> List[Dict]:
        """Get quality scores from database"""
        try:
            conn = get_pg_connection()
            try:
                cur = conn.cursor()
                query = """
                    SELECT source_name, table_name, score_date, overall_score,
                           completeness_score, accuracy_score, consistency_score,
                           timeliness_score, uniqueness_score, total_rows,
                           null_count, duplicate_count, error_count, details
                    FROM metrics_sync_tables.data_quality_scores
                    WHERE score_date >= CURRENT_DATE - INTERVAL '%s days'
                """
                params = [str(days)]
                
                if source_name:
                    query += " AND source_name = %s"
                    params.append(source_name)
                
                if table_name:
                    query += " AND table_name = %s"
                    params.append(table_name)
                
                query += " ORDER BY score_date DESC, overall_score ASC"
                
                cur.execute(query, params)
                rows = cur.fetchall()
                cur.close()
                
                scores = []
                for row in rows:
                    scores.append({
                        'source_name': row[0],
                        'table_name': row[1],
                        'score_date': row[2].isoformat() if row[2] else None,
                        'overall_score': float(row[3]) if row[3] else 0.0,
                        'completeness_score': float(row[4]) if row[4] else 0.0,
                        'accuracy_score': float(row[5]) if row[5] else 0.0,
                        'consistency_score': float(row[6]) if row[6] else 0.0,
                        'timeliness_score': float(row[7]) if row[7] else 0.0,
                        'uniqueness_score': float(row[8]) if row[8] else 0.0,
                        'total_rows': row[9] or 0,
                        'null_count': row[10] or 0,
                        'duplicate_count': row[11] or 0,
                        'error_count': row[12] or 0,
                        'details': json.loads(row[13]) if row[13] else {}
                    })
                
                return scores
            finally:
                return_pg_connection(conn)
        except Exception as e:
            logger.error(f"Error getting quality scores: {e}")
            return []


# Global quality scorer instance
quality_scorer = DataQualityScorer()

