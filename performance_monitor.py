"""
Performance Monitoring Module - Phase 2
Tracks and optimizes sync performance
"""

import logging
import time
import psutil
import os
from datetime import datetime
from typing import Dict, Optional, Callable, List
from functools import wraps
from contextlib import contextmanager
from db_utils import get_pg_connection, return_pg_connection

logger = logging.getLogger(__name__)


class PerformanceMonitor:
    """Monitors and tracks performance metrics"""
    
    def __init__(self):
        self.active_operations = {}
    
    @contextmanager
    def track_operation(
        self,
        source_name: str,
        table_name: str,
        operation_type: str,
        sync_id: str = None
    ):
        """Context manager for tracking operation performance"""
        operation_id = f"{source_name}_{table_name}_{operation_type}_{int(time.time())}"
        
        # Record start metrics
        start_time = time.time()
        start_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024  # MB
        start_cpu = psutil.cpu_percent(interval=None)
        
        rows_processed = 0
        query_time = 0
        insert_time = 0
        
        try:
            yield {
                'operation_id': operation_id,
                'set_rows': lambda x: setattr(self, '_rows', x),
                'set_query_time': lambda x: setattr(self, '_query_time', x),
                'set_insert_time': lambda x: setattr(self, '_insert_time', x)
            }
            
            # Record end metrics
            end_time = time.time()
            end_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024  # MB
            end_cpu = psutil.cpu_percent(interval=None)
            
            duration_ms = int((end_time - start_time) * 1000)
            memory_usage = end_memory - start_memory
            cpu_usage = end_cpu
            
            rows_processed = getattr(self, '_rows', 0)
            query_time = getattr(self, '_query_time', 0)
            insert_time = getattr(self, '_insert_time', 0)
            
            rows_per_second = (rows_processed / (duration_ms / 1000)) if duration_ms > 0 else 0
            
            # Save metrics
            self.save_metrics(
                source_name=source_name,
                table_name=table_name,
                operation_type=operation_type,
                duration_ms=duration_ms,
                rows_processed=rows_processed,
                rows_per_second=rows_per_second,
                memory_usage_mb=memory_usage,
                cpu_usage_percent=cpu_usage,
                query_time_ms=query_time,
                insert_time_ms=insert_time,
                sync_id=sync_id
            )
            
        except Exception as e:
            # Still record metrics even on error
            end_time = time.time()
            duration_ms = int((end_time - start_time) * 1000)
            
            self.save_metrics(
                source_name=source_name,
                table_name=table_name,
                operation_type=operation_type,
                duration_ms=duration_ms,
                rows_processed=0,
                rows_per_second=0,
                memory_usage_mb=0,
                cpu_usage_percent=0,
                query_time_ms=0,
                insert_time_ms=0,
                sync_id=sync_id
            )
            raise
    
    def save_metrics(
        self,
        source_name: str,
        table_name: str,
        operation_type: str,
        duration_ms: int,
        rows_processed: int,
        rows_per_second: float,
        memory_usage_mb: float,
        cpu_usage_percent: float,
        query_time_ms: int = 0,
        insert_time_ms: int = 0,
        sync_id: str = None
    ):
        """Save performance metrics to database"""
        try:
            conn = get_pg_connection()
            try:
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO metrics_sync_tables.performance_metrics
                    (source_name, table_name, operation_type, duration_ms, rows_processed,
                     rows_per_second, memory_usage_mb, cpu_usage_percent, query_time_ms,
                     insert_time_ms, sync_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    source_name,
                    table_name,
                    operation_type,
                    duration_ms,
                    rows_processed,
                    rows_per_second,
                    memory_usage_mb,
                    cpu_usage_percent,
                    query_time_ms,
                    insert_time_ms,
                    sync_id
                ))
                conn.commit()
                cur.close()
            finally:
                return_pg_connection(conn)
        except Exception as e:
            logger.warning(f"Error saving performance metrics: {e}")
    
    def get_performance_stats(
        self,
        source_name: str = None,
        table_name: str = None,
        days: int = 7
    ) -> Dict:
        """Get performance statistics"""
        try:
            conn = get_pg_connection()
            try:
                cur = conn.cursor()
                query = """
                    SELECT 
                        AVG(duration_ms) as avg_duration,
                        AVG(rows_per_second) as avg_throughput,
                        AVG(memory_usage_mb) as avg_memory,
                        AVG(cpu_usage_percent) as avg_cpu,
                        SUM(rows_processed) as total_rows,
                        COUNT(*) as operation_count
                    FROM metrics_sync_tables.performance_metrics
                    WHERE recorded_at >= NOW() - INTERVAL '%s days'
                """
                params = [str(days)]
                
                if source_name:
                    query += " AND source_name = %s"
                    params.append(source_name)
                
                if table_name:
                    query += " AND table_name = %s"
                    params.append(table_name)
                
                cur.execute(query, params)
                row = cur.fetchone()
                cur.close()
                
                if row and row[0]:
                    return {
                        'avg_duration_ms': float(row[0]) if row[0] else 0,
                        'avg_throughput_rows_per_sec': float(row[1]) if row[1] else 0,
                        'avg_memory_mb': float(row[2]) if row[2] else 0,
                        'avg_cpu_percent': float(row[3]) if row[3] else 0,
                        'total_rows_processed': int(row[4]) if row[4] else 0,
                        'operation_count': int(row[5]) if row[5] else 0
                    }
                else:
                    return {
                        'avg_duration_ms': 0,
                        'avg_throughput_rows_per_sec': 0,
                        'avg_memory_mb': 0,
                        'avg_cpu_percent': 0,
                        'total_rows_processed': 0,
                        'operation_count': 0
                    }
            finally:
                return_pg_connection(conn)
        except Exception as e:
            logger.error(f"Error getting performance stats: {e}")
            return {}
    
    def get_slow_operations(self, threshold_ms: int = 5000, limit: int = 20) -> List[Dict]:
        """Get slow operations above threshold"""
        try:
            conn = get_pg_connection()
            try:
                cur = conn.cursor()
                cur.execute("""
                    SELECT source_name, table_name, operation_type, duration_ms,
                           rows_processed, rows_per_second, recorded_at
                    FROM metrics_sync_tables.performance_metrics
                    WHERE duration_ms >= %s
                    ORDER BY duration_ms DESC
                    LIMIT %s
                """, (threshold_ms, limit))
                
                rows = cur.fetchall()
                cur.close()
                
                slow_ops = []
                for row in rows:
                    slow_ops.append({
                        'source_name': row[0],
                        'table_name': row[1],
                        'operation_type': row[2],
                        'duration_ms': row[3],
                        'rows_processed': row[4],
                        'rows_per_second': row[5],
                        'recorded_at': row[6].isoformat() if row[6] else None
                    })
                
                return slow_ops
            finally:
                return_pg_connection(conn)
        except Exception as e:
            logger.error(f"Error getting slow operations: {e}")
            return []


# Global performance monitor instance
performance_monitor = PerformanceMonitor()

