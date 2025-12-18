import datetime
from db_utils import get_pg_connection, return_pg_connection

def log_sync(server_name: str, status: str, details: str = None):
    """
    Log sync attempt into Postgres (metrics_sync_tables.sync_history).
    If there's an existing 'in-progress' entry for this server within the last hour,
    update it instead of creating a new entry.
    Uses connection pooling for better performance.
    """
    conn = get_pg_connection()
    try:
        cur = conn.cursor()
        
        # Check if there's a recent in-progress entry for this server
        if status in ["success", "failed", "error"]:
            cur.execute("""
                UPDATE metrics_sync_tables.sync_history
                SET status = %s, details = %s, sync_time = NOW()
                WHERE server_name = %s 
                AND status = 'in-progress'
                AND sync_time >= NOW() - INTERVAL '2 hours'
                ORDER BY sync_time DESC
                LIMIT 1
            """, (status, details or "-", server_name))
            
            if cur.rowcount > 0:
                # Updated existing entry
                conn.commit()
                cur.close()
                return
        
        # Otherwise, insert new entry
        cur.execute("""
            INSERT INTO metrics_sync_tables.sync_history (server_name, sync_time, status, details)
            VALUES (%s, NOW(), %s, %s)
        """, (server_name, status, details or "-"))
        conn.commit()
        cur.close()
    finally:
        return_pg_connection(conn)


def get_last_10_syncs():
    """
    Return the last 10 sync attempts (newest first).
    Excludes 'started' status - only shows 'success', 'failed', 'error', 'in-progress', 'partial'.
    Includes all sync types: SQL Server, HANA, API (REST/SSE), etc.
    Uses connection pooling for better performance.
    """
    conn = get_pg_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT server_name, sync_time, status, details
            FROM metrics_sync_tables.sync_history
            WHERE status != 'started'
            ORDER BY sync_time DESC
            LIMIT 10
        """)
        rows = cur.fetchall()
        cur.close()
        
        return [
            {
                "server": r[0],
                "time": r[1].strftime("%Y-%m-%d %H:%M:%S"),
                "status": r[2],
                "details": r[3]
            }
            for r in rows
        ]
    finally:
        return_pg_connection(conn)


def get_last_sync_details():
    """
    Return details of the most recent sync (if any).
    Excludes 'started' status - only shows 'success', 'failed', 'error', 'in-progress', 'partial'.
    Uses connection pooling for better performance.
    """
    conn = get_pg_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT server_name, sync_time, status, details
            FROM metrics_sync_tables.sync_history
            WHERE status != 'started'
            ORDER BY sync_time DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        cur.close()

        if not row:
            return None

        return {
            "server": row[0],
            "time": row[1].strftime("%Y-%m-%d %H:%M:%S"),
            "status": row[2],
            "details": row[3]
        }
    finally:
        return_pg_connection(conn)


def get_last_sync_for_server(server_name: str):
    """
    Return the most recent sync entry for a specific server, or None.
    Uses connection pooling for better performance.
    """
    conn = get_pg_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT server_name, sync_time, status, details
            FROM metrics_sync_tables.sync_history
            WHERE server_name = %s
            ORDER BY sync_time DESC
            LIMIT 1
        """, (server_name,))
        row = cur.fetchone()
        cur.close()

        if not row:
            return None

        return {
            "server": row[0],
            "time": row[1].strftime("%Y-%m-%d %H:%M:%S"),
            "status": row[2],
            "details": row[3]
        }
    finally:
        return_pg_connection(conn)


def get_dashboard_metrics():
    """
    Get comprehensive dashboard metrics for charts and statistics.
    Excludes 'started' status from all calculations.
    Returns metrics including: total syncs, success rate, failed count, in-progress count,
    hourly trends, status distribution, and top sources.
    """
    conn = get_pg_connection()
    try:
        cur = conn.cursor()
        
        # Overall statistics (excluding 'started')
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE status = 'success') as success_count,
                COUNT(*) FILTER (WHERE status IN ('failed', 'error')) as failed_count,
                COUNT(*) FILTER (WHERE status IN ('in-progress', 'partial')) as in_progress_count
            FROM metrics_sync_tables.sync_history
            WHERE status != 'started'
            AND sync_time >= NOW() - INTERVAL '7 days'
        """)
        stats = cur.fetchone()
        total, success_count, failed_count, in_progress_count = stats or (0, 0, 0, 0)
        
        # Calculate success rate
        success_rate = round((success_count / total * 100) if total > 0 else 0, 1)
        
        # Hourly trends for last 24 hours
        cur.execute("""
            SELECT 
                DATE_TRUNC('hour', sync_time) as hour,
                COUNT(*) FILTER (WHERE status = 'success') as success,
                COUNT(*) FILTER (WHERE status IN ('failed', 'error')) as failed,
                COUNT(*) FILTER (WHERE status IN ('in-progress', 'partial')) as in_progress
            FROM metrics_sync_tables.sync_history
            WHERE status != 'started'
            AND sync_time >= NOW() - INTERVAL '24 hours'
            GROUP BY hour
            ORDER BY hour
        """)
        hourly_trends = cur.fetchall()
        
        # Status distribution (last 7 days)
        cur.execute("""
            SELECT 
                CASE 
                    WHEN status = 'success' THEN 'Success'
                    WHEN status IN ('failed', 'error') THEN 'Failed'
                    WHEN status IN ('in-progress', 'partial') THEN 'In Progress'
                    ELSE 'Other'
                END as status_group,
                COUNT(*) as count
            FROM metrics_sync_tables.sync_history
            WHERE status != 'started'
            AND sync_time >= NOW() - INTERVAL '7 days'
            GROUP BY status_group
        """)
        status_dist = cur.fetchall()
        
        # Top sources by sync count (last 7 days)
        cur.execute("""
            SELECT server_name, COUNT(*) as sync_count
            FROM metrics_sync_tables.sync_history
            WHERE status != 'started'
            AND sync_time >= NOW() - INTERVAL '7 days'
            GROUP BY server_name
            ORDER BY sync_count DESC
            LIMIT 10
        """)
        top_sources = cur.fetchall()
        
        cur.close()
        
        return {
            'total_syncs': total,
            'success_count': success_count,
            'failed_count': failed_count,
            'in_progress_count': in_progress_count,
            'success_rate': success_rate,
            'hourly_trends': [
                {
                    'hour': row[0].strftime('%Y-%m-%d %H:00'),
                    'success': row[1],
                    'failed': row[2],
                    'in_progress': row[3]
                }
                for row in hourly_trends
            ],
            'status_distribution': {
                row[0]: row[1] for row in status_dist
            },
            'top_sources': [
                {'server': row[0], 'count': row[1]}
                for row in top_sources
            ]
        }
    finally:
        return_pg_connection(conn)
