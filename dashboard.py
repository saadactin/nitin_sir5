import datetime
from db_utils import get_pg_connection, return_pg_connection

def log_sync(server_name: str, status: str, details: str = None):
    """
    Log sync attempt into Postgres (metrics_sync_tables.sync_history).
    Uses connection pooling for better performance.
    """
    conn = get_pg_connection()
    try:
        cur = conn.cursor()
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
    Includes all sync types: SQL Server, HANA, API (REST/SSE), etc.
    Uses connection pooling for better performance.
    """
    conn = get_pg_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT server_name, sync_time, status, details
            FROM metrics_sync_tables.sync_history
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
    Uses connection pooling for better performance.
    """
    conn = get_pg_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT server_name, sync_time, status, details
            FROM metrics_sync_tables.sync_history
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
