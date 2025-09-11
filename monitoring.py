"""
monitoring.py

Comprehensive monitoring and validation module for the SQL Server → Postgres hybrid sync app.
This module integrates non-invasively by reading existing tracking tables and logs without
changing sync behavior.

Features:
- Session and per-table metrics (derived from tracking tables and logs)
- Alert aggregation with severity levels
- Validation utilities: row counts, row-hash deltas, duplicate detection
- Debug utilities: find unsynced rows
- Smart sync hints for tables without primary keys
- Enhanced type inference helper (advisory)
- Connection error categorization (advisory) and timeout-friendly retry wrappers
- Structured session reports (HTML-rendered by templates; JSON returned by endpoints)

Note: This module does not persist new state in DB; it computes reports by reading
existing Postgres tracking tables and by scanning hybrid_sync.log.
"""

from __future__ import annotations

import os
import json
import time
import logging
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import text

from manage_server import load_config
from hybrid_sync import (
    get_pg_engine,
    get_sqlalchemy_engine,
    get_sql_connection,
    get_table_row_count,
)

logger = logging.getLogger(__name__)

# ----------------------------- Data classes -----------------------------

@dataclass
class TableMetric:
    schema: str
    table: str
    rows_source: int
    rows_destination: int
    duration_ms: Optional[int] = None
    errors: Optional[List[str]] = None

@dataclass
class SessionSummary:
    session_id: str
    server: str
    database: str
    started_at: str
    finished_at: Optional[str]
    duration_ms: Optional[int]
    total_tables: int
    total_rows_source: int
    total_rows_destination: int
    warnings: List[str]
    errors: List[str]

@dataclass
class Alert:
    severity: str  # INFO, WARNING, HIGH, CRITICAL
    message: str
    server: Optional[str] = None
    database: Optional[str] = None
    table: Optional[str] = None
    when: Optional[str] = None
    category: Optional[str] = None  # connection, schema_mismatch, data_conflict, etc.

# ----------------------------- Helper: severity -----------------------------

def _severity_for_status(status: Optional[str]) -> str:
    if not status or status.upper() == "COMPLETED":
        return "INFO"
    status_upper = status.upper()
    if "FAILED" in status_upper:
        return "CRITICAL"
    if "WARNING" in status_upper:
        return "WARNING"
    return "HIGH"

# ----------------------------- Session metrics -----------------------------

def get_recent_sessions(server: str, db: str, limit: int = 10) -> List[SessionSummary]:
    """Build recent session summaries from sync_database_status entries.
    We treat each updated_at row as a logical session with coarse stats.
    """
    engine = get_pg_engine()
    q = """
    SELECT last_full_sync, last_incremental_sync, sync_status, updated_at
    FROM sync_database_status
    WHERE server_name = :s AND database_name = :d
    ORDER BY updated_at DESC
    LIMIT :limit
    """
    rows = []
    with engine.connect() as conn:
        rows = conn.execute(text(q), {"s": server, "d": db, "limit": limit}).fetchall()

    sessions: List[SessionSummary] = []
    # Derive table counts/rows from table status
    tables = get_table_snapshots(server, db)
    total_tables = len(tables)
    total_src = sum(t["rows_source"] for t in tables)
    total_dst = sum(t["rows_destination"] for t in tables)

    for r in rows:
        started = r[0] or r[1] or r[3]
        finished = r[3]
        duration_ms = None
        try:
            if started and finished:
                duration_ms = int((finished - started).total_seconds() * 1000)
        except Exception:
            duration_ms = None
        status = r[2]
        sev = _severity_for_status(status)
        warn = [] if sev in ("INFO",) else [f"Database status: {status}"]
        sessions.append(SessionSummary(
            session_id=f"{server}:{db}:{int(time.time()*1000)}",
            server=server,
            database=db,
            started_at=started.isoformat() if started else None,
            finished_at=finished.isoformat() if finished else None,
            duration_ms=duration_ms,
            total_tables=total_tables,
            total_rows_source=total_src,
            total_rows_destination=total_dst,
            warnings=warn,
            errors=[] if sev != "CRITICAL" else [f"Status {status}"],
        ))
    return sessions


def get_table_snapshots(server: str, db: str) -> List[Dict]:
    """Return per-table snapshot: rows_source/destination using existing helpers."""
    engine = get_pg_engine()
    q = """
    SELECT schema_name, table_name, last_pk_value, updated_at
    FROM sync_table_status
    WHERE server_name = :s AND database_name = :d
    ORDER BY schema_name, table_name
    """
    with engine.connect() as conn:
        trows = conn.execute(text(q), {"s": server, "d": db}).fetchall()

    config = load_config()
    conf = config["sqlservers"].get(server)
    results: List[Dict] = []
    if not conf:
        return results

    for tr in trows:
        schema, table = tr[0], tr[1]
        # counts
        sql_conn = get_sql_connection(conf, db)
        src_count = get_table_row_count(sql_conn, schema, table)
        sql_conn.close()

        pg = get_pg_engine()
        server_clean = ''.join(c for c in conf['server'] if c.isalnum() or c in '_-')
        pg_schema = f"{server_clean}_{db}".replace('-', '_').replace(' ', '_')
        pg_table = f"{schema}_{table}"
        try:
            with pg.connect() as c:
                dst_count = c.execute(text(f'SELECT COUNT(*) FROM "{pg_schema}"."{pg_table}"')).fetchone()[0]
        except Exception:
            dst_count = 0
        results.append({
            "schema": schema,
            "table": table,
            "rows_source": src_count,
            "rows_destination": dst_count,
            "last_pk": tr[2],
            "updated_at": tr[3].isoformat() if tr[3] else None,
        })
    return results

# ----------------------------- Alerts -----------------------------

def collect_alerts_with_severity(server: Optional[str] = None, db: Optional[str] = None) -> Dict:
    """Aggregate alerts with severity levels.

    Sources:
    - sync_database_status: non-COMPLETED statuses, stale syncs
    - Per-table consistency differences
    - Server connectivity (config check)
    - schedules table: failed jobs
    - hybrid_sync.log: error lines
    """
    engine = get_pg_engine()
    where = []
    params = {}
    if server:
        where.append("server_name = :s")
        params["s"] = server
    if db:
        where.append("database_name = :d")
        params["d"] = db

    q = """
    SELECT server_name, database_name, last_full_sync, last_incremental_sync, sync_status, updated_at
    FROM sync_database_status
    {where}
    ORDER BY updated_at DESC
    """.format(where=("WHERE "+" AND ".join(where)) if where else "")
    rows = []
    with engine.connect() as conn:
        rows = conn.execute(text(q), params).fetchall()

    alerts: List[Alert] = []
    for r in rows:
        sev = _severity_for_status(r[4])
        if sev != "INFO":
            alerts.append(Alert(severity=sev, message="Database sync status not completed", server=r[0], database=r[1], when=r[5].isoformat() if r[5] else None, category="status"))

    # Consistency-derived warnings
    if server and db:
        snap = get_table_snapshots(server, db)
        for t in snap:
            if t["rows_destination"] == 0 and t["rows_source"] > 0:
                alerts.append(Alert(severity="HIGH", message="Destination empty but source has rows", server=server, database=db, table=f"{t['schema']}.{t['table']}", category="consistency"))
            elif t["rows_destination"] != t["rows_source"]:
                alerts.append(Alert(severity="WARNING", message="Row counts differ", server=server, database=db, table=f"{t['schema']}.{t['table']}", category="consistency"))

    # Stale sync warnings (older than 24h)
    try:
        stale_q = """
        SELECT server_name, database_name, COALESCE(last_incremental_sync, last_full_sync) AS last_sync
        FROM sync_database_status
        WHERE COALESCE(last_incremental_sync, last_full_sync) IS NOT NULL
        """
        with engine.connect() as conn:
            for r in conn.execute(text(stale_q)).fetchall():
                last_sync = r[2]
                try:
                    age_hours = (pd.Timestamp.utcnow().to_pydatetime() - last_sync).total_seconds() / 3600.0
                    if age_hours > 24 and (not server or server == r[0]) and (not db or db == r[1]):
                        alerts.append(Alert(severity="WARNING", message=f"Stale sync ({int(age_hours)}h)", server=r[0], database=r[1], category="stale"))
                except Exception:
                    pass
    except Exception:
        pass

    # Server connectivity checks
    try:
        cfg = load_config()
        for s_name, s_conf in cfg.get("sqlservers", {}).items():
            if server and s_name != server:
                continue
            try:
                conn = get_sql_connection(s_conf)
                conn.close()
            except Exception as e:
                alerts.append(Alert(severity="CRITICAL", message=f"Server unreachable: {e}", server=s_name, category="connection"))
    except Exception:
        pass

    # schedules table failures
    try:
        with engine.raw_connection() as pg_raw:
            cur = pg_raw.cursor()
            cur.execute("""
                SELECT server_name, job_type, status, COALESCE(error,'')
                FROM metrics_sync_tables.schedules
                WHERE status = 'failed'
            """)
            for r in cur.fetchall():
                if server and r[0] != server:
                    continue
                alerts.append(Alert(severity="HIGH", message=f"Schedule failed ({r[1]}): {r[3]}", server=r[0], category="schedule"))
    except Exception:
        pass

    # Parse errors from log
    try:
        log_path = os.getenv("HYBRID_SYNC_LOG", "hybrid_sync.log")
        if os.path.exists(log_path):
            with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f.readlines()[-200:]:
                    if "Error processing" in line or "Failed to sync/load" in line:
                        alerts.append(Alert(severity="CRITICAL", message=line.strip(), category="log"))
    except Exception:
        pass

    alerts_list = [asdict(a) for a in alerts]
    return {
        "alerts": alerts_list,
        "counts": {
            "total": len(alerts_list),
            "critical": sum(1 for a in alerts_list if a["severity"] == "CRITICAL"),
            "high": sum(1 for a in alerts_list if a["severity"] == "HIGH"),
            "warning": sum(1 for a in alerts_list if a["severity"] == "WARNING"),
        }
    }

# ----------------------------- Validation -----------------------------

def validate_table(server: str, db: str, table_fq: str) -> Dict:
    """Validate counts, detect duplicates by row-hash, and report deltas."""
    if "." in table_fq:
        schema, table = table_fq.split(".", 1)
    else:
        schema, table = "dbo", table_fq

    config = load_config()
    conf = config["sqlservers"][server]

    sql_engine = get_sqlalchemy_engine(conf, db)
    src_df = pd.read_sql(f"SELECT * FROM [{schema}].[{table}]", sql_engine)

    pg = get_pg_engine()
    server_clean = ''.join(c for c in conf['server'] if c.isalnum() or c in '_-')
    pg_schema = f"{server_clean}_{db}".replace('-', '_').replace(' ', '_')
    pg_table = f"{schema}_{table}"
    try:
        dst_df = pd.read_sql(f'SELECT * FROM "{pg_schema}"."{pg_table}"', pg)
    except Exception:
        dst_df = pd.DataFrame(columns=src_df.columns)

    def row_hash_df(df: pd.DataFrame) -> pd.Series:
        if df.empty:
            return pd.Series([], dtype="int64")
        return df.fillna("") .apply(lambda r: hash(tuple(r)), axis=1)

    src_hash = row_hash_df(src_df)
    dst_hash = row_hash_df(dst_df)

    # duplicates by hash in source/target
    src_dup = src_hash[src_hash.duplicated()].shape[0]
    dst_dup = dst_hash[dst_hash.duplicated()].shape[0]

    # delta
    missing = set(src_hash) - set(dst_hash)
    extra = set(dst_hash) - set(src_hash)

    return {
        "rows_source": len(src_df),
        "rows_destination": len(dst_df),
        "duplicate_source_rows": int(src_dup),
        "duplicate_destination_rows": int(dst_dup),
        "delta_missing": len(missing),
        "delta_extra": len(extra),
    }

# ----------------------------- Debug -----------------------------

def debug_find_new_rows(server: str, db: str, table_fq: str, limit: int = 100) -> Dict:
    """Return sample rows present in source but missing in destination by row-hash."""
    if "." in table_fq:
        schema, table = table_fq.split(".", 1)
    else:
        schema, table = "dbo", table_fq

    config = load_config()
    conf = config["sqlservers"][server]

    sql_engine = get_sqlalchemy_engine(conf, db)
    src_df = pd.read_sql(f"SELECT * FROM [{schema}].[{table}]", sql_engine)

    pg = get_pg_engine()
    server_clean = ''.join(c for c in conf['server'] if c.isalnum() or c in '_-')
    pg_schema = f"{server_clean}_{db}".replace('-', '_').replace(' ', '_')
    pg_table = f"{schema}_{table}"
    try:
        dst_df = pd.read_sql(f'SELECT * FROM "{pg_schema}"."{pg_table}"', pg)
    except Exception:
        dst_df = pd.DataFrame(columns=src_df.columns)

    if src_df.empty:
        return {"rows": [], "count": 0}

    src = src_df.fillna("")
    dst = dst_df.fillna("")
    src["row_hash"] = src.apply(lambda r: hash(tuple(r)), axis=1)
    dst["row_hash"] = dst.apply(lambda r: hash(tuple(r)), axis=1)

    missing = src[~src["row_hash"].isin(dst["row_hash"])]
    sample = missing.drop(columns=["row_hash"]) .head(limit)
    return {"rows": sample.to_dict(orient="records"), "count": int(missing.shape[0])}

# ----------------------------- Smart sync (advisory) -----------------------------

def smart_sync_without_pk_plan(server: str, db: str, table_fq: str) -> Dict:
    """Provide a recommended plan for tables without PK using row-hash + surrogate key."""
    return {
        "strategy": "row_hash + surrogate key",
        "notes": [
            "Create row_hash column as TEXT and a surrogate serial key.",
            "Insert new rows where row_hash not present; avoid deletes for safety.",
            "Use incremental batches and maintain last_max(hash) if ordered column exists.",
        ],
    }

# ----------------------------- Type inference (advisory) -----------------------------

def enhanced_type_inference(df: pd.DataFrame) -> Dict[str, str]:
    """Suggest Postgres types for columns with a slightly richer mapping."""
    types = {}
    for c in df.columns:
        s = df[c]
        if pd.api.types.is_integer_dtype(s):
            types[c] = "BIGINT"
        elif pd.api.types.is_float_dtype(s):
            types[c] = "DOUBLE PRECISION"
        elif pd.api.types.is_bool_dtype(s):
            types[c] = "BOOLEAN"
        elif pd.api.types.is_datetime64_any_dtype(s):
            types[c] = "TIMESTAMP"
        elif s.astype(str).str.len().max() <= 255:
            types[c] = "VARCHAR(255)"
        else:
            types[c] = "TEXT"
    return types

# ----------------------------- Connection retry (advisory) -----------------------------

def categorize_error(e: Exception) -> str:
    """Categorize an error message into high-level categories."""
    msg = str(e).lower()
    if "timeout" in msg or "timed out" in msg:
        return "connection_timeout"
    if "could not connect" in msg or "connection refused" in msg or "login timeout" in msg:
        return "connection"
    if "schema" in msg or "column" in msg or "type" in msg:
        return "schema_mismatch"
    if "duplicate key" in msg or "conflict" in msg:
        return "data_conflict"
    return "unknown"

# ----------------------------- Session report -----------------------------

def build_session_report(server: str, db: str) -> Dict:
    """Build a structured session report for the latest session based on tracking tables."""
    sessions = get_recent_sessions(server, db, limit=1)
    session = sessions[0] if sessions else None
    tables = get_table_snapshots(server, db)
    return {
        "session": asdict(session) if session else None,
        "tables": tables,
        "alerts": collect_alerts_with_severity(server, db),
    }
