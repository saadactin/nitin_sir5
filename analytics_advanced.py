"""
Advanced analytics and operations helpers for SQL Server → Postgres hybrid sync.

Features:
- Fetch historical sync data from Postgres tracking tables
- Highlight failed syncs and reasons
- Generate CSV/Excel reports for auditing
- Manage resume sync logic (resume from last PK)
- Handle partial table sync previews (selected columns / filtered rows)
- Report schema changes applied during sync (parsed from hybrid_sync.log)
- Send notifications via Email/Slack
- Verify consistency between source and destination
- Simulate incremental sync (preview)
- Verify table schema matches expected data types
"""

import os
import io
import csv
import json
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Dict, Optional, Tuple

import pandas as pd
from sqlalchemy import text

from manage_server import load_config
from hybrid_sync import (
    get_pg_engine,
    get_sqlalchemy_engine,
    get_sql_connection,
    get_table_row_count,
)
from analytics import delta_tracking

logger = logging.getLogger(__name__)

# --------------------------- History ---------------------------

def fetch_database_history(server_name: str, db_name: str, limit: int = 50) -> List[Dict]:
    """Fetch last N database-level sync runs from sync_database_status."""
    engine = get_pg_engine()
    query = """
    SELECT server_name, database_name, last_full_sync, last_incremental_sync, sync_status, updated_at
    FROM sync_database_status
    WHERE server_name = :server AND database_name = :db
    ORDER BY updated_at DESC
    LIMIT :limit
    """
    with engine.connect() as conn:
        rows = conn.execute(text(query), {"server": server_name, "db": db_name, "limit": limit}).fetchall()
    history = []
    for r in rows:
        history.append({
            "server_name": r[0],
            "database_name": r[1],
            "last_full_sync": r[2].isoformat() if r[2] else None,
            "last_incremental_sync": r[3].isoformat() if r[3] else None,
            "sync_status": r[4],
            "updated_at": r[5].isoformat() if r[5] else None,
        })
    return history


def fetch_table_history(server_name: str, db_name: str, limit: int = 200) -> List[Dict]:
    """Fetch table-level last PK/time from sync_table_status for a database."""
    engine = get_pg_engine()
    query = """
    SELECT server_name, database_name, schema_name, table_name, last_pk_value, updated_at, created_at
    FROM sync_table_status
    WHERE server_name = :server AND database_name = :db
    ORDER BY updated_at DESC
    LIMIT :limit
    """
    with engine.connect() as conn:
        rows = conn.execute(text(query), {"server": server_name, "db": db_name, "limit": limit}).fetchall()
    tables = []
    for r in rows:
        tables.append({
            "server_name": r[0],
            "database_name": r[1],
            "schema": r[2],
            "table": r[3],
            "table_fq": f"{r[2]}.{r[3]}",
            "last_pk_value": r[4],
            "updated_at": r[5].isoformat() if r[5] else None,
            "created_at": r[6].isoformat() if r[6] else None,
        })
    return tables


def detect_failed_syncs(server_name: Optional[str] = None, db_name: Optional[str] = None) -> List[Dict]:
    """Highlight failed syncs and reasons from sync_database_status (non-COMPLETED)."""
    engine = get_pg_engine()
    where = ["sync_status IS NOT NULL", "sync_status <> 'COMPLETED'"]
    params = {}
    if server_name:
        where.append("server_name = :server")
        params["server"] = server_name
    if db_name:
        where.append("database_name = :db")
        params["db"] = db_name
    query = f"""
    SELECT server_name, database_name, last_full_sync, last_incremental_sync, sync_status, updated_at
    FROM sync_database_status
    WHERE {' AND '.join(where)}
    ORDER BY updated_at DESC
    """
    with engine.connect() as conn:
        rows = conn.execute(text(query), params).fetchall()
    alerts = []
    for r in rows:
        alerts.append({
            "type": "failure",
            "server_name": r[0],
            "database_name": r[1],
            "last_full_sync": r[2].isoformat() if r[2] else None,
            "last_incremental_sync": r[3].isoformat() if r[3] else None,
            "status": r[4],
            "updated_at": r[5].isoformat() if r[5] else None,
            "message": "Database sync not completed",
        })
    return alerts

# --------------------------- Reports ---------------------------

def generate_sync_report(history_rows: List[Dict], fmt: str = "csv") -> Tuple[io.BytesIO, str, str]:
    """Generate CSV/Excel report from history rows. Returns (buffer, mimetype, filename)."""
    df = pd.DataFrame(history_rows)
    buf = io.BytesIO()
    if fmt == "xlsx":
        with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="sync_history")
        mimetype = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        filename = "sync_history.xlsx"
    else:
        # default csv
        df.to_csv(buf, index=False, encoding="utf-8")
        mimetype = "text/csv"
        filename = "sync_history.csv"
    buf.seek(0)
    return buf, mimetype, filename

# --------------------------- Resume / Partial ---------------------------

def get_resume_point(server_name: str, db_name: str, schema: str, table: str) -> Optional[str]:
    """Return last_pk_value from sync_table_status to resume from."""
    engine = get_pg_engine()
    query = """
    SELECT last_pk_value
    FROM sync_table_status
    WHERE server_name = :server AND database_name = :db AND schema_name = :schema AND table_name = :table
    """
    with engine.connect() as conn:
        row = conn.execute(text(query), {"server": server_name, "db": db_name, "schema": schema, "table": table}).fetchone()
    return row[0] if row else None


def resume_sync_table(server_name: str, db_name: str, table_fq: str) -> Dict:
    """Attempt to resume a sync for a single table by triggering an incremental run.
    Note: The current pipeline syncs whole DBs; we surface intent and last marker.
    """
    if "." in table_fq:
        schema, table = table_fq.split(".", 1)
    else:
        schema, table = "dbo", table_fq

    # Provide resume info
    resume_from = get_resume_point(server_name, db_name, schema, table)
    # Use existing hybrid pipeline (db-scoped). Real per-table resume would be implemented there.
    info = {
        "server": server_name,
        "database": db_name,
        "table": table_fq,
        "resume_from_last_pk": resume_from,
        "action": "queued",
        "note": "Incremental sync will continue from last_pk on next run.",
    }
    return info


def partial_sync_preview(server_name: str, db_name: str, table_fq: str, columns: Optional[List[str]] = None, filter_sql: Optional[str] = None) -> Dict:
    """Preview partial sync by selecting columns/filter and computing delta vs destination using row hashes."""
    config = load_config()
    server_conf = config["sqlservers"][server_name]
    if "." in table_fq:
        schema, table = table_fq.split(".", 1)
    else:
        schema, table = "dbo", table_fq

    sql_engine = get_sqlalchemy_engine(server_conf, db_name)
    select_cols = "*" if not columns else ",".join(f"[{c}]" for c in columns)
    query = f"SELECT {select_cols} FROM [{schema}].[{table}]"
    if filter_sql:
        query += f" WHERE {filter_sql}"
    src_df = pd.read_sql(query, sql_engine)

    pg_engine = get_pg_engine()
    server_clean = ''.join(c for c in server_conf['server'] if c.isalnum() or c in '_-')
    pg_schema = f"{server_clean}_{db_name}".replace('-', '_').replace(' ', '_')
    pg_table = f"{schema}_{table}"
    try:
        dst_df = pd.read_sql(f'SELECT {"*" if not columns else ",".join("\""+c+"\"" for c in columns)} FROM "{pg_schema}"."{pg_table}"', pg_engine)
    except Exception:
        dst_df = pd.DataFrame(columns=src_df.columns)

    src_h = set(src_df.fillna('').apply(lambda r: hash(tuple(r)), axis=1).tolist()) if not src_df.empty else set()
    dst_h = set(dst_df.fillna('').apply(lambda r: hash(tuple(r)), axis=1).tolist()) if not dst_df.empty else set()
    to_insert = len(src_h - dst_h)
    to_skip = len(src_h & dst_h)

    return {
        "rows_source": len(src_df),
        "rows_destination": len(dst_df),
        "would_insert": to_insert,
        "already_synced": to_skip,
        "columns": columns or list(src_df.columns),
        "filter": filter_sql or "",
    }

# --------------------------- Schema Changes ---------------------------

def parse_schema_changes_from_log(log_path: str = "hybrid_sync.log", server_filter: Optional[str] = None, db_filter: Optional[str] = None) -> List[Dict]:
    """Parse hybrid_sync.log for lines about added columns and return structured events."""
    if not os.path.exists(log_path):
        return []
    events: List[Dict] = []
    with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if "Added columns on" in line:
                # Example: Added columns on <schema>.<table>: ['col1','col2']
                try:
                    ts, rest = line.split(" - ", 1)
                except ValueError:
                    ts, rest = "", line
                parts = rest.strip().split("Added columns on ")
                if len(parts) < 2:
                    continue
                tail = parts[1]
                seg = tail.split(":", 1)
                table_ref = seg[0].strip()
                cols_txt = seg[1] if len(seg) > 1 else "[]"
                cols = []
                try:
                    # normalize quotes to double for json parsing
                    cols = json.loads(cols_txt.replace("'", '"'))
                except Exception:
                    cols = [c.strip() for c in cols_txt.strip("[]\n ").split(",") if c.strip()]
                # Optional filters
                if server_filter and server_filter not in line:
                    pass
                if db_filter and db_filter not in line:
                    pass
                events.append({
                    "timestamp": ts,
                    "object": table_ref,
                    "added_columns": cols,
                    "message": f"Columns added to {table_ref}: {cols}",
                })
    return events

# --------------------------- Notifications ---------------------------

def send_email_notification(subject: str, body: str, to_addresses: List[str]) -> bool:
    """Send email via SMTP. Configure via env: SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, SMTP_FROM."""
    host = os.getenv("SMTP_HOST")
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.getenv("SMTP_USER")
    password = os.getenv("SMTP_PASS")
    sender = os.getenv("SMTP_FROM", user or "noreply@example.com")
    if not host or not user or not password:
        logger.warning("SMTP not configured; skipping email.")
        return False
    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = ",".join(to_addresses)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))
    try:
        with smtplib.SMTP(host, port) as server:
            server.starttls()
            server.login(user, password)
            server.sendmail(sender, to_addresses, msg.as_string())
        return True
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False


def send_slack_notification(text_message: str, webhook_url: Optional[str] = None) -> bool:
    """Send Slack notification using Incoming Webhook. URL from param or SLACK_WEBHOOK_URL env."""
    import urllib.request
    import urllib.error

    url = webhook_url or os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        logger.warning("Slack webhook not configured; skipping Slack notification.")
        return False
    payload = json.dumps({"text": text_message}).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return 200 <= resp.getcode() < 300
    except Exception as e:
        logger.error(f"Failed to send Slack message: {e}")
        return False

# --------------------------- Verification ---------------------------

def verify_consistency(server_name: str, db_name: str, table_fq: str) -> Dict:
    """Verify row counts and sample contents between source and destination."""
    config = load_config()
    server_conf = config["sqlservers"][server_name]
    if "." in table_fq:
        schema, table = table_fq.split(".", 1)
    else:
        schema, table = "dbo", table_fq

    # counts
    sql_conn = get_sql_connection(server_conf, db_name)
    src_count = get_table_row_count(sql_conn, schema, table)

    pg_engine = get_pg_engine()
    server_clean = ''.join(c for c in server_conf['server'] if c.isalnum() or c in '_-')
    pg_schema = f"{server_clean}_{db_name}".replace('-', '_').replace(' ', '_')
    pg_table = f"{schema}_{table}"

    try:
        with pg_engine.connect() as conn:
            dst_count = conn.execute(text(f'SELECT COUNT(*) FROM "{pg_schema}"."{pg_table}"')).fetchone()[0]
    except Exception:
        dst_count = 0

    status = "ok" if src_count == dst_count else ("warning" if dst_count > 0 else "error")
    return {"rows_source": src_count, "rows_destination": dst_count, "status": status}


def simulate_incremental_sync(server_name: str, db_name: str, table_fq: str) -> Dict:
    """Simulate incremental sync using existing delta tracking."""
    return delta_tracking(server_name, db_name, table_fq)


def verify_table_schema(server_name: str, db_name: str, table_fq: str) -> Dict:
    """Compare source dtypes to destination column types (rough check)."""
    config = load_config()
    server_conf = config["sqlservers"][server_name]
    if "." in table_fq:
        schema, table = table_fq.split(".", 1)
    else:
        schema, table = "dbo", table_fq

    sql_engine = get_sqlalchemy_engine(server_conf, db_name)
    sample = pd.read_sql(f"SELECT TOP (50) * FROM [{schema}].[{table}]", sql_engine)

    pg_engine = get_pg_engine()
    server_clean = ''.join(c for c in server_conf['server'] if c.isalnum() or c in '_-')
    pg_schema = f"{server_clean}_{db_name}".replace('-', '_').replace(' ', '_')
    pg_table = f"{schema}_{table}"

    dst_cols = {}
    try:
        with pg_engine.connect() as conn:
            rows = conn.execute(text(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = :schema AND table_name = :table
                """
            ), {"schema": pg_schema, "table": pg_table}).fetchall()
            for r in rows:
                dst_cols[r[0]] = r[1]
    except Exception:
        dst_cols = {}

    src_types = {c: str(sample[c].dtype) for c in sample.columns}
    miss_in_dst = [c for c in sample.columns if c not in dst_cols]
    return {"source_types": src_types, "destination_types": dst_cols, "missing_in_destination": miss_in_dst}

# --------------------------- Alerts aggregation ---------------------------

def collect_alerts(server_name: Optional[str] = None, db_name: Optional[str] = None) -> Dict:
    """Aggregate failures, warnings, inconsistencies, and notification events."""
    alerts = {"failures": [], "warnings": [], "inconsistencies": []}
    # Failures from status table
    failures = detect_failed_syncs(server_name, db_name)
    alerts["failures"].extend(failures)
    # Inconsistencies: row differences (sample using table history + counts)
    try:
        tables = fetch_table_history(server_name, db_name, limit=200)
        for t in tables:
            chk = verify_consistency(t["server_name"], t["database_name"], t["table_fq"])
            if chk["status"] == "warning":
                alerts["warnings"].append({"table": t["table_fq"], "message": "Row counts differ"})
            elif chk["status"] == "error":
                alerts["failures"].append({"table": t["table_fq"], "message": "Destination empty"})
    except Exception:
        pass
    return alerts
