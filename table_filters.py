"""
Centralized helpers to decide which schemas/tables to exclude from comparisons.
Keep a small list of excluded schema names (case-insensitive). Use this helper from
metrics, analytics, and sync_summary so the exclusion logic is consistent.
"""
EXCLUDED_SCHEMAS = {"public", "metric_sync_tables", "metrics_sync_tables"}

def is_excluded_schema(schema_name: str) -> bool:
    if not schema_name:
        return False
    return schema_name.lower() in EXCLUDED_SCHEMAS

def filter_pg_tables(rows):
    """Filter an iterable of (schema_name, table_name) tuples, yielding only those
    that are NOT in the excluded schemas.
    """
    for schema_name, table_name in rows:
        if not is_excluded_schema(schema_name):
            yield schema_name, table_name
