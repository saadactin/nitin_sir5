import psycopg2
import os
import yaml

# Path to YAML config
CONFIG_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "config/db_connections.yaml")
)

def load_pg_config():
    """Load PostgreSQL config from YAML"""
    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)
    return config["postgresql"]

def get_pg_connection():
    """Return a live PostgreSQL connection"""
    conf = load_pg_config()
    return psycopg2.connect(
        dbname=conf["database"],
        user=conf["username"],
        password=conf["password"],
        host=conf["host"],
        port=conf["port"],
    )

def init_pg_schema():
    """Create schema + tables if not exists"""
    conn = get_pg_connection()
    cur = conn.cursor()

    # Create schema
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS metrics_sync_tables;
    """)

    # Create schedules table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS metrics_sync_tables.schedules (
            id SERIAL PRIMARY KEY,
            server_name TEXT NOT NULL,
            job_type TEXT NOT NULL,
            last_run TIMESTAMP,
            status TEXT,
            error TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)

    # Create sync_history table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS metrics_sync_tables.sync_history (
            id SERIAL PRIMARY KEY,
            server_name TEXT NOT NULL,
            sync_time TIMESTAMP DEFAULT NOW(),
            status TEXT NOT NULL,
            details TEXT
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
