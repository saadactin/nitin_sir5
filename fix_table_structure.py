"""Check actual data_sources table structure"""
import psycopg2
import sys
sys.path.insert(0, '.')
from db_utils import load_pg_config

pg_conf = load_pg_config()
conn = psycopg2.connect(
    dbname=pg_conf.get('database', 'metrics_sync_tables'),
    user=pg_conf.get('username'),
    password=pg_conf.get('password'),
    host=pg_conf.get('host'),
    port=int(pg_conf.get('port', 5432))
)

cur = conn.cursor()

# Get table structure
cur.execute("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = 'data_sources'
    ORDER BY ordinal_position
""")

print("Current data_sources table structure:")
print("-" * 50)
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]}")
print("-" * 50)

# Check if we need to add connection_details column
cur.execute("""
    SELECT EXISTS (
        SELECT FROM information_schema.columns 
        WHERE table_name = 'data_sources' AND column_name = 'connection_details'
    )
""")

has_column = cur.fetchone()[0]

if not has_column:
    print("\n⚠ connection_details column is MISSING")
    print("Adding column...")
    cur.execute("ALTER TABLE data_sources ADD COLUMN connection_details JSONB")
    conn.commit()
    print("✓ Column added")
else:
    print("\n✓ connection_details column exists")

cur.close()
conn.close()
