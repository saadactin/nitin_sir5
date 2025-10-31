import psycopg2

conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='test1',
    user='migration_user',
    password='StrongPassword123'
)

cur = conn.cursor()

# Delete the problematic source
cur.execute("DELETE FROM data_sources WHERE server_address = 'http://localhost:4007/api/data'")
deleted = cur.rowcount

conn.commit()
cur.close()
conn.close()

print(f"Deleted {deleted} source(s) pointing to localhost:4007")
print("Restart Flask now - the errors will stop!")

