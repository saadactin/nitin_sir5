"""Display all data from saadtest2.school_data_4"""
try:
    from clickhouse_driver import Client as CHClient
except ImportError:
    print("✗ clickhouse-driver not installed")
    exit(1)

client = CHClient(
    host='localhost',
    port=9000,
    user='default',
    password=''
)

print("=" * 120)
print("DATA IN: saadtest2.school_data_4")
print("=" * 120)

# Get all data
result = client.execute('SELECT * FROM saadtest2.school_data_4 ORDER BY student_id')

print(f"\nTotal Rows: {len(result)}\n")
print(f"{'#':<5} {'Student ID':<12} {'Student Name':<25} {'Subject':<20} {'Grade':<7} {'Attendance':<12}")
print("-" * 120)

for i, row in enumerate(result, 1):
    print(f"{i:<5} {row[0]:<12} {row[1]:<25} {row[2]:<20} {row[3]:<7} {row[4]:<12}")

print("-" * 120)
print(f"\n✅ Total: {len(result)} rows displayed")
print("=" * 120)

# Summary statistics
print("\n📊 DATA SUMMARY:")
print("-" * 120)

# Grade distribution
result = client.execute('SELECT grade, COUNT(*) as cnt FROM saadtest2.school_data_4 GROUP BY grade ORDER BY grade')
print("\nGrade Distribution:")
for row in result:
    print(f"  Grade {row[0]}: {row[1]} students")

# Subject distribution
result = client.execute('SELECT subject, COUNT(*) as cnt FROM saadtest2.school_data_4 GROUP BY subject ORDER BY cnt DESC LIMIT 10')
print("\nTop 10 Subjects:")
for row in result:
    print(f"  {row[0]}: {row[1]} students")

print("=" * 120)
