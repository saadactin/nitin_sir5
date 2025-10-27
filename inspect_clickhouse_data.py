"""Direct ClickHouse query to check table structure and data"""
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

print("=" * 100)
print("CLICKHOUSE TABLE STRUCTURE AND DATA CHECK")
print("=" * 100)

# Check table structure
print("\n1. Table Structure (DESCRIBE):")
print("-" * 100)
result = client.execute('DESCRIBE saadtest.school_data_1')
for row in result:
    print(f"Column: {row[0]}, Type: {row[1]}")

# Count rows
print("\n2. Total Row Count:")
print("-" * 100)
result = client.execute('SELECT COUNT(*) FROM saadtest.school_data_1')
print(f"Total rows: {result[0][0]}")

# Sample data with all columns
print("\n3. First 5 Rows (All Columns):")
print("-" * 100)
result = client.execute('SELECT * FROM saadtest.school_data_1 LIMIT 5')
for i, row in enumerate(result, 1):
    print(f"\nRow {i}:")
    print(f"  student_id: {row[0]}")
    print(f"  student_name: {row[1]}")
    print(f"  subject: {row[2]}")
    print(f"  grade: {row[3]}")
    print(f"  attendance: {row[4]}")

# Check if there are any NULL values
print("\n4. NULL Value Check:")
print("-" * 100)
result = client.execute('''
    SELECT 
        countIf(student_id IS NULL) as null_ids,
        countIf(student_name IS NULL) as null_names,
        countIf(subject IS NULL) as null_subjects,
        countIf(grade IS NULL) as null_grades,
        countIf(attendance IS NULL) as null_attendance
    FROM saadtest.school_data_1
''')
print(f"NULL student_id: {result[0][0]}")
print(f"NULL student_name: {result[0][1]}")
print(f"NULL subject: {result[0][2]}")
print(f"NULL grade: {result[0][3]}")
print(f"NULL attendance: {result[0][4]}")

# Check data distribution
print("\n5. Data Distribution (Grade Count):")
print("-" * 100)
result = client.execute('SELECT grade, COUNT(*) as cnt FROM saadtest.school_data_1 GROUP BY grade ORDER BY cnt DESC')
for row in result:
    print(f"Grade {row[0]}: {row[1]} students")

print("\n" + "=" * 100)
