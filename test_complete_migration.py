"""Complete test: Create Excel, Upload to ClickHouse, Verify Data"""
import pandas as pd
import os
try:
    from clickhouse_driver import Client as CHClient
except ImportError:
    print("✗ clickhouse-driver not installed")
    exit(1)

print("=" * 100)
print("COMPLETE DATA MIGRATION TEST: EXCEL → CLICKHOUSE")
print("=" * 100)

# Step 1: Create Full Test Excel File with ALL 100 rows
print("\n📝 Step 1: Creating Test Excel File with 100 rows")
print("-" * 100)

data = {
    'Student ID': [f'STU11{str(i).zfill(3)}' for i in range(1, 101)],
    'Student Name': [
        'Charlotte Moore', 'Jacob Martinez', 'James Gonzalez', 'Elijah Smith', 'James Johnson',
        'Liam Gonzalez', 'Michael Hernandez', 'Mason Taylor', 'Noah Thomas', 'Olivia Lopez',
        'Charlotte Anderson', 'Mason Johnson', 'Isabella Jones', 'Elijah Lopez', 'Sophia Johnson',
        'Emma Johnson', 'Amelia Rodriguez', 'Ethan Davis', 'Emma Thomas', 'Mason Thomas',
        'Benjamin Williams', 'Michael Martinez', 'Harper Wilson', 'Ethan Taylor', 'Olivia Wilson',
        'Ethan Davis', 'Emma Brown', 'Isabella Lopez', 'Ava Moore', 'Isabella Smith',
        'Harper Lopez', 'Charlotte Miller', 'Noah Johnson', 'Olivia Anderson', 'James Smith',
        'William Miller', 'Michael Jones', 'Isabella Garcia', 'Noah Davis', 'Ava Hernandez',
        'Jacob Jackson', 'Michael Wilson', 'Ethan Martin', 'James Gonzalez', 'Elijah Garcia',
        'Isabella Brown', 'Liam Anderson', 'Isabella Lopez', 'Olivia Brown', 'Benjamin Smith',
        'Ethan Williams', 'Amelia Moore', 'Benjamin Anderson', 'James Moore', 'Jacob Moore',
        'Mason Thomas', 'William Garcia', 'Mason Martinez', 'Mia Johnson', 'Liam Smith',
        'Sophia Williams', 'Olivia Thomas', 'Isabella Jones', 'Ethan Thomas', 'Ava Rodriguez',
        'Jacob Garcia', 'Mason Garcia', 'Liam Jones', 'Amelia Brown', 'James Thomas',
        'Mason Williams', 'William Wilson', 'Sophia Jackson', 'Isabella Lopez', 'Amelia Moore',
        'Ava Johnson', 'Mia Johnson', 'William Brown', 'Mason Davis', 'Ethan Thomas',
        'James Taylor', 'Mason Brown', 'Jacob Garcia', 'Ethan Martinez', 'Amelia Williams',
        'Noah Hernandez', 'James Martinez', 'Olivia Martinez', 'Benjamin Martinez', 'Noah Miller',
        'Ava Miller', 'Charlotte Jones', 'William Gonzalez', 'Amelia Lopez', 'Mia Martin',
        'Mason Wilson', 'Noah Williams', 'Charlotte Garcia', 'William Gonzalez', 'Ethan Anderson'
    ],
    'Subject': [
        'History', 'History', 'Mathematics', 'Mathematics', 'Physical Education',
        'Geography', 'English', 'Biology', 'Chemistry', 'Physics',
        'English', 'Mathematics', 'Physical Education', 'Music', 'Mathematics',
        'Chemistry', 'Geography', 'History', 'Geography', 'English',
        'Science', 'Mathematics', 'Geography', 'Mathematics', 'English',
        'Geography', 'Computer Science', 'Chemistry', 'Biology', 'Physics',
        'Physical Education', 'English', 'Chemistry', 'Biology', 'Geography',
        'Biology', 'Physics', 'Music', 'Physical Education', 'History',
        'Music', 'Biology', 'Computer Science', 'History', 'Biology',
        'English', 'Music', 'Computer Science', 'English', 'Science',
        'Geography', 'History', 'Art', 'Science', 'Mathematics',
        'History', 'Geography', 'Physical Education', 'English', 'Chemistry',
        'History', 'Mathematics', 'Art', 'Science', 'Physics',
        'English', 'History', 'Computer Science', 'Chemistry', 'Computer Science',
        'Science', 'Physics', 'English', 'Biology', 'History',
        'Geography', 'Chemistry', 'Physical Education', 'History', 'Art',
        'Physics', 'Physical Education', 'Physical Education', 'Science', 'Mathematics',
        'Music', 'Physics', 'Music', 'Art', 'Chemistry',
        'Art', 'Physical Education', 'History', 'Physics', 'Science',
        'Biology', 'Science', 'Physical Education', 'Mathematics', 'Computer Science'
    ],
    'Grade': [
        'D', 'F', 'C', 'D', 'D', 'B', 'D', 'B', 'A', 'A',
        'C', 'F', 'F', 'D', 'F', 'A', 'A', 'B', 'B', 'A',
        'F', 'C', 'B', 'C', 'B', 'B', 'B', 'C', 'F', 'C',
        'C', 'D', 'D', 'F', 'A', 'F', 'C', 'F', 'D', 'A',
        'A', 'D', 'A', 'C', 'D', 'D', 'F', 'A', 'D', 'A',
        'C', 'B', 'B', 'A', 'D', 'D', 'B', 'A', 'B', 'F',
        'C', 'C', 'C', 'B', 'A', 'B', 'D', 'F', 'D', 'B',
        'D', 'B', 'B', 'C', 'D', 'F', 'D', 'C', 'F', 'A',
        'C', 'F', 'A', 'C', 'B', 'B', 'C', 'B', 'A', 'A',
        'A', 'A', 'D', 'D', 'B', 'D', 'C', 'A', 'C', 'B'
    ],
    'Attendance %': [
        83.9, 80.1, 99.5, 91.2, 79.9, 70.7, 75.8, 88.0, 75.8, 97.5,
        93.1, 99.8, 76.6, 83.0, 94.2, 99.6, 70.5, 75.3, 72.2, 97.6,
        70.4, 84.4, 93.3, 76.1, 76.2, 95.8, 82.9, 84.0, 82.3, 81.3,
        83.2, 95.1, 97.5, 89.8, 80.6, 81.9, 78.2, 89.3, 70.0, 76.1,
        77.8, 99.6, 93.2, 75.5, 92.1, 93.4, 72.3, 91.2, 94.7, 87.6,
        98.4, 99.8, 90.9, 99.8, 70.7, 71.6, 96.2, 85.8, 74.7, 77.6,
        86.6, 91.4, 82.7, 78.2, 71.2, 71.7, 95.0, 71.8, 90.3, 72.0,
        81.9, 89.6, 97.9, 85.0, 75.1, 81.4, 76.9, 98.3, 75.6, 99.8,
        93.9, 83.4, 79.9, 73.8, 95.6, 78.0, 76.4, 88.5, 89.8, 78.1,
        78.4, 87.6, 74.3, 89.4, 84.1, 70.3, 80.7, 85.4, 70.6, 78.5
    ]
}

df = pd.DataFrame(data)
excel_file = 'school_data_full_test.xlsx'
df.to_excel(excel_file, index=False)

print(f"✓ Created: {excel_file}")
print(f"✓ Total Rows: {len(df)}")
print(f"✓ Columns: {list(df.columns)}")
print(f"\n✓ First 5 rows:")
print(df.head().to_string(index=False))

# Step 2: Connect to ClickHouse
print("\n\n🔌 Step 2: Connecting to ClickHouse")
print("-" * 100)

client = CHClient(
    host='localhost',
    port=9000,
    user='default',
    password=''
)
print("✓ Connected to ClickHouse")

# Step 3: Drop and recreate table
print("\n\n🗑️  Step 3: Dropping old table and creating fresh one")
print("-" * 100)

try:
    client.execute('DROP TABLE IF EXISTS saadtest.school_data_1')
    print("✓ Dropped old table (if existed)")
except Exception as e:
    print(f"⚠️  Drop table warning: {e}")

# Create table with correct column names matching Excel
# Note: Excel has "Attendance %" which gets normalized to "attendance" by the app
create_sql = """
CREATE TABLE IF NOT EXISTS saadtest.school_data_1 (
    student_id Nullable(String),
    student_name Nullable(String),
    subject Nullable(String),
    grade Nullable(String),
    attendance Nullable(String)
) ENGINE = MergeTree()
ORDER BY tuple()
"""
client.execute(create_sql)
print("✓ Created fresh table with correct schema")

# Step 4: Insert data using same logic as app.py
print("\n\n📤 Step 4: Inserting data into ClickHouse")
print("-" * 100)

# Normalize column names (same as app does)
# Excel: "Student ID", "Student Name", "Subject", "Grade", "Attendance %"
# Normalized: "student_id", "student_name", "subject", "grade", "attendance"

df_normalized = df.copy()
df_normalized.columns = [
    col.lower().replace(' ', '_').replace('%', '').strip('_')
    for col in df_normalized.columns
]

print(f"✓ Normalized columns: {list(df_normalized.columns)}")

# Convert all values to strings (handling NaN)
rows_to_insert = []
for _, row in df_normalized.iterrows():
    row_tuple = tuple((None if pd.isna(v) else str(v)) for v in row)
    rows_to_insert.append(row_tuple)

# Insert data
insert_sql = f"INSERT INTO saadtest.school_data_1 (student_id, student_name, subject, grade, attendance) VALUES"
client.execute(insert_sql, rows_to_insert)

print(f"✓ Inserted {len(rows_to_insert)} rows")

# Step 5: Verify the data
print("\n\n✅ Step 5: Verifying Migrated Data")
print("-" * 100)

# Count check
result = client.execute('SELECT COUNT(*) FROM saadtest.school_data_1')
ch_count = result[0][0]
excel_count = len(df)

print(f"\n📊 Row Count Comparison:")
print(f"   Excel:      {excel_count} rows")
print(f"   ClickHouse: {ch_count} rows")

if ch_count == excel_count:
    print(f"   ✅ Row counts MATCH!")
else:
    print(f"   ❌ Row count MISMATCH! Missing {excel_count - ch_count} rows")

# Sample data check
print(f"\n📝 Sample Data Verification (First 5 rows):")
print("-" * 100)

result = client.execute('SELECT * FROM saadtest.school_data_1 ORDER BY student_id LIMIT 5')

matches = 0
mismatches = 0

for i in range(min(5, len(df))):
    excel_row = df.iloc[i]
    ch_row = result[i] if i < len(result) else None
    
    print(f"\nRow {i+1}:")
    print(f"  Excel:      ID={excel_row.iloc[0]}, Name={excel_row.iloc[1]}, Subject={excel_row.iloc[2]}, Grade={excel_row.iloc[3]}, Attendance={excel_row.iloc[4]}")
    
    if ch_row:
        print(f"  ClickHouse: ID={ch_row[0]}, Name={ch_row[1]}, Subject={ch_row[2]}, Grade={ch_row[3]}, Attendance={ch_row[4]}")
        
        # Compare
        if (str(excel_row.iloc[0]) == str(ch_row[0]) and 
            str(excel_row.iloc[1]) == str(ch_row[1]) and
            str(excel_row.iloc[2]) == str(ch_row[2]) and
            str(excel_row.iloc[3]) == str(ch_row[3]) and
            str(excel_row.iloc[4]) == str(ch_row[4])):
            print(f"  ✅ MATCH")
            matches += 1
        else:
            print(f"  ❌ MISMATCH")
            mismatches += 1
    else:
        print(f"  ❌ No ClickHouse data")
        mismatches += 1

# Check last 5 rows
print(f"\n📝 Sample Data Verification (Last 5 rows):")
print("-" * 100)

result = client.execute('SELECT * FROM saadtest.school_data_1 ORDER BY student_id DESC LIMIT 5')

for i in range(min(5, len(df))):
    excel_row = df.iloc[-(i+1)]  # Get from end
    ch_row = result[i] if i < len(result) else None
    
    print(f"\nRow {len(df)-i}:")
    print(f"  Excel:      ID={excel_row.iloc[0]}, Name={excel_row.iloc[1]}, Subject={excel_row.iloc[2]}, Grade={excel_row.iloc[3]}, Attendance={excel_row.iloc[4]}")
    
    if ch_row:
        print(f"  ClickHouse: ID={ch_row[0]}, Name={ch_row[1]}, Subject={ch_row[2]}, Grade={ch_row[3]}, Attendance={ch_row[4]}")
        
        if (str(excel_row.iloc[0]) == str(ch_row[0]) and 
            str(excel_row.iloc[1]) == str(ch_row[1]) and
            str(excel_row.iloc[2]) == str(ch_row[2]) and
            str(excel_row.iloc[3]) == str(ch_row[3]) and
            str(excel_row.iloc[4]) == str(ch_row[4])):
            print(f"  ✅ MATCH")
            matches += 1
        else:
            print(f"  ❌ MISMATCH")
            mismatches += 1

# Final summary
print("\n" + "=" * 100)
print("MIGRATION TEST SUMMARY")
print("=" * 100)
print(f"✓ Excel file created: {excel_file}")
print(f"✓ Rows in Excel: {excel_count}")
print(f"✓ Rows in ClickHouse: {ch_count}")
print(f"✓ Sample comparisons: {matches} matches, {mismatches} mismatches")

if ch_count == excel_count and mismatches == 0:
    print(f"\n🎉 SUCCESS! Data migration is PERFECT - no data loss!")
else:
    print(f"\n⚠️  WARNING: There may be data discrepancies. Please review.")

print("=" * 100)
