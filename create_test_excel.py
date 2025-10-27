"""Create test Excel file with student data"""
import pandas as pd

# Sample data from user
data = {
    'Student ID': ['STU11001', 'STU11002', 'STU11003', 'STU11004', 'STU11005', 'STU11006', 'STU11007', 'STU11008', 'STU11009', 'STU11010'],
    'Student Name': ['Charlotte Moore', 'Jacob Martinez', 'James Gonzalez', 'Elijah Smith', 'James Johnson', 'Liam Gonzalez', 'Michael Hernandez', 'Mason Taylor', 'Noah Thomas', 'Olivia Lopez'],
    'Subject': ['History', 'History', 'Mathematics', 'Mathematics', 'Physical Education', 'Geography', 'English', 'Biology', 'Chemistry', 'Physics'],
    'Grade': ['D', 'F', 'C', 'D', 'D', 'B', 'D', 'B', 'A', 'A'],
    'Attendance %': [83.9, 80.1, 99.5, 91.2, 79.9, 70.7, 75.8, 88.0, 75.8, 97.5]
}

df = pd.DataFrame(data)

# Save to Excel
output_file = 'test_school_data.xlsx'
df.to_excel(output_file, index=False)
print(f"✓ Created test Excel file: {output_file}")
print(f"✓ Rows: {len(df)}")
print(f"✓ Columns: {list(df.columns)}")
print(f"\n✓ First few rows:")
print(df.head())
