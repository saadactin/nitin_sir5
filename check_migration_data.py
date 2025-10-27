"""Check data migration from Excel to ClickHouse"""
try:
    from clickhouse_driver import Client as CHClient
except ImportError:
    CHClient = None
import pandas as pd
import os

def check_clickhouse_data():
    """Check data in ClickHouse"""
    try:
        if CHClient is None:
            print("✗ clickhouse-driver not installed")
            return []
            
        client = CHClient(
            host='localhost',
            port=9000,
            user='default',
            password=''
        )
        
        # Get total count
        result = client.execute('SELECT COUNT(*) as total FROM saadtest.school_data_1')
        total = result[0][0]
        print(f"✓ Total rows in ClickHouse: {total}")
        
        # Get first 10 rows
        result = client.execute('SELECT * FROM saadtest.school_data_1 ORDER BY student_id LIMIT 10')
        print(f"\n✓ First 10 rows in ClickHouse:")
        print("-" * 100)
        for row in result:
            print(f"ID: {row[0]}, Name: {row[1]}, Subject: {row[2]}, Grade: {row[3]}, Attendance: {row[4]}")
        
        # Get all data for comparison
        result = client.execute('SELECT * FROM saadtest.school_data_1 ORDER BY student_id')
        all_rows = result
        print(f"\n✓ All rows count: {len(all_rows)}")
        
        return all_rows
        
    except Exception as e:
        print(f"✗ Error checking ClickHouse: {e}")
        return []

def check_excel_data():
    """Check data in Excel file"""
    try:
        # Look for Excel files in uploads directory
        uploads_dir = 'uploads'
        if not os.path.exists(uploads_dir):
            print(f"✗ Uploads directory not found")
            return None
            
        excel_files = [f for f in os.listdir(uploads_dir) if f.endswith(('.xlsx', '.xls'))]
        
        if not excel_files:
            print(f"✗ No Excel files found in uploads directory")
            return None
        
        # Use the most recent Excel file
        excel_files.sort(key=lambda x: os.path.getmtime(os.path.join(uploads_dir, x)), reverse=True)
        excel_file = os.path.join(uploads_dir, excel_files[0])
        
        print(f"✓ Reading Excel file: {excel_file}")
        df = pd.read_excel(excel_file)
        
        print(f"\n✓ Excel data shape: {df.shape}")
        print(f"✓ Excel columns: {df.columns.tolist()}")
        print(f"\n✓ First 10 rows in Excel:")
        print("-" * 100)
        print(df.head(10).to_string())
        
        print(f"\n✓ Total rows in Excel: {len(df)}")
        
        return df
        
    except Exception as e:
        print(f"✗ Error reading Excel: {e}")
        return None

def compare_data(excel_df, clickhouse_rows):
    """Compare Excel and ClickHouse data"""
    print("\n" + "="*100)
    print("DATA COMPARISON")
    print("="*100)
    
    if excel_df is None:
        print("✗ Cannot compare - Excel data not available")
        return
    
    excel_count = len(excel_df)
    clickhouse_count = len(clickhouse_rows)
    
    print(f"\n📊 Row Count:")
    print(f"   Excel: {excel_count} rows")
    print(f"   ClickHouse: {clickhouse_count} rows")
    
    if excel_count == clickhouse_count:
        print(f"   ✓ Row counts match!")
    else:
        print(f"   ✗ Row count mismatch! Missing {excel_count - clickhouse_count} rows")
    
    # Check specific records
    if clickhouse_count > 0 and excel_count > 0:
        print(f"\n📝 Sample Data Verification:")
        print("-" * 100)
        
        # Check first 3 rows
        for i in range(min(3, excel_count)):
            excel_row = excel_df.iloc[i]
            print(f"\nRow {i+1}:")
            print(f"  Excel: {excel_row.to_dict()}")
            
            if i < len(clickhouse_rows):
                ch_row = clickhouse_rows[i]
                print(f"  ClickHouse: student_id={ch_row[0]}, student_name={ch_row[1]}, subject={ch_row[2]}, grade={ch_row[3]}, attendance={ch_row[4]}")
                
                # Compare values
                excel_id = str(excel_row.iloc[0]).strip() if pd.notna(excel_row.iloc[0]) else ''
                ch_id = str(ch_row[0]).strip() if ch_row[0] else ''
                
                if excel_id == ch_id:
                    print(f"  ✓ Match")
                else:
                    print(f"  ✗ Mismatch - Excel ID: {excel_id}, ClickHouse ID: {ch_id}")

if __name__ == '__main__':
    print("="*100)
    print("EXCEL TO CLICKHOUSE DATA MIGRATION VERIFICATION")
    print("="*100)
    
    print("\n1. Checking ClickHouse Data...")
    print("-" * 100)
    clickhouse_rows = check_clickhouse_data()
    
    print("\n\n2. Checking Excel Data...")
    print("-" * 100)
    excel_df = check_excel_data()
    
    print("\n\n3. Comparing Data...")
    compare_data(excel_df, clickhouse_rows)
    
    print("\n" + "="*100)
    print("VERIFICATION COMPLETE")
    print("="*100)
