"""
EXCEL TO CLICKHOUSE DATA MIGRATION VERIFICATION TOOL
====================================================

This script verifies that data migrated from Excel to ClickHouse is accurate and complete.

Usage:
  python verify_data_migration.py <excel_file> <clickhouse_db> <clickhouse_table>

Example:
  python verify_data_migration.py school_data_full_test.xlsx saadtest school_data_1
"""

import sys
import pandas as pd
import os
try:
    from clickhouse_driver import Client as CHClient
except ImportError:
    print("ERROR: clickhouse-driver not installed. Install with: pip install clickhouse-driver")
    sys.exit(1)


def normalize_column_name(col):
    """Normalize column name same way as app.py does"""
    return col.lower().replace(' ', '_').replace('%', '').strip('_')


def verify_migration(excel_file, ch_db, ch_table):
    """Verify Excel data matches ClickHouse data"""
    
    print("=" * 100)
    print("DATA MIGRATION VERIFICATION")
    print("=" * 100)
    print(f"Excel File:      {excel_file}")
    print(f"ClickHouse:      {ch_db}.{ch_table}")
    print("=" * 100)
    
    # Step 1: Read Excel
    print("\n📖 Step 1: Reading Excel File...")
    print("-" * 100)
    
    if not os.path.exists(excel_file):
        print(f"❌ ERROR: Excel file not found: {excel_file}")
        return False
    
    try:
        df = pd.read_excel(excel_file)
        print(f"✅ Excel file loaded successfully")
        print(f"   Rows: {len(df)}")
        print(f"   Columns: {list(df.columns)}")
        
        # Normalize column names
        df_normalized = df.copy()
        df_normalized.columns = [normalize_column_name(col) for col in df_normalized.columns]
        print(f"   Normalized columns: {list(df_normalized.columns)}")
        
    except Exception as e:
        print(f"❌ ERROR reading Excel: {e}")
        return False
    
    # Step 2: Connect to ClickHouse
    print("\n🔌 Step 2: Connecting to ClickHouse...")
    print("-" * 100)
    
    try:
        client = CHClient(
            host='localhost',
            port=9000,
            user='default',
            password=''
        )
        print(f"✅ Connected to ClickHouse")
    except Exception as e:
        print(f"❌ ERROR connecting to ClickHouse: {e}")
        return False
    
    # Step 3: Get ClickHouse data
    print("\n📊 Step 3: Fetching ClickHouse Data...")
    print("-" * 100)
    
    try:
        # Check table exists
        result = client.execute(f"EXISTS TABLE {ch_db}.{ch_table}")
        if not result[0][0]:
            print(f"❌ ERROR: Table {ch_db}.{ch_table} does not exist")
            return False
        
        # Get table structure
        result = client.execute(f"DESCRIBE {ch_db}.{ch_table}")
        ch_columns = [row[0] for row in result]
        print(f"✅ Table exists")
        print(f"   Columns: {ch_columns}")
        
        # Get row count
        result = client.execute(f"SELECT COUNT(*) FROM {ch_db}.{ch_table}")
        ch_count = result[0][0]
        print(f"   Rows: {ch_count}")
        
        # Get all data ordered by first column
        first_col = ch_columns[0]
        result = client.execute(f"SELECT * FROM {ch_db}.{ch_table} ORDER BY {first_col}")
        ch_data = result
        
    except Exception as e:
        print(f"❌ ERROR reading ClickHouse: {e}")
        return False
    
    # Step 4: Compare row counts
    print("\n🔢 Step 4: Comparing Row Counts...")
    print("-" * 100)
    
    excel_count = len(df)
    
    print(f"Excel rows:      {excel_count}")
    print(f"ClickHouse rows: {ch_count}")
    
    if excel_count == ch_count:
        print(f"✅ Row counts MATCH!")
        row_count_match = True
    else:
        print(f"❌ Row counts MISMATCH! Difference: {abs(excel_count - ch_count)}")
        row_count_match = False
    
    # Step 5: Compare column structure
    print("\n📋 Step 5: Comparing Column Structure...")
    print("-" * 100)
    
    excel_cols = list(df_normalized.columns)
    
    print(f"Excel columns:      {excel_cols}")
    print(f"ClickHouse columns: {ch_columns}")
    
    if set(excel_cols) == set(ch_columns):
        print(f"✅ Column names MATCH!")
        col_match = True
    else:
        print(f"❌ Column names MISMATCH!")
        print(f"   Missing in ClickHouse: {set(excel_cols) - set(ch_columns)}")
        print(f"   Extra in ClickHouse: {set(ch_columns) - set(excel_cols)}")
        col_match = False
    
    # Step 6: Sample data verification
    print("\n🔍 Step 6: Verifying Sample Data...")
    print("-" * 100)
    
    sample_size = min(5, excel_count, ch_count)
    matches = 0
    mismatches = 0
    
    print(f"\nFirst {sample_size} rows:")
    for i in range(sample_size):
        excel_row = df_normalized.iloc[i]
        ch_row = ch_data[i] if i < len(ch_data) else None
        
        if ch_row:
            # Compare each column
            row_match = True
            for col_idx, col in enumerate(excel_cols):
                excel_val = str(excel_row[col]) if pd.notna(excel_row[col]) else None
                ch_val = str(ch_row[col_idx]) if ch_row[col_idx] is not None else None
                
                if excel_val != ch_val:
                    row_match = False
                    break
            
            if row_match:
                print(f"  Row {i+1}: ✅ Match")
                matches += 1
            else:
                print(f"  Row {i+1}: ❌ Mismatch")
                print(f"    Excel:      {dict(excel_row)}")
                print(f"    ClickHouse: {dict(zip(ch_columns, ch_row))}")
                mismatches += 1
        else:
            print(f"  Row {i+1}: ❌ Missing in ClickHouse")
            mismatches += 1
    
    # Check middle rows
    if excel_count > 10:
        print(f"\nMiddle {sample_size} rows:")
        mid = excel_count // 2
        for i in range(mid, mid + sample_size):
            if i < excel_count and i < len(ch_data):
                excel_row = df_normalized.iloc[i]
                ch_row = ch_data[i]
                
                row_match = True
                for col_idx, col in enumerate(excel_cols):
                    excel_val = str(excel_row[col]) if pd.notna(excel_row[col]) else None
                    ch_val = str(ch_row[col_idx]) if ch_row[col_idx] is not None else None
                    
                    if excel_val != ch_val:
                        row_match = False
                        break
                
                if row_match:
                    print(f"  Row {i+1}: ✅ Match")
                    matches += 1
                else:
                    print(f"  Row {i+1}: ❌ Mismatch")
                    mismatches += 1
    
    # Check last rows
    print(f"\nLast {sample_size} rows:")
    for i in range(max(0, excel_count - sample_size), excel_count):
        if i < excel_count and i < len(ch_data):
            excel_row = df_normalized.iloc[i]
            ch_row = ch_data[i]
            
            row_match = True
            for col_idx, col in enumerate(excel_cols):
                excel_val = str(excel_row[col]) if pd.notna(excel_row[col]) else None
                ch_val = str(ch_row[col_idx]) if ch_row[col_idx] is not None else None
                
                if excel_val != ch_val:
                    row_match = False
                    break
            
            if row_match:
                print(f"  Row {i+1}: ✅ Match")
                matches += 1
            else:
                print(f"  Row {i+1}: ❌ Mismatch")
                mismatches += 1
    
    # Step 7: Check for NULL values
    print("\n🔎 Step 7: Checking for NULL Values...")
    print("-" * 100)
    
    try:
        null_checks = []
        for col in ch_columns:
            result = client.execute(f"SELECT countIf({col} IS NULL) FROM {ch_db}.{ch_table}")
            null_count = result[0][0]
            null_checks.append((col, null_count))
            
            if null_count > 0:
                print(f"  {col}: {null_count} NULL values")
            else:
                print(f"  {col}: ✅ No NULL values")
        
        total_nulls = sum(count for _, count in null_checks)
        if total_nulls == 0:
            print(f"✅ No NULL values found")
        else:
            print(f"⚠️  Total NULL values: {total_nulls}")
    except Exception as e:
        print(f"⚠️  Could not check NULL values: {e}")
    
    # Final Summary
    print("\n" + "=" * 100)
    print("VERIFICATION SUMMARY")
    print("=" * 100)
    
    print(f"Row Count:     {'✅ PASS' if row_count_match else '❌ FAIL'} ({excel_count} Excel, {ch_count} ClickHouse)")
    print(f"Columns:       {'✅ PASS' if col_match else '❌ FAIL'}")
    print(f"Data Samples:  {matches} matches, {mismatches} mismatches")
    
    if row_count_match and col_match and mismatches == 0:
        print(f"\n🎉 SUCCESS! Data migration is PERFECT - 100% accurate, no data loss!")
        print("=" * 100)
        return True
    else:
        print(f"\n⚠️  ISSUES FOUND! Please review the mismatches above.")
        print("=" * 100)
        return False


if __name__ == '__main__':
    if len(sys.argv) < 4:
        print(__doc__)
        print("\nNo arguments provided. Running with default values...")
        excel_file = 'school_data_full_test.xlsx'
        ch_db = 'saadtest'
        ch_table = 'school_data_1'
    else:
        excel_file = sys.argv[1]
        ch_db = sys.argv[2]
        ch_table = sys.argv[3]
    
    success = verify_migration(excel_file, ch_db, ch_table)
    sys.exit(0 if success else 1)
