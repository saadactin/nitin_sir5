"""
Check CSV Import Status - Compare CSV files with ClickHouse tables
Shows exact percentage of data transferred
"""

import os
import csv
from pathlib import Path
from db_utils import load_clickhouse_config
from clickhouse_driver import Client

def count_csv_rows(csv_file):
    """Count rows in CSV file (excluding header)"""
    try:
        with open(csv_file, 'r', encoding='utf-8-sig') as f:
            return sum(1 for _ in f) - 1  # Subtract header
    except Exception as e:
        print(f"Error counting {csv_file}: {e}")
        return 0

def check_import_status():
    """Check import status and calculate percentage"""
    csv_folder = Path('csv_output')
    csv_files = list(csv_folder.rglob('*.csv'))
    
    # Map CSV files to expected table names
    expected_tables = {
        'AN/ANN1.csv': 'ANN1',
        'NN/NNM1.csv': 'NNM1',
        'OD/ODSN.csv': 'ODSN',
        'OS/OSRQ.csv': 'OSRQ',
        'OU/OUAL.csv': 'OUAL'
    }
    
    print("=" * 90)
    print("CSV TO CLICKHOUSE IMPORT STATUS CHECK")
    print("=" * 90)
    print()
    
    # Step 1: Count CSV rows
    print("[STEP 1] Counting CSV rows...")
    print("-" * 90)
    csv_data = {}
    total_csv_rows = 0
    
    for csv_file in sorted(csv_files):
        rel_path = str(csv_file.relative_to(csv_folder))
        table_name = expected_tables.get(rel_path, csv_file.stem.upper())
        row_count = count_csv_rows(csv_file)
        csv_data[table_name] = {
            'csv_rows': row_count,
            'file': csv_file.name,
            'path': rel_path
        }
        total_csv_rows += row_count
        print(f"  {csv_file.name:25} -> {table_name:10} : {row_count:>12,} rows")
    
    print()
    print(f"  [TOTAL] Total CSV rows across all files: {total_csv_rows:,}")
    print()
    
    # Step 2: Check ClickHouse
    print("[STEP 2] Checking ClickHouse JARVIS_DB...")
    print("-" * 90)
    
    try:
        config = load_clickhouse_config()
        client = Client(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            password=config['password']
        )
        
        # Get all tables in JARVIS_DB
        all_tables = client.execute('SHOW TABLES FROM JARVIS_DB')
        all_table_names = [t[0] for t in all_tables]
        
        print(f"  Found {len(all_table_names)} tables in JARVIS_DB")
        print()
        
        total_ch_rows = 0
        tables_found = 0
        tables_complete = 0
        tables_missing = []
        table_details = []
        
        print("  Table-by-table comparison:")
        print()
        
        for table_name, data in sorted(csv_data.items()):
            csv_rows = data['csv_rows']
            
            if table_name in all_table_names:
                try:
                    ch_rows = client.execute(f'SELECT count() FROM JARVIS_DB.{table_name}')[0][0]
                    match = ch_rows == csv_rows
                    percentage = (ch_rows / csv_rows * 100) if csv_rows > 0 else 0
                    
                    status_icon = "[OK]" if match else "[WARN]"
                    status_text = "COMPLETE" if match else f"{percentage:.1f}%"
                    
                    print(f"  {status_icon} {table_name:10} : CSV={csv_rows:>12,} | CH={ch_rows:>12,} | {status_text:>10}")
                    
                    total_ch_rows += ch_rows
                    tables_found += 1
                    if match:
                        tables_complete += 1
                    
                    table_details.append({
                        'table': table_name,
                        'csv_rows': csv_rows,
                        'ch_rows': ch_rows,
                        'percentage': percentage,
                        'status': 'COMPLETE' if match else 'INCOMPLETE'
                    })
                    
                    if not match:
                        missing = csv_rows - ch_rows
                        tables_missing.append((table_name, csv_rows, ch_rows, missing))
                        
                except Exception as e:
                    print(f"  [ERROR] {table_name:10} : Error reading table - {e}")
                    tables_missing.append((table_name, csv_rows, 0, csv_rows))
                    table_details.append({
                        'table': table_name,
                        'csv_rows': csv_rows,
                        'ch_rows': 0,
                        'percentage': 0,
                        'status': 'ERROR'
                    })
            else:
                print(f"  [ERROR] {table_name:10} : NOT FOUND in ClickHouse")
                tables_missing.append((table_name, csv_rows, 0, csv_rows))
                table_details.append({
                    'table': table_name,
                    'csv_rows': csv_rows,
                    'ch_rows': 0,
                    'percentage': 0,
                    'status': 'NOT FOUND'
                })
        
        print()
        print("=" * 90)
        print("[OVERALL IMPORT STATUS]")
        print("=" * 90)
        print()
        
        # Calculate overall percentage
        if total_csv_rows > 0:
            overall_percentage = (total_ch_rows / total_csv_rows * 100)
        else:
            overall_percentage = 0
        
        print(f"  CSV Files Processed:        {len(csv_data)}")
        print(f"  Tables Found in ClickHouse: {tables_found}/{len(csv_data)}")
        print(f"  Tables 100% Complete:      {tables_complete}/{len(csv_data)}")
        print(f"  Tables Missing/Incomplete:  {len(tables_missing)}")
        print()
        print(f"  Total CSV Rows:            {total_csv_rows:>15,}")
        print(f"  Total ClickHouse Rows:      {total_ch_rows:>15,}")
        print(f"  Missing Rows:               {(total_csv_rows - total_ch_rows):>15,}")
        print()
        print("  " + "=" * 86)
        print(f"  [OVERALL TRANSFER PROGRESS]: {overall_percentage:>6.2f}%")
        print("  " + "=" * 86)
        print()
        
        # Detailed breakdown
        if tables_missing:
            print("  [WARNING] DETAILED BREAKDOWN:")
            print()
            for table, csv_rows, ch_rows, missing in tables_missing:
                if ch_rows == 0:
                    print(f"     [ERROR] {table:10} : {csv_rows:>12,} rows missing (0.00%)")
                else:
                    pct = (ch_rows / csv_rows * 100) if csv_rows > 0 else 0
                    print(f"     [WARN] {table:10} : {ch_rows:>12,}/{csv_rows:<12,} rows ({pct:>5.2f}%) - {missing:>12,} missing")
            print()
        
        # Final status
        if overall_percentage == 100:
            print("  [SUCCESS] ALL DATA SUCCESSFULLY TRANSFERRED!")
            print("  [SUCCESS] All tables are complete with 100% accuracy")
        elif overall_percentage > 0:
            print(f"  [IN PROGRESS] Import in progress: {overall_percentage:.2f}% complete")
            print(f"  [WARNING] {len(tables_missing)} table(s) need attention")
            print()
            print("  [TIP] To complete the import, run:")
            print("     python import_csv_to_clickhouse.py")
        else:
            print("  [ERROR] No data imported yet")
            print()
            print("  [TIP] To start the import, run:")
            print("     python import_csv_to_clickhouse.py")
        
        print()
        print("=" * 90)
        
        return {
            'overall_percentage': overall_percentage,
            'total_csv_rows': total_csv_rows,
            'total_ch_rows': total_ch_rows,
            'tables_complete': tables_complete,
            'tables_total': len(csv_data),
            'table_details': table_details
        }
        
    except Exception as e:
        print(f"  [ERROR] Error connecting to ClickHouse: {e}")
        print()
        print("  [TIP] Make sure ClickHouse is running and .env file is configured")
        print("=" * 90)
        return None

if __name__ == '__main__':
    result = check_import_status()
    if result:
        print(f"\n[FINAL RESULT] {result['overall_percentage']:.2f}% of data transferred")

