"""
Verify Zoho data in ClickHouse database
Shows what data was actually stored from the API sync
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db_utils import load_clickhouse_config
from clickhouse_driver import Client

def verify_zoho_data():
    """Verify Zoho data stored in ClickHouse"""
    print("\n" + "="*70)
    print("Zoho Data Verification in ClickHouse")
    print("="*70)
    
    # Connect to ClickHouse
    ch_config = load_clickhouse_config()
    client = Client(
        host=ch_config['host'],
        port=ch_config['port'],
        user=ch_config['user'],
        password=ch_config['password']
    )
    
    database = "zoho"
    tables_to_check = [
        "test_zoho_leads",
        "test_zoho_leads_2", 
        "test_zoho_contacts"
    ]
    
    print(f"\nChecking database: {database}")
    print("-"*70)
    
    for table_name in tables_to_check:
        try:
            full_table = f"{database}.{table_name}"
            
            # Check if table exists
            tables = client.execute(f"SHOW TABLES FROM {database}")
            table_names = [t[0] for t in tables]
            
            if table_name not in table_names:
                print(f"\n[SKIP] Table {table_name} does not exist")
                continue
            
            # Get row count
            count_result = client.execute(f"SELECT COUNT(*) FROM {full_table}")
            row_count = count_result[0][0] if count_result else 0
            
            # Get column info
            columns_result = client.execute(f"DESCRIBE TABLE {full_table}")
            columns = [(col[0], col[1]) for col in columns_result]
            
            # Get sample data (first 3 rows)
            sample_result = client.execute(f"SELECT * FROM {full_table} LIMIT 3")
            
            print(f"\n[TABLE] {table_name}")
            print(f"  Rows: {row_count}")
            print(f"  Columns: {len(columns)}")
            print(f"\n  Column Schema:")
            for col_name, col_type in columns[:10]:  # Show first 10 columns
                print(f"    - {col_name}: {col_type}")
            if len(columns) > 10:
                print(f"    ... and {len(columns) - 10} more columns")
            
            if sample_result:
                print(f"\n  Sample Data (first row):")
                for i, (col_name, col_type) in enumerate(columns[:5]):
                    if i < len(sample_result[0]):
                        value = sample_result[0][i]
                        value_str = str(value)[:60] if value else "NULL"
                        print(f"    {col_name}: {value_str}")
            
        except Exception as e:
            print(f"\n[ERROR] Failed to check {table_name}: {e}")
    
    print("\n" + "="*70)
    print("Verification Complete")
    print("="*70 + "\n")

if __name__ == '__main__':
    verify_zoho_data()

