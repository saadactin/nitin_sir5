"""
Check HANA Import Status
Shows how many tables have been imported and total row counts
"""

import sys
import os
from db_utils import load_clickhouse_config
from clickhouse_driver import Client

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def check_import_status():
    """Check the status of HANA import"""
    try:
        config = load_clickhouse_config()
        client = Client(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            password=config['password']
        )
        
        # Get all tables in JARVIS_DB
        tables = client.execute('SHOW TABLES FROM JARVIS_DB')
        
        print("=" * 80)
        print("HANA Import Status")
        print("=" * 80)
        print(f"Total tables in JARVIS_DB: {len(tables)}")
        print()
        
        # Get row counts for tables that look like HANA imports
        hana_tables = [t[0] for t in tables if not t[0].startswith('crm_') and t[0] not in ['example_table', 'test_table']]
        
        if hana_tables:
            print(f"HANA-imported tables: {len(hana_tables)}")
            print()
            print("Top 20 tables by row count:")
            print("-" * 80)
            
            table_rows = []
            for table in hana_tables[:50]:  # Check first 50
                try:
                    count = client.execute(f'SELECT count() FROM JARVIS_DB.{table}')[0][0]
                    table_rows.append((table, count))
                except Exception as e:
                    table_rows.append((table, f"Error: {str(e)[:50]}"))
            
            # Sort by row count
            table_rows.sort(key=lambda x: x[1] if isinstance(x[1], int) else 0, reverse=True)
            
            for table, count in table_rows[:20]:
                if isinstance(count, int):
                    print(f"  {table:<50} {count:>15,} rows")
                else:
                    print(f"  {table:<50} {count}")
            
            total_rows = sum(c for _, c in table_rows if isinstance(c, int))
            print("-" * 80)
            print(f"  Total rows in checked tables: {total_rows:,}")
        
        print()
        print("=" * 80)
        print("Import is still running in the background...")
        print("Check again later or view hana_import.log for details")
        print("=" * 80)
        
    except Exception as e:
        print(f"Error checking status: {e}")

if __name__ == '__main__':
    check_import_status()


