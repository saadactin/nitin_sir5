"""
Quick verification script to check Zoho data in ClickHouse
Run this anytime to see what data has been synced
"""
from clickhouse_connect import get_client

# ClickHouse credentials
CLICKHOUSE_HOST = "74.225.251.123"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASS = "root"
CLICKHOUSE_DB = "test1"

def verify_data():
    """Verify Zoho data in ClickHouse"""
    print("\n" + "="*60)
    print("ZOHO CRM DATA VERIFICATION")
    print("="*60)
    print(f"Database: {CLICKHOUSE_DB}")
    print(f"Host: {CLICKHOUSE_HOST}")
    print("="*60)
    
    try:
        client = get_client(
            host=CLICKHOUSE_HOST,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASS,
            database=CLICKHOUSE_DB,
        )
        
        # List all zoho_* tables
        result = client.query(f"""
            SELECT name 
            FROM system.tables 
            WHERE database = '{CLICKHOUSE_DB}' 
            AND name LIKE 'zoho_%'
            ORDER BY name
        """)
        
        tables = [row[0] for row in result.result_rows]
        
        if not tables:
            print(f"\n⚠️  No Zoho tables found in database '{CLICKHOUSE_DB}'")
            return
        
        print(f"\n✅ Found {len(tables)} Zoho tables:\n")
        
        total_records = 0
        table_details = []
        
        for table in tables:
            try:
                # Get record count
                count_result = client.query(f"SELECT count() FROM {CLICKHOUSE_DB}.{table}")
                count = count_result.result_rows[0][0] if count_result.result_rows else 0
                total_records += count
                
                # Get column count
                desc_result = client.query(f"DESCRIBE TABLE {CLICKHOUSE_DB}.{table}")
                column_count = len(desc_result.result_rows) if desc_result.result_rows else 0
                
                # Get last sync time (most recent load_time)
                try:
                    time_result = client.query(f"""
                        SELECT max(load_time) 
                        FROM {CLICKHOUSE_DB}.{table}
                    """)
                    last_sync = time_result.result_rows[0][0] if time_result.result_rows else None
                except:
                    last_sync = None
                
                table_details.append({
                    'name': table,
                    'records': count,
                    'columns': column_count,
                    'last_sync': last_sync
                })
                
            except Exception as e:
                print(f"   ❌ {table}: Error - {str(e)}")
        
        # Display results in a nice table format
        print(f"{'Table Name':<30} {'Records':<12} {'Columns':<10} {'Last Sync':<20}")
        print("-" * 75)
        for detail in sorted(table_details, key=lambda x: x['records'], reverse=True):
            last_sync_str = str(detail['last_sync'])[:19] if detail['last_sync'] else 'N/A'
            print(f"{detail['name']:<30} {detail['records']:<12} {detail['columns']:<10} {last_sync_str:<20}")
        
        print("-" * 75)
        print(f"{'TOTAL':<30} {total_records:<12} {'':<10} {'':<20}")
        
        # Show sample data from the largest table
        if table_details:
            largest = max(table_details, key=lambda x: x['records'])
            if largest['records'] > 0:
                print(f"\n📋 Sample data from {largest['name']} (first record):")
                try:
                    sample = client.query(f"SELECT * FROM {CLICKHOUSE_DB}.{largest['name']} LIMIT 1")
                    if sample.result_rows:
                        columns = sample.column_names
                        values = sample.result_rows[0]
                        print("\n   Fields:")
                        for col, val in zip(columns[:10], values[:10]):  # Show first 10
                            val_str = str(val)[:50] if val else 'NULL'
                            if len(str(val)) > 50:
                                val_str += '...'
                            print(f"     {col}: {val_str}")
                        if len(columns) > 10:
                            print(f"     ... and {len(columns) - 10} more fields")
                except Exception as e:
                    print(f"   Error reading sample: {str(e)}")
        
        print("\n" + "="*60)
        print("✅ Verification Complete")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    verify_data()

