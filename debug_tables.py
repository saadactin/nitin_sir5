from sync_summary import get_detailed_table_comparison

print("Testing detailed table comparison for server1...")
result = get_detailed_table_comparison('server1')

if 'error' in result:
    print('Error:', result['error'])
else:
    print(f"SQL Server tables: {result.get('sql_server_tables', 0)}")
    print(f"PostgreSQL tables: {result.get('postgresql_tables', 0)}")
    print(f"Total comparisons: {result.get('summary', {}).get('total_comparisons', 0)}")
    print(f"Synced tables: {result.get('summary', {}).get('synced_tables', 0)}")
    print(f"Incomplete tables: {result.get('summary', {}).get('incomplete_tables', 0)}")
    
    print("\nTable comparisons:")
    comparisons = result.get('table_comparison', [])
    for i, comp in enumerate(comparisons):
        print(f"{i+1}. SQL: {comp['sql_server']['database']}.{comp['sql_server']['table']} ({comp['sql_server']['rows']} rows)")
        print(f"   PG: {comp['postgresql']['schema']}.{comp['postgresql']['table']} ({comp['postgresql']['rows']} rows)")
        print(f"   Status: {comp['sync_status']}")
        print()