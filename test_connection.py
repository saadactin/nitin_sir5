import sys
sys.path.append('C:/Users/SaadSayyed/Desktop/nitin_sir_7/nitin_sir5')

from sync_summary import get_sqlserver_total_rows, get_sync_comparison

print('Testing SQL Server connection with Windows Auth...')
try:
    result = get_sqlserver_total_rows()
    print(f'SQL Server connection successful! Result: {result}')
except Exception as e:
    print(f'SQL Server connection failed: {e}')

print('\nTesting sync comparison...')
try:
    comparison = get_sync_comparison()
    print('Sync comparison successful!')
    print(f'SQL Server rows: {comparison["comparison"]["sql_total_rows"]}')
    print(f'PostgreSQL rows: {comparison["comparison"]["postgres_total_rows"]}')
    print(f'Difference: {comparison["comparison"]["difference"]}')
    print(f'Sync percentage: {comparison["comparison"]["sync_percentage"]}%')
    print(f'Status: {comparison["comparison"]["status"]}')
except Exception as e:
    print(f'Sync comparison failed: {e}')