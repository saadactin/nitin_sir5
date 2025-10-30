"""
Quick test script to sync SSE API to ClickHouse
"""
from api_sync import sync_api_to_clickhouse
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

print("="*60)
print("Testing SSE API Sync to ClickHouse")
print("="*60)
print()
print("API URL: http://localhost:3000/api/crm/stream")
print("Target: ClickHouse test4 database")
print("Table: crm_deals (auto-created)")
print()
print("Press Ctrl+C to stop syncing...")
print("="*60)
print()

try:
    success = sync_api_to_clickhouse(
        api_url="http://localhost:3000/api/crm/stream",
        target_database="test4",
        target_table="crm_deals",
        is_sse=True,
        auto_create_table=True
    )
    
    if success:
        print("\n✓ Sync completed successfully")
    else:
        print("\n✗ Sync failed - check logs above")
        
except KeyboardInterrupt:
    print("\n\n🛑 Sync stopped by user")
except Exception as e:
    print(f"\n✗ Error: {e}")
