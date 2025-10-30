"""
Check if data was synced to ClickHouse
"""
from clickhouse_driver import Client
from db_utils import load_clickhouse_config

try:
    ch_conf = load_clickhouse_config()
    client = Client(
        host=ch_conf['host'],
        port=ch_conf['port'],
        user=ch_conf['user'],
        password=ch_conf['password']
    )
    
    print("="*60)
    print("Checking ClickHouse for synced data")
    print("="*60)
    
    # Check if table exists
    tables = client.execute("SHOW TABLES FROM test4")
    print(f"\nTables in test4 database: {[t[0] for t in tables]}")
    
    if ('crm_deals',) in tables:
        print("\n✅ Table 'crm_deals' exists!")
        
        # Get table structure
        print("\n📋 Table Structure:")
        columns = client.execute("DESCRIBE test4.crm_deals")
        for col_name, col_type, *_ in columns:
            print(f"   {col_name:30} {col_type}")
        
        # Count records
        count = client.execute("SELECT COUNT(*) FROM test4.crm_deals")[0][0]
        print(f"\n📊 Total Records: {count}")
        
        if count > 0:
            print("\n✅ DATA WAS SYNCED SUCCESSFULLY!")
            
            # Show sample records
            print("\n📝 Sample Records:")
            records = client.execute("""
                SELECT 
                    data_deal_name,
                    data_amount,
                    data_stage,
                    data_account_name,
                    _sync_timestamp
                FROM test4.crm_deals
                ORDER BY _sync_timestamp DESC
                LIMIT 10
            """)
            
            print("\n{:<30} {:<15} {:<20} {:<15}".format(
                "Deal Name", "Amount", "Stage", "Account"
            ))
            print("-" * 85)
            
            for deal_name, amount, stage, account, sync_time in records:
                print("{:<30} ${:<14,} {:<20} {:<15}".format(
                    deal_name[:28], amount, stage[:18], account[:13]
                ))
            
            # Group by stage
            print("\n📈 Deals by Stage:")
            stage_stats = client.execute("""
                SELECT 
                    data_stage,
                    COUNT(*) as count,
                    SUM(data_amount) as total_amount
                FROM test4.crm_deals
                GROUP BY data_stage
                ORDER BY count DESC
            """)
            
            for stage, count, total in stage_stats:
                print(f"   {stage:20} {count:3} deals    ${total:,}")
            
            print("\n" + "="*60)
            print("🎉 SUCCESS - API sync is working perfectly!")
            print("="*60)
        else:
            print("\n⚠️  Table exists but no records found")
    else:
        print("\n❌ Table 'crm_deals' not found")
        
except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()
