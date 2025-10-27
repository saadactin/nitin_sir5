"""
Production Database Optimization
Adds indexes to sync_history table for fast queries with 100+ servers and millions of records
"""
import psycopg2
import sys

def optimize_database():
    """Add indexes to sync_history table for production performance"""
    
    try:
        print("=" * 80)
        print("PRODUCTION DATABASE OPTIMIZATION")
        print("=" * 80)
        print("\nConnecting to PostgreSQL...")
        
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="test1",
            user="migration_user",
            password="StrongPassword123"
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        print("✓ Connected successfully\n")
        
        # Check existing indexes
        print("Checking existing indexes...")
        cursor.execute("""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = 'metrics_sync_tables'
            AND tablename = 'sync_history'
        """)
        existing_indexes = cursor.fetchall()
        
        print(f"Found {len(existing_indexes)} existing indexes:")
        for idx_name, idx_def in existing_indexes:
            print(f"  - {idx_name}")
        
        # Indexes to create for production performance
        indexes = [
            {
                'name': 'idx_sync_history_server_name',
                'sql': """
                    CREATE INDEX IF NOT EXISTS idx_sync_history_server_name 
                    ON metrics_sync_tables.sync_history(server_name)
                """,
                'purpose': 'Fast server-specific queries'
            },
            {
                'name': 'idx_sync_history_sync_time',
                'sql': """
                    CREATE INDEX IF NOT EXISTS idx_sync_history_sync_time 
                    ON metrics_sync_tables.sync_history(sync_time DESC)
                """,
                'purpose': 'Fast time-based queries and sorting'
            },
            {
                'name': 'idx_sync_history_status',
                'sql': """
                    CREATE INDEX IF NOT EXISTS idx_sync_history_status 
                    ON metrics_sync_tables.sync_history(status)
                """,
                'purpose': 'Fast status filtering (success/failed/started)'
            },
            {
                'name': 'idx_sync_history_sync_time_status',
                'sql': """
                    CREATE INDEX IF NOT EXISTS idx_sync_history_sync_time_status 
                    ON metrics_sync_tables.sync_history(sync_time DESC, status)
                """,
                'purpose': 'Composite index for date+status queries'
            },
            {
                'name': 'idx_sync_history_server_time',
                'sql': """
                    CREATE INDEX IF NOT EXISTS idx_sync_history_server_time 
                    ON metrics_sync_tables.sync_history(server_name, sync_time DESC)
                """,
                'purpose': 'Fast latest sync per server queries'
            }
        ]
        
        print("\n" + "=" * 80)
        print("CREATING PRODUCTION INDEXES")
        print("=" * 80)
        
        for idx in indexes:
            print(f"\n📊 Creating: {idx['name']}")
            print(f"   Purpose: {idx['purpose']}")
            
            try:
                cursor.execute(idx['sql'])
                print(f"   ✅ Success!")
            except Exception as e:
                print(f"   ⚠️  Warning: {e}")
        
        # Add table statistics update
        print("\n" + "=" * 80)
        print("UPDATING TABLE STATISTICS")
        print("=" * 80)
        
        cursor.execute("ANALYZE metrics_sync_tables.sync_history")
        print("✅ Table statistics updated for query planner")
        
        # Show final index list
        print("\n" + "=" * 80)
        print("FINAL INDEX CONFIGURATION")
        print("=" * 80)
        
        cursor.execute("""
            SELECT 
                indexname,
                pg_size_pretty(pg_relation_size(schemaname || '.' || indexname)) as size
            FROM pg_indexes
            WHERE schemaname = 'metrics_sync_tables'
            AND tablename = 'sync_history'
            ORDER BY indexname
        """)
        
        final_indexes = cursor.fetchall()
        print(f"\nTotal indexes: {len(final_indexes)}")
        for idx_name, idx_size in final_indexes:
            print(f"  - {idx_name:45s} | Size: {idx_size}")
        
        # Show table size
        cursor.execute("""
            SELECT 
                pg_size_pretty(pg_total_relation_size('metrics_sync_tables.sync_history')) as total_size,
                pg_size_pretty(pg_relation_size('metrics_sync_tables.sync_history')) as table_size
        """)
        total_size, table_size = cursor.fetchone()
        
        print(f"\n📈 Table size: {table_size}")
        print(f"📊 Total size (with indexes): {total_size}")
        
        cursor.close()
        conn.close()
        
        print("\n" + "=" * 80)
        print("✅ OPTIMIZATION COMPLETE!")
        print("=" * 80)
        print("\nYour database is now optimized for production with 100+ servers.")
        print("Queries will be significantly faster even with millions of sync records.")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = optimize_database()
    sys.exit(0 if success else 1)
