"""
Quick script to check if you have any sync data in the database.
Run this to see what data exists.
"""

import psycopg2
import yaml
from datetime import datetime

def check_database_data():
    # Read config
    try:
        with open('config/db_connections.yaml', 'r') as f:
            config = yaml.safe_load(f)
    except Exception as e:
        print(f"❌ Cannot read config file: {e}")
        return
    
    pg_config = config['postgresql']
    
    try:
        # Connect to database
        conn = psycopg2.connect(
            dbname=pg_config['database'],
            user=pg_config['username'],
            password=pg_config['password'],
            host=pg_config['host'],
            port=pg_config['port']
        )
        cursor = conn.cursor()
        
        print("\n" + "="*70)
        print("DATABASE DATA CHECK")
        print("="*70)
        
        # Check if schema exists
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name = 'metrics_sync_tables'
        """)
        schema_exists = cursor.fetchone()
        
        if not schema_exists:
            print("\n❌ Schema 'metrics_sync_tables' does not exist!")
            print("   Run: python create_database.py")
            return
        
        print("\n✓ Schema 'metrics_sync_tables' exists")
        
        # Check sync_history table
        try:
            cursor.execute("""
                SELECT COUNT(*) FROM metrics_sync_tables.sync_history
            """)
            sync_count = cursor.fetchone()[0]
            
            print(f"\n📊 SYNC HISTORY:")
            print(f"   Total syncs: {sync_count}")
            
            if sync_count > 0:
                # Get recent syncs
                cursor.execute("""
                    SELECT server_name, status, sync_date, 
                           EXTRACT(EPOCH FROM (end_time - sync_date)) as duration
                    FROM metrics_sync_tables.sync_history
                    ORDER BY sync_date DESC
                    LIMIT 5
                """)
                recent_syncs = cursor.fetchall()
                
                print("\n   Recent syncs:")
                for row in recent_syncs:
                    server, status, sync_date, duration = row
                    duration_str = f"{int(duration)}s" if duration else "N/A"
                    print(f"     • {server} - {status} - {sync_date.strftime('%Y-%m-%d %H:%M:%S')} - {duration_str}")
                
                # Get today's stats
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(CASE WHEN status = 'completed' THEN 1 END) as success,
                        COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed
                    FROM metrics_sync_tables.sync_history
                    WHERE DATE(sync_date) = CURRENT_DATE
                """)
                today_stats = cursor.fetchone()
                total_today, success_today, failed_today = today_stats
                
                print(f"\n   📅 Today's Statistics:")
                print(f"     Total: {total_today}")
                print(f"     Success: {success_today}")
                print(f"     Failed: {failed_today}")
                if total_today > 0:
                    success_rate = (success_today / total_today) * 100
                    print(f"     Success Rate: {success_rate:.1f}%")
            else:
                print("\n   ⚠️  No sync data found!")
                print("\n   💡 This is why your dashboard is empty.")
                print("   👉 Go to the homepage and click 'Sync Now' to run your first sync!")
        
        except Exception as e:
            print(f"\n❌ Error checking sync_history table: {e}")
            print("   The table might not exist yet.")
        
        # Check schedules table
        try:
            cursor.execute("""
                SELECT COUNT(*) FROM metrics_sync_tables.schedules
                WHERE status != 'deleted'
            """)
            schedule_count = cursor.fetchone()[0]
            
            print(f"\n📅 SCHEDULES:")
            print(f"   Active schedules: {schedule_count}")
            
            if schedule_count > 0:
                cursor.execute("""
                    SELECT server_name, job_type, status, last_run
                    FROM metrics_sync_tables.schedules
                    WHERE status != 'deleted'
                    ORDER BY created_at DESC
                """)
                schedules = cursor.fetchall()
                
                print("\n   Your schedules:")
                for row in schedules:
                    server, job_type, status, last_run = row
                    last_run_str = last_run.strftime('%Y-%m-%d %H:%M:%S') if last_run else "Not run yet"
                    print(f"     • {server} - {job_type} - {status} - Last: {last_run_str}")
        
        except Exception as e:
            print(f"\n❌ Error checking schedules table: {e}")
        
        # Check users table
        try:
            cursor.execute("""
                SELECT COUNT(*) FROM metrics_sync_tables.users
            """)
            user_count = cursor.fetchone()[0]
            
            print(f"\n👤 USERS:")
            print(f"   Total users: {user_count}")
            
            if user_count > 0:
                cursor.execute("""
                    SELECT username, role, created_at
                    FROM metrics_sync_tables.users
                    ORDER BY created_at DESC
                """)
                users = cursor.fetchall()
                
                print("\n   Registered users:")
                for row in users:
                    username, role, created_at = row
                    print(f"     • {username} ({role}) - Created: {created_at.strftime('%Y-%m-%d %H:%M:%S')}")
        
        except Exception as e:
            print(f"\n❌ Error checking users table: {e}")
        
        print("\n" + "="*70)
        print("SUMMARY")
        print("="*70)
        
        if sync_count == 0:
            print("\n🎯 NEXT STEPS:")
            print("   1. Make sure your SQL Servers are running")
            print("   2. Start the app: python app.py")
            print("   3. Login at: http://127.0.0.1:5001")
            print("   4. Click 'Sync Now' on any server")
            print("   5. Go to 'Advanced Analytics' to see your data!")
        else:
            print("\n✅ You have sync data! The Advanced Analytics dashboard should show it.")
            print("   If dashboard is still empty, try:")
            print("   1. Refresh the page")
            print("   2. Check browser console for errors (F12)")
            print("   3. Restart the app")
        
        print("\n")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\n❌ Database connection error: {e}")
        print("\n💡 Make sure:")
        print("   1. PostgreSQL is running")
        print("   2. Database exists: python create_database.py")
        print("   3. Credentials in config/db_connections.yaml are correct")

if __name__ == "__main__":
    check_database_data()
