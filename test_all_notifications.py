"""
Comprehensive test for all high-priority email notifications
Tests: Sync Failed, Sync Success, Server Down, Row Mismatch, Partial Sync, Daily Summary
"""
import os
import sys
from pathlib import Path
from datetime import datetime

# Load .env file
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent / '.env'
    load_dotenv(env_path)
    print(f"✓ Loaded .env from: {env_path}\n")
except Exception as e:
    print(f"Warning: Could not load .env: {e}\n")

# Add utils to path and import email service
sys.path.insert(0, str(Path(__file__).parent / "utils"))
from email_service import email_service

def print_header(title):
    print("\n" + "="*70)
    print(f"  {title}")
    print("="*70)

def print_result(result, test_num, total):
    status = "✅ SUCCESS" if result.success else "❌ FAILED"
    print(f"\n[{test_num}/{total}] {status}")
    print(f"  Subject: {result.subject}")
    print(f"  To: {', '.join(result.recipients[:2])}{'...' if len(result.recipients) > 2 else ''}")
    print(f"  Attempts: {result.attempts}")
    if result.error:
        print(f"  Error: {result.error}")
    return result.success

def test_1_sync_failed():
    """Test 1: Sync Failed Notification"""
    print_header("TEST 1: SYNC FAILED")
    result = email_service.notify_sync_failed(
        server_name="PROD-SQL-01",
        error_message="Connection timeout after 30 seconds.\nServer: 192.168.1.100:1433\nDatabase: CustomerDB\n\nStack trace:\n  at pyodbc.connect()\n  at sync_manager.py:145"
    )
    return result

def test_2_sync_success():
    """Test 2: Sync Completed Successfully"""
    print_header("TEST 2: SYNC COMPLETED")
    result = email_service.notify_sync_success(
        server_name="PROD-SQL-01",
        summary="✓ Successfully synced 23 tables\n✓ Total rows synced: 1,245,678\n✓ Duration: 3m 42s\n✓ No errors encountered"
    )
    return result

def test_3_server_down():
    """Test 3: Server Down Alert"""
    print_header("TEST 3: SERVER DOWN")
    result = email_service.notify_server_down(
        server_name="BACKUP-SQL-02",
        error_message="Server is not responding to connection attempts.\n\nLast successful connection: 2 hours ago\nPing status: Failed\nPort 1433: Closed\n\nAction required: Check server status and network connectivity."
    )
    return result

def test_4_row_count_mismatch():
    """Test 4: Large Row Count Mismatch"""
    print_header("TEST 4: ROW COUNT MISMATCH")
    result = email_service.notify_row_count_mismatch(
        server_name="PROD-SQL-01",
        table_name="SalesDB.dbo.Orders",
        source_rows=150000,
        target_rows=135000,
        percentage_diff=-10.0  # 10% fewer rows in target
    )
    return result

def test_5_partial_sync():
    """Test 5: Sync Partial Success"""
    print_header("TEST 5: PARTIAL SYNC")
    result = email_service.notify_sync_partial_success(
        server_name="PROD-SQL-01",
        total_tables=20,
        success_count=17,
        failed_tables=[
            "SalesDB.dbo.LargeTransactions",
            "SalesDB.dbo.AuditLog",
            "InventoryDB.dbo.StockMovements"
        ],
        error_summary="3 tables failed due to:\n- Timeout errors (2 tables)\n- Permission denied (1 table)\n\nRecommendation: Retry failed tables with increased timeout."
    )
    return result

def test_6_daily_summary():
    """Test 6: Daily Sync Summary"""
    print_header("TEST 6: DAILY SUMMARY")
    today = datetime.now().strftime("%Y-%m-%d")
    
    servers_summary = [
        {"server": "PROD-SQL-01", "syncs": 4, "status": "success"},
        {"server": "PROD-SQL-02", "syncs": 3, "status": "success"},
        {"server": "BACKUP-SQL-01", "syncs": 2, "status": "failed"},
        {"server": "DEV-SQL-01", "syncs": 5, "status": "success"}
    ]
    
    top_errors = [
        "Connection timeout - occurred 3 times",
        "Permission denied on SalesDB - occurred 2 times",
        "Disk space low warning - occurred 1 time"
    ]
    
    result = email_service.notify_daily_sync_summary(
        date=today,
        total_syncs=14,
        successful_syncs=12,
        failed_syncs=2,
        total_rows_synced=3456789,
        servers_summary=servers_summary,
        top_errors=top_errors
    )
    return result

def main():
    print("\n" + "╔" + "═"*68 + "╗")
    print("║" + " "*15 + "HIGH PRIORITY EMAIL NOTIFICATIONS TEST" + " "*14 + "║")
    print("╚" + "═"*68 + "╝")
    
    # Display configuration
    print(f"\n📧 Email Configuration:")
    print(f"   From: {email_service.email_user or 'NOT SET'}")
    print(f"   To: {', '.join(email_service.admin_emails) if email_service.admin_emails else 'NOT SET'}")
    print(f"   Host: {email_service.email_host}:{email_service.email_port}")
    
    if not email_service.email_user or not email_service.email_password:
        print("\n❌ ERROR: Email credentials not configured!")
        return False
    
    print("\n" + "─"*70)
    print("This will send 6 test emails covering all high-priority scenarios:")
    print("  1. ✗ Sync Failed")
    print("  2. ✓ Sync Completed")
    print("  3. ⚠️  Server Down")
    print("  4. ⚠️  Row Count Mismatch")
    print("  5. ⚠️  Partial Sync Success")
    print("  6. 📊 Daily Summary Report")
    print("─"*70)
    
    proceed = input("\nSend all test emails? (yes/no): ").strip().lower()
    if proceed not in ['yes', 'y']:
        print("Test cancelled.")
        return False
    
    # Run all tests
    results = []
    total_tests = 6
    
    try:
        print("\n" + "🚀 Starting tests...\n")
        
        result = test_1_sync_failed()
        results.append(("Sync Failed", print_result(result, 1, total_tests)))
        input("\nPress Enter for next test...")
        
        result = test_2_sync_success()
        results.append(("Sync Success", print_result(result, 2, total_tests)))
        input("\nPress Enter for next test...")
        
        result = test_3_server_down()
        results.append(("Server Down", print_result(result, 3, total_tests)))
        input("\nPress Enter for next test...")
        
        result = test_4_row_count_mismatch()
        results.append(("Row Count Mismatch", print_result(result, 4, total_tests)))
        input("\nPress Enter for next test...")
        
        result = test_5_partial_sync()
        results.append(("Partial Sync", print_result(result, 5, total_tests)))
        input("\nPress Enter for next test...")
        
        result = test_6_daily_summary()
        results.append(("Daily Summary", print_result(result, 6, total_tests)))
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
        return False
    except Exception as e:
        print(f"\n❌ Error during tests: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Final Summary
    print_header("FINAL SUMMARY")
    passed = sum(1 for _, success in results if success)
    
    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"  {status} - {test_name}")
    
    print(f"\n  Total: {passed}/{total_tests} tests passed")
    print(f"  Success Rate: {(passed/total_tests*100):.1f}%")
    
    if passed == total_tests:
        print("\n  🎉 All high-priority notifications working correctly!")
        print(f"\n  📬 Check your inbox at: {', '.join(email_service.admin_emails)}")
    else:
        print(f"\n  ⚠️  {total_tests - passed} test(s) failed")
    
    print(f"\n  📄 Delivery log: {email_service._delivery_log_path}")
    print("="*70 + "\n")
    
    return passed == total_tests

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
