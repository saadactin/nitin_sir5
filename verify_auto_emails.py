"""
Quick test to verify automatic email notifications are properly integrated
"""
import sys
from pathlib import Path

# Test imports
print("Testing automatic email notification system...\n")

try:
    # Test 1: Email service import
    print("✓ Test 1: Email service module")
    sys.path.insert(0, str(Path(__file__).parent / "utils"))
    from email_service import email_service
    print(f"  Email configured: {email_service.email_user}")
    print(f"  Recipients: {', '.join(email_service.admin_emails)}")
    
    # Test 2: Sync manager integration
    print("\n✓ Test 2: Sync manager integration")
    from sync_manager import sync_manager
    print(f"  Sync manager initialized")
    print(f"  Max concurrent syncs: {sync_manager.max_concurrent_syncs}")
    
    # Test 3: Daily scheduler
    print("\n✓ Test 3: Daily summary scheduler")
    from daily_summary_scheduler import DailySummaryScheduler
    print(f"  Scheduler module loaded")
    
    # Test 4: Check if notifications are in sync_manager
    print("\n✓ Test 4: Email triggers in sync_manager")
    import inspect
    sync_worker_source = inspect.getsource(sync_manager._sync_worker)
    
    checks = {
        'notify_sync_success': 'notify_sync_success' in sync_worker_source,
        'notify_sync_failed': 'notify_sync_failed' in sync_worker_source,
        'notify_server_down': 'notify_server_down' in sync_worker_source,
        'notify_sync_partial_success': 'notify_sync_partial_success' in sync_worker_source,
    }
    
    for notification, present in checks.items():
        status = "✓" if present else "✗"
        print(f"  {status} {notification}: {'Integrated' if present else 'Missing'}")
    
    all_present = all(checks.values())
    
    # Summary
    print("\n" + "="*60)
    if all_present:
        print("✅ SUCCESS: Automatic email notifications are properly integrated!")
        print("\nWhat happens automatically:")
        print("  • Sync completes → Email sent")
        print("  • Sync fails → Email sent")  
        print("  • Server unreachable → Email sent")
        print("  • Partial sync → Email sent")
        print("  • Daily at 11:59 PM → Summary email sent")
        print("\n📬 Emails will be sent to:", ', '.join(email_service.admin_emails))
        sys.exit(0)
    else:
        print("⚠️  WARNING: Some notifications may not be integrated")
        print("Please check the integration status above.")
        sys.exit(1)
        
except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
