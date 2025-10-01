#!/usr/bin/env python3
"""
Test script to verify schedule deletion works permanently
"""

import sys
import os
sys.path.append(os.path.dirname(__file__))

from scheduler_utils import get_schedules, delete_schedule, clean_deleted_schedules
from db_utils import get_pg_connection

def test_schedule_deletion():
    """Test that schedule deletion is permanent"""
    print("🧪 TESTING SCHEDULE DELETION")
    print("=" * 50)
    
    # Get current schedules
    schedules = get_schedules()
    print(f"Current schedules: {len(schedules)}")
    
    for schedule in schedules:
        print(f"  - {schedule['server']} / {schedule['type']} / {schedule['status']}")
    
    if not schedules:
        print("No schedules found to test deletion")
        return
    
    # Test deletion on first schedule
    test_schedule = schedules[0]
    server_name = test_schedule['server']
    job_type = test_schedule['type']
    
    print(f"\n🗑️ Testing deletion of: {server_name} / {job_type}")
    
    # Delete the schedule
    delete_schedule(server_name, job_type)
    
    # Check if it's gone from the list
    updated_schedules = get_schedules()
    remaining = [s for s in updated_schedules if s['server'] == server_name and s['type'] == job_type]
    
    if remaining:
        print(f"❌ DELETION FAILED: Schedule still appears in list")
        return False
    else:
        print(f"✅ SUCCESS: Schedule no longer appears in active list")
    
    # Check if it's marked as deleted in database
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT status FROM metrics_sync_tables.schedules 
        WHERE server_name = %s AND job_type = %s
    """, (server_name, job_type))
    result = cur.fetchone()
    cur.close()
    conn.close()
    
    if result and result[0] == 'deleted':
        print(f"✅ SUCCESS: Schedule marked as 'deleted' in database")
        return True
    else:
        print(f"❌ FAILURE: Schedule not properly marked as deleted")
        return False

def test_schedule_reloading():
    """Test that deleted schedules don't get reloaded"""
    print("\n🔄 TESTING SCHEDULE RELOADING")
    print("=" * 30)
    
    # Get schedules before reload
    before_schedules = get_schedules()
    before_count = len(before_schedules)
    
    # Reimport the module to trigger reload
    import importlib
    import scheduler_utils
    importlib.reload(scheduler_utils)
    
    # Get schedules after reload
    after_schedules = scheduler_utils.get_schedules()
    after_count = len(after_schedules)
    
    print(f"Schedules before reload: {before_count}")
    print(f"Schedules after reload: {after_count}")
    
    if before_count == after_count:
        print("✅ SUCCESS: No deleted schedules were reloaded")
        return True
    else:
        print("❌ FAILURE: Schedule count changed after reload")
        return False

if __name__ == "__main__":
    print("🔧 SCHEDULE DELETION TEST")
    print("=" * 60)
    
    deletion_test = test_schedule_deletion()
    reload_test = test_schedule_reloading()
    
    print("=" * 60)
    print("TEST RESULTS:")
    print(f"Deletion Test: {'✅ PASS' if deletion_test else '❌ FAIL'}")
    print(f"Reload Test: {'✅ PASS' if reload_test else '❌ FAIL'}")
    
    if deletion_test and reload_test:
        print("🎉 ALL TESTS PASSED! Schedule deletion is working properly.")
    else:
        print("⚠️ Some tests failed. Schedule deletion may not be permanent.")
    
    # Show cleanup option
    print(f"\n💡 TIP: To permanently remove deleted schedule records from database:")
    print(f"    python -c \"from scheduler_utils import clean_deleted_schedules; clean_deleted_schedules()\"")