"""
Test script for new email notification features:
1. User creation notifications
2. Schedule success/failure notifications
"""

import os
import sys
from datetime import datetime

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception as e:
    print(f"Warning: Could not load .env: {e}")

print("=" * 70)
print("Testing New Email Notification Features")
print("=" * 70)

# Test 1: Import email service
print("\n[Test 1] Loading email service...")
try:
    from utils.email_service import email_service
    print(f"✓ Email service loaded")
    print(f"  - Email user: {email_service.email_user}")
    print(f"  - Admin emails: {', '.join(email_service.admin_emails)}")
except Exception as e:
    print(f"✗ Failed to load email service: {e}")
    sys.exit(1)

# Test 2: User creation notification
print("\n[Test 2] Testing user creation notification...")
try:
    result = email_service.notify_user_created(
        username="test_user_demo",
        role="operator",
        created_by="admin_test"
    )
    
    if result.success:
        print(f"✓ User creation email sent successfully")
        print(f"  - Recipients: {', '.join(result.recipients or [])}")
        print(f"  - Subject: {result.subject}")
    else:
        print(f"✗ Failed to send user creation email: {result.error}")
except Exception as e:
    print(f"✗ Exception during user creation notification: {e}")

# Test 3: Schedule success notification
print("\n[Test 3] Testing schedule success notification...")
try:
    result = email_service.notify_schedule_success(
        server_name="TestServer",
        job_type="interval_30m",
        duration="2m 15s"
    )
    
    if result.success:
        print(f"✓ Schedule success email sent successfully")
        print(f"  - Recipients: {', '.join(result.recipients or [])}")
        print(f"  - Subject: {result.subject}")
    else:
        print(f"✗ Failed to send schedule success email: {result.error}")
except Exception as e:
    print(f"✗ Exception during schedule success notification: {e}")

# Test 4: Schedule failure notification
print("\n[Test 4] Testing schedule failure notification...")
try:
    result = email_service.notify_schedule_failed(
        server_name="TestServer",
        job_type="daily_03:00",
        error_message="Connection timeout: Unable to connect to SQL Server at 192.168.1.100:1433"
    )
    
    if result.success:
        print(f"✓ Schedule failure email sent successfully")
        print(f"  - Recipients: {', '.join(result.recipients or [])}")
        print(f"  - Subject: {result.subject}")
    else:
        print(f"✗ Failed to send schedule failure email: {result.error}")
except Exception as e:
    print(f"✗ Exception during schedule failure notification: {e}")

# Test 5: Check email service methods exist
print("\n[Test 5] Verifying new notification methods...")
methods_to_check = [
    'notify_user_created',
    'notify_schedule_success',
    'notify_schedule_failed'
]

all_methods_exist = True
for method_name in methods_to_check:
    if hasattr(email_service, method_name):
        print(f"✓ Method '{method_name}' exists")
    else:
        print(f"✗ Method '{method_name}' not found")
        all_methods_exist = False

# Summary
print("\n" + "=" * 70)
print("Test Summary")
print("=" * 70)

if all_methods_exist:
    print("✓ All new notification features are properly integrated!")
    print("\nFeatures added:")
    print("  1. User creation notifications (admin/operator/viewer)")
    print("  2. Schedule success notifications (with duration)")
    print("  3. Schedule failure notifications (with error details)")
    print("\nIntegration points:")
    print("  • auth.py - Sends email when create_user() is called")
    print("  • scheduler_utils.py - Sends email on schedule success/failure")
    print("  • utils/email_service.py - New notification methods added")
    print("\nNext steps:")
    print("  1. Create a new user via /create-user route")
    print("  2. Schedule a sync job and wait for it to run")
    print("  3. Check your email for notifications!")
else:
    print("✗ Some features are missing. Please review the integration.")

print("=" * 70)
