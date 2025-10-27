"""
Test Script: Server Down Email
This script tests the server down email notification
"""

import os
from pathlib import Path
from datetime import datetime

# Load environment variables
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent / '.env'
    load_dotenv(env_path)
    print(f"✓ Loaded .env from: {env_path}")
except Exception as e:
    print(f"Warning: Could not load .env: {e}")

print("\n" + "="*60)
print("  TEST: Server Down Email Notification")
print("="*60 + "\n")

# Import email service
from utils.email_service import email_service

# Display configuration
print("Configuration:")
print(f"  Email User: {email_service.email_user}")
print(f"  Admin Emails: {email_service.admin_emails}")
print(f"  SMTP Host: {email_service.email_host}:{email_service.email_port}")
print()

# Test 1: Flask Server Down
print("-" * 60)
print("TEST 1: Flask Application Server Down")
print("-" * 60)

timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
error_message = f"""Flask Application Server has stopped responding.

Timestamp: {timestamp}
Server URL: http://127.0.0.1:5001
Test Type: Manual Test

The Flask server was manually stopped or crashed.

This is a TEST email to verify the server down notification works.
"""

print("Sending server down email...")
print()

result = email_service.notify_server_down(
    server_name="Flask Application Server (TEST)",
    error_message=error_message
)

print(f"Result: {'✓ SUCCESS' if result.success else '✗ FAILED'}")
if result.success:
    print(f"  Recipients: {result.recipients}")
    print(f"  Attempts: {result.attempts}")
    print(f"  Alert Type: {result.alert_type}")
    print()
    print("✓ Server down email sent successfully!")
    print()
    print("Check your email at:")
    for email in result.recipients:
        print(f"  - {email}")
else:
    print(f"  Error: {result.error}")
    print(f"  Attempts: {result.attempts}")

print()
print("-" * 60)

# Test 2: SQL Server Down
print("\nTEST 2: SQL Server Connection Failed")
print("-" * 60)

error_message_sql = """SQL Server connection failed during sync operation.

Server: server1 (192.168.1.100)
Error: Login timeout expired
Connection timeout: 30 seconds

Possible causes:
- SQL Server service is stopped
- Network connectivity issues
- Firewall blocking connection
- Invalid credentials

This is a TEST email to verify SQL server down notifications work.
"""

print("Sending SQL server down email...")
print()

result2 = email_service.notify_server_down(
    server_name="server1 (SQL Server TEST)",
    error_message=error_message_sql
)

print(f"Result: {'✓ SUCCESS' if result2.success else '✗ FAILED'}")
if result2.success:
    print(f"  Recipients: {result2.recipients}")
    print(f"  Attempts: {result2.attempts}")
    print()
    print("✓ SQL server down email sent successfully!")
else:
    print(f"  Error: {result2.error}")

print()
print("="*60)
print("  TEST COMPLETE")
print("="*60)
print()

if result.success and result2.success:
    print("✓ ALL TESTS PASSED")
    print()
    print("You should have received 2 emails:")
    print("  1. [ALERT][SERVER DOWN] Flask Application Server (TEST)")
    print("  2. [ALERT][SERVER DOWN] server1 (SQL Server TEST)")
    print()
    print("Check your inbox at:")
    for email in email_service.admin_emails:
        print(f"  - {email}")
else:
    print("✗ SOME TESTS FAILED")
    print()
    print("Check the error messages above for details.")

print()
