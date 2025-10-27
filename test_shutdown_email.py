"""
Test Shutdown Email
This script simulates what happens when you press Ctrl+C on the Flask server
"""

import os
import sys
from datetime import datetime
from pathlib import Path

# Add project directory to path
sys.path.insert(0, str(Path(__file__).parent))

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✓ Environment variables loaded")
except Exception as e:
    print(f"Warning: {e}")

print("\n" + "="*60)
print("  TESTING: Flask Shutdown Email Notification")
print("="*60 + "\n")

# Import email service
from utils.email_service import email_service

print("Email Configuration:")
print(f"  User: {email_service.email_user}")
print(f"  Admin Emails: {email_service.admin_emails}")
print()

# Simulate shutdown
print("-" * 60)
print("Simulating Flask server shutdown (Ctrl+C)...")
print("-" * 60)
print()

timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
shutdown_message = f"""Flask Application Server is shutting down.

Timestamp: {timestamp}
Shutdown Reason: Manual shutdown (Ctrl+C)
Server URL: http://127.0.0.1:5001

The Flask server has been stopped.

Possible reasons:
- Manual shutdown (Ctrl+C pressed)
- Application error or crash
- System shutdown
- Process terminated

Action Required: Restart the Flask application server if this was not intentional.

To restart:
1. Open PowerShell in project directory
2. Run: .\\myenv1\\Scripts\\python.exe app.py
"""

print("Sending shutdown notification email...")
print()

result = email_service.notify_server_down(
    server_name="Flask Application Server",
    error_message=shutdown_message
)

print(f"Result: {'✓ SUCCESS' if result.success else '✗ FAILED'}")

if result.success:
    print(f"  Recipients: {result.recipients}")
    print(f"  Attempts: {result.attempts}")
    print()
    print("="*60)
    print("  ✓ SHUTDOWN EMAIL SENT SUCCESSFULLY!")
    print("="*60)
    print()
    print("Check your email at:")
    for email in result.recipients:
        print(f"  - {email}")
    print()
    print("Subject: [ALERT][SERVER DOWN] Flask Application Server")
else:
    print(f"  Error: {result.error}")
    print()
    print("="*60)
    print("  ✗ FAILED TO SEND SHUTDOWN EMAIL")
    print("="*60)

print()
