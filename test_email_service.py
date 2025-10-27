"""
Test script for email service verification
Run this to confirm email notifications are working correctly
"""
import os
import sys
from pathlib import Path

# Add utils directory to path
utils_path = Path(__file__).parent / "utils"
sys.path.insert(0, str(utils_path))

from email_service import email_service

def print_section(title):
    """Print a formatted section header"""
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)

def print_config():
    """Display current email configuration"""
    print_section("EMAIL CONFIGURATION")
    print(f"Host:         {email_service.email_host}")
    print(f"Port:         {email_service.email_port}")
    print(f"Use TLS:      {email_service.email_use_tls}")
    print(f"Use SSL:      {email_service.email_use_ssl}")
    print(f"User:         {email_service.email_user}")
    print(f"Password:     {'*' * len(email_service.email_password) if email_service.email_password else 'NOT SET'}")
    print(f"From:         {email_service.default_from}")
    print(f"Admin Emails: {', '.join(email_service.admin_emails) if email_service.admin_emails else 'NONE'}")
    print(f"Rate Limit:   {email_service._rate_limit_max_per_hour} emails/hour")
    
def test_sync_failed():
    """Test sync failed notification"""
    print_section("TEST 1: SYNC FAILED NOTIFICATION")
    result = email_service.notify_sync_failed(
        server_name="TEST-SQL-SERVER",
        error_message="Connection timeout after 30 seconds. Unable to reach SQL Server at 192.168.1.100:1433"
    )
    print_result(result)
    return result.success

def test_sync_success():
    """Test sync success notification"""
    print_section("TEST 2: SYNC SUCCESS NOTIFICATION")
    result = email_service.notify_sync_success(
        server_name="TEST-SQL-SERVER",
        summary="Successfully synced 15 tables with 45,678 total rows in 2m 34s"
    )
    print_result(result)
    return result.success

def test_server_down():
    """Test server down notification"""
    print_section("TEST 3: SERVER DOWN ALERT")
    result = email_service.notify_server_down(
        server_name="PRODUCTION-DB-01",
        error_message="Server not responding to ping. Last successful connection: 2 hours ago"
    )
    print_result(result)
    return result.success

def test_system_error():
    """Test system error notification"""
    print_section("TEST 4: SYSTEM ERROR NOTIFICATION")
    result = email_service.notify_system_error(
        title="Database Migration Failed",
        details="Error: Column 'user_id' does not exist in table 'orders'\nStacktrace: ...\nLocation: sync_manager.py:245"
    )
    print_result(result)
    return result.success

def test_custom_recipients():
    """Test with custom recipient list"""
    print_section("TEST 5: CUSTOM RECIPIENTS")
    custom_email = input("\nEnter a test email address (or press Enter to skip): ").strip()
    if not custom_email:
        print("Skipped - no email provided")
        return True
    
    result = email_service.notify_sync_success(
        server_name="CUSTOM-TEST",
        summary="This is a test email sent to a custom recipient",
        recipients=[custom_email]
    )
    print_result(result)
    return result.success

def print_result(result):
    """Print the result of an email send attempt"""
    print(f"\nResult: {'✓ SUCCESS' if result.success else '✗ FAILED'}")
    print(f"Alert Type:   {result.alert_type}")
    print(f"Subject:      {result.subject}")
    print(f"Recipients:   {', '.join(result.recipients or [])}")
    print(f"Attempts:     {result.attempts}")
    print(f"Timestamp:    {result.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    if result.error:
        print(f"Error:        {result.error}")

def run_all_tests():
    """Run all email tests"""
    print("\n" + "╔" + "═" * 58 + "╗")
    print("║" + " " * 15 + "EMAIL SERVICE TEST SUITE" + " " * 19 + "║")
    print("╚" + "═" * 58 + "╝")
    
    # Display configuration
    print_config()
    
    # Check if email is configured
    if not email_service.email_user or not email_service.email_password:
        print("\n❌ ERROR: Email credentials not configured!")
        print("Please set EMAIL_HOST_USER and EMAIL_HOST_PASSWORD in your .env file")
        return False
    
    if not email_service.admin_emails:
        print("\n⚠️  WARNING: No ADMIN_EMAILS configured!")
        print("Emails will be sent to:", email_service.email_user)
    
    print("\n" + "─" * 60)
    proceed = input("Ready to send test emails? (yes/no): ").strip().lower()
    if proceed not in ['yes', 'y']:
        print("Test cancelled by user")
        return False
    
    # Run tests
    results = []
    
    try:
        results.append(("Sync Failed", test_sync_failed()))
        input("\nPress Enter to continue to next test...")
        
        results.append(("Sync Success", test_sync_success()))
        input("\nPress Enter to continue to next test...")
        
        results.append(("Server Down", test_server_down()))
        input("\nPress Enter to continue to next test...")
        
        results.append(("System Error", test_system_error()))
        input("\nPress Enter to continue to next test...")
        
        results.append(("Custom Recipients", test_custom_recipients()))
        
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
        return False
    
    # Summary
    print_section("TEST SUMMARY")
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for test_name, success in results:
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"{status:8} - {test_name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! Email service is working correctly.")
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Check the errors above.")
    
    # Show delivery log location
    print(f"\nDelivery log: {email_service._delivery_log_path}")
    
    return passed == total

def quick_test():
    """Quick test - send one email only"""
    print_section("QUICK EMAIL TEST")
    print_config()
    
    if not email_service.email_user or not email_service.email_password:
        print("\n❌ ERROR: Email credentials not configured!")
        return False
    
    print("\nSending a quick test email...")
    result = email_service.notify_sync_success(
        server_name="QUICK-TEST",
        summary="This is a quick test to verify email delivery is working."
    )
    print_result(result)
    
    if result.success:
        print("\n✓ Email sent successfully!")
        print(f"Check your inbox at: {', '.join(result.recipients or [])}")
    else:
        print(f"\n✗ Failed to send email: {result.error}")
    
    return result.success

if __name__ == "__main__":
    print("\nEmail Service Test Options:")
    print("1. Quick Test (single email)")
    print("2. Full Test Suite (5 different email types)")
    print("3. Exit")
    
    choice = input("\nSelect option (1-3): ").strip()
    
    if choice == "1":
        success = quick_test()
    elif choice == "2":
        success = run_all_tests()
    else:
        print("Exiting...")
        sys.exit(0)
    
    sys.exit(0 if success else 1)
