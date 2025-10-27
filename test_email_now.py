"""
Simple email test that loads .env and sends a test email
"""
import os
import sys
from pathlib import Path

# Load .env file
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent / '.env'
    load_dotenv(env_path)
    print(f"✓ Loaded .env from: {env_path}")
except Exception as e:
    print(f"Warning: Could not load .env: {e}")

# Add utils to path and import email service
sys.path.insert(0, str(Path(__file__).parent / "utils"))
from email_service import email_service

def main():
    print("\n" + "="*60)
    print("  EMAIL SERVICE QUICK TEST")
    print("="*60)
    
    # Display configuration
    print(f"\n📧 Configuration:")
    print(f"   Host:     {email_service.email_host}")
    print(f"   Port:     {email_service.email_port}")
    print(f"   User:     {email_service.email_user or 'NOT SET'}")
    print(f"   Password: {'*' * 16 if email_service.email_password else 'NOT SET'}")
    print(f"   From:     {email_service.default_from}")
    print(f"   To:       {', '.join(email_service.admin_emails) if email_service.admin_emails else 'NOT SET'}")
    
    # Check configuration
    if not email_service.email_user or not email_service.email_password:
        print("\n❌ ERROR: Email credentials not found!")
        print("\nDebug info:")
        print(f"   EMAIL_HOST_USER from env: {os.getenv('EMAIL_HOST_USER')}")
        print(f"   EMAIL_HOST_PASSWORD from env: {'SET' if os.getenv('EMAIL_HOST_PASSWORD') else 'NOT SET'}")
        print(f"   SMTP_USER from env: {os.getenv('SMTP_USER')}")
        print(f"   SMTP_PASS from env: {'SET' if os.getenv('SMTP_PASS') else 'NOT SET'}")
        return False
    
    if not email_service.admin_emails:
        print("\n⚠️  Warning: No ADMIN_EMAILS configured, using sender email")
    
    # Send test email
    print("\n📤 Sending test email...")
    result = email_service.notify_sync_success(
        server_name="EMAIL-TEST",
        summary="This is a test email to verify the email service is working correctly.\n\n"
                "If you receive this, your email configuration is set up properly!"
    )
    
    print("\n" + "="*60)
    if result.success:
        print("✅ SUCCESS - Email sent!")
        print(f"   To: {', '.join(result.recipients)}")
        print(f"   Subject: {result.subject}")
        print(f"   Attempts: {result.attempts}")
        print(f"\n📬 Check your inbox!")
    else:
        print("❌ FAILED - Email not sent")
        print(f"   Error: {result.error}")
        print(f"   Attempts: {result.attempts}")
    print("="*60)
    
    return result.success

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
