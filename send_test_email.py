from dotenv import load_dotenv
load_dotenv()
import os
from utils.email_service import email_service

admins = [a.strip() for a in (os.getenv('ADMIN_EMAILS') or '').split(',') if a.strip()]
print(f"Sending test email to: {admins}")

res = email_service.notify_system_error(
    '[TEST] Gmail SMTP verification', 
    'This is a test email from the SQL-PG sync app.',
    recipients=admins
)

print(f"Success: {res.success}")
print(f"Error: {res.error}")
print(f"Attempts: {res.attempts}")
print(f"Recipients: {admins}")








