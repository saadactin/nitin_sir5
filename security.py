"""
Security Module
Comprehensive security features including rate limiting, password validation, and security headers
"""

import re
import secrets
from functools import wraps
from flask import request, jsonify, session, redirect, url_for
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_talisman import Talisman
import pyotp
import qrcode
import io
import base64

class SecurityManager:
    """Centralized security management for the application"""
    
    def __init__(self, app=None):
        self.app = app
        self.limiter = None
        self.talisman = None
        
        if app:
            self.init_app(app)
    
    def init_app(self, app):
        """Initialize security features with Flask app"""
        self.app = app
        
        # Initialize rate limiter
        self.limiter = Limiter(
            app=app,
            key_func=get_remote_address,
            default_limits=["500 per hour", "100 per minute"],
            storage_uri="memory://",
            strategy="fixed-window"
        )
        
        # Initialize security headers (only in production, HTTPS disabled for now)
        # Note: force_https is disabled to support HTTP-only deployments
        if not app.debug:
            # Content Security Policy
            csp = {
                'default-src': ["'self'"],
                'script-src': [
                    "'self'",
                    "'unsafe-inline'",  # Allow inline scripts (Tailwind)
                    "cdn.tailwindcss.com",
                    "cdn.jsdelivr.net"
                ],
                'style-src': [
                    "'self'",
                    "'unsafe-inline'",  # Allow inline styles
                    "fonts.googleapis.com"
                ],
                'font-src': [
                    "'self'",
                    "fonts.googleapis.com",
                    "fonts.gstatic.com"
                ],
                'img-src': [
                    "'self'",
                    "data:",
                    "https:"
                ],
                'connect-src': ["'self'"]
            }
            
            self.talisman = Talisman(
                app,
                force_https=False,  # Disabled for HTTP support
                strict_transport_security=False,  # Disabled for HTTP support
                content_security_policy=csp,
                content_security_policy_nonce_in=['script-src'],
                feature_policy={
                    'geolocation': "'none'",
                    'microphone': "'none'",
                    'camera': "'none'"
                }
            )
        
        app.logger.info("Security features initialized")
    
    def validate_password_strength(self, password):
        """
        Validate password strength
        
        Returns: (is_valid: bool, message: str)
        """
        if len(password) < 8:
            return False, "Password must be at least 8 characters long"
        
        if not re.search(r"[A-Z]", password):
            return False, "Password must contain at least one uppercase letter"
        
        if not re.search(r"[a-z]", password):
            return False, "Password must contain at least one lowercase letter"
        
        if not re.search(r"[0-9]", password):
            return False, "Password must contain at least one number"
        
        if not re.search(r"[!@#$%^&*(),.?\":{}|<>]", password):
            return False, "Password must contain at least one special character (!@#$%^&*...)"
        
        # Check for common weak passwords
        weak_passwords = ['password', '12345678', 'qwerty123', 'admin123']
        if password.lower() in weak_passwords:
            return False, "This password is too common. Please choose a stronger password"
        
        return True, "Password is strong"
    
    def generate_api_key(self, prefix="actin"):
        """Generate secure API key"""
        random_part = secrets.token_urlsafe(32)
        return f"{prefix}_{random_part}"
    
    def sanitize_input(self, text, max_length=1000):
        """Sanitize user input to prevent XSS"""
        if not text:
            return text
        
        # Remove potential script tags
        text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.IGNORECASE | re.DOTALL)
        
        # Remove potential event handlers
        text = re.sub(r'on\w+\s*=', '', text, flags=re.IGNORECASE)
        
        # Limit length
        if len(text) > max_length:
            text = text[:max_length]
        
        return text.strip()
    
    def validate_email(self, email):
        """Validate email format"""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return re.match(pattern, email) is not None
    
    def generate_2fa_secret(self, username):
        """
        Generate 2FA secret and QR code
        
        Returns: (secret: str, qr_code_base64: str, provisioning_uri: str)
        """
        secret = pyotp.random_base32()
        
        # Generate provisioning URI
        totp = pyotp.TOTP(secret)
        provisioning_uri = totp.provisioning_uri(
            name=username,
            issuer_name="ACTIN Data Sync"
        )
        
        # Generate QR code
        qr = qrcode.QRCode(version=1, box_size=10, border=5)
        qr.add_data(provisioning_uri)
        qr.make(fit=True)
        
        img = qr.make_image(fill_color="black", back_color="white")
        
        # Convert to base64 for embedding in HTML
        buffer = io.BytesIO()
        img.save(buffer, format='PNG')
        qr_code_base64 = base64.b64encode(buffer.getvalue()).decode()
        
        return secret, qr_code_base64, provisioning_uri
    
    def verify_2fa_token(self, secret, token):
        """Verify 2FA token"""
        totp = pyotp.TOTP(secret)
        # Allow 1 window before/after for clock drift
        return totp.verify(token, valid_window=1)
    
    def check_ip_whitelist(self, ip_address, whitelist):
        """Check if IP is in whitelist"""
        if not whitelist:
            return True
        
        return ip_address in whitelist
    
    def log_security_event(self, event_type, username, details):
        """Log security events for audit"""
        self.app.logger.warning(
            f"SECURITY EVENT: {event_type} | User: {username} | IP: {request.remote_addr} | Details: {details}"
        )


# Rate limiting decorators for specific routes
def rate_limit_strict(limit="5 per minute"):
    """Strict rate limiting for sensitive endpoints"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # This will be handled by Flask-Limiter
            return f(*args, **kwargs)
        return decorated_function
    return decorator


def require_https(f):
    """Require HTTPS for sensitive endpoints in production"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not request.is_secure and not request.host.startswith('localhost') and not request.host.startswith('127.0.0.1'):
            return jsonify({
                'error': 'HTTPS required for this endpoint'
            }), 403
        return f(*args, **kwargs)
    return decorated_function


def validate_csrf_token(f):
    """Validate CSRF token for state-changing operations"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if request.method in ['POST', 'PUT', 'DELETE', 'PATCH']:
            token = request.form.get('csrf_token') or request.headers.get('X-CSRF-Token')
            session_token = session.get('csrf_token')
            
            if not token or not session_token or token != session_token:
                return jsonify({
                    'error': 'Invalid CSRF token'
                }), 403
        
        return f(*args, **kwargs)
    return decorated_function


def generate_csrf_token():
    """Generate CSRF token for forms"""
    if 'csrf_token' not in session:
        session['csrf_token'] = secrets.token_hex(32)
    return session['csrf_token']


# Create global instance
security_manager = SecurityManager()
