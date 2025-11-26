"""
URL Validation Module
Validates and sanitizes URLs to prevent SSRF (Server-Side Request Forgery) attacks.
"""

import re
import logging
from urllib.parse import urlparse, urlunparse
from ipaddress import ip_address, IPv4Address, IPv6Address
from typing import Tuple, Optional

logger = logging.getLogger(__name__)

# Blocked private/internal IP ranges
PRIVATE_IP_RANGES = [
    # IPv4 private ranges
    ('10.0.0.0', '10.255.255.255'),
    ('172.16.0.0', '172.31.255.255'),
    ('192.168.0.0', '192.168.255.255'),
    ('127.0.0.0', '127.255.255.255'),
    ('169.254.0.0', '169.254.255.255'),  # Link-local
    ('0.0.0.0', '0.255.255.255'),
    # IPv6 private ranges
    ('::1', '::1'),  # localhost
    ('fc00::', 'fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'),  # Unique local
    ('fe80::', 'febf:ffff:ffff:ffff:ffff:ffff:ffff:ffff'),  # Link-local
]

# Allowed URL schemes
ALLOWED_SCHEMES = {'http', 'https'}

# Blocked hostname patterns (case-insensitive)
BLOCKED_HOSTNAME_PATTERNS = [
    r'^localhost$',
    r'^127\.',
    r'^0\.0\.0\.0$',
    r'^::1$',
    r'^\[::1\]$',
]


def is_private_ip(ip_str: str) -> bool:
    """
    Check if an IP address is in a private/internal range.
    
    Args:
        ip_str: IP address string (IPv4 or IPv6)
        
    Returns:
        True if IP is private/internal, False otherwise
    """
    try:
        ip = ip_address(ip_str)
        
        # Check IPv4 private ranges
        if isinstance(ip, IPv4Address):
            ip_int = int(ip)
            for start_str, end_str in PRIVATE_IP_RANGES:
                try:
                    start_ip = ip_address(start_str)
                    end_ip = ip_address(end_str)
                    if isinstance(start_ip, IPv4Address) and isinstance(end_ip, IPv4Address):
                        if int(start_ip) <= ip_int <= int(end_ip):
                            return True
                except ValueError:
                    continue
        
        # Check IPv6 private ranges
        elif isinstance(ip, IPv6Address):
            ip_int = int(ip)
            for start_str, end_str in PRIVATE_IP_RANGES:
                try:
                    start_ip = ip_address(start_str)
                    end_ip = ip_address(end_str)
                    if isinstance(start_ip, IPv6Address) and isinstance(end_ip, IPv6Address):
                        if int(start_ip) <= ip_int <= int(end_ip):
                            return True
                except ValueError:
                    continue
        
        return False
    except ValueError:
        # Not a valid IP address
        return False


def is_blocked_hostname(hostname: str) -> bool:
    """
    Check if a hostname matches blocked patterns.
    
    Args:
        hostname: Hostname to check
        
    Returns:
        True if hostname is blocked, False otherwise
    """
    hostname_lower = hostname.lower()
    for pattern in BLOCKED_HOSTNAME_PATTERNS:
        if re.match(pattern, hostname_lower):
            return True
    return False


def validate_url(url: str, allow_private_ips: bool = False) -> Tuple[bool, Optional[str]]:
    """
    Validate a URL to prevent SSRF attacks.
    
    Args:
        url: URL string to validate
        allow_private_ips: If True, allow private/internal IPs (default: False)
        
    Returns:
        Tuple of (is_valid, error_message)
        - is_valid: True if URL is safe, False otherwise
        - error_message: Error description if invalid, None if valid
    """
    if not url or not isinstance(url, str):
        return False, "URL must be a non-empty string"
    
    url = url.strip()
    if not url:
        return False, "URL cannot be empty"
    
    # Parse URL
    try:
        parsed = urlparse(url)
    except Exception as e:
        return False, f"Invalid URL format: {str(e)}"
    
    # Check scheme
    scheme = parsed.scheme.lower()
    if not scheme:
        return False, "URL must include a scheme (http:// or https://)"
    
    if scheme not in ALLOWED_SCHEMES:
        return False, f"URL scheme must be http:// or https://, got: {scheme}://"
    
    # Check hostname
    hostname = parsed.hostname
    if not hostname:
        return False, "URL must include a hostname"
    
    # Remove brackets from IPv6 addresses
    if hostname.startswith('[') and hostname.endswith(']'):
        hostname = hostname[1:-1]
    
    # Check for blocked hostname patterns
    if is_blocked_hostname(hostname):
        return False, f"Hostname '{hostname}' is not allowed (localhost/internal addresses blocked)"
    
    # Check if hostname is an IP address
    try:
        ip = ip_address(hostname)
        # Check if it's a private IP
        if is_private_ip(hostname):
            if not allow_private_ips:
                return False, f"Private/internal IP addresses are not allowed: {hostname}"
    except ValueError:
        # Not an IP address, check if it's a valid hostname
        # Allow valid hostnames (contains at least one dot or is a valid domain)
        if not re.match(r'^[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?)*$', hostname):
            return False, f"Invalid hostname format: {hostname}"
    
    # Check for suspicious characters in path/query
    if parsed.path:
        # Block file:// protocol attempts in path
        if 'file://' in parsed.path.lower() or '://' in parsed.path:
            return False, "URL path contains suspicious protocol indicators"
    
    # Check for suspicious query parameters
    if parsed.query:
        # Block attempts to inject protocols in query
        if '://' in parsed.query or 'file://' in parsed.query.lower():
            return False, "URL query contains suspicious protocol indicators"
    
    # URL is valid
    return True, None


def sanitize_url(url: str) -> Optional[str]:
    """
    Sanitize a URL by normalizing it and removing potentially dangerous elements.
    
    Args:
        url: URL string to sanitize
        
    Returns:
        Sanitized URL string, or None if URL is invalid
    """
    if not url or not isinstance(url, str):
        return None
    
    url = url.strip()
    if not url:
        return None
    
    # Validate first
    is_valid, error = validate_url(url)
    if not is_valid:
        logger.warning(f"URL validation failed during sanitization: {error}")
        return None
    
    try:
        # Parse and reconstruct URL to normalize it
        parsed = urlparse(url)
        
        # Normalize scheme and hostname to lowercase
        normalized = parsed._replace(
            scheme=parsed.scheme.lower(),
            netloc=parsed.netloc.lower() if parsed.netloc else parsed.netloc
        )
        
        # Reconstruct URL
        sanitized = urlunparse(normalized)
        return sanitized
    except Exception as e:
        logger.error(f"Error sanitizing URL: {e}")
        return None


def validate_api_url(url: str) -> Tuple[bool, Optional[str]]:
    """
    Validate an API URL (convenience function with default settings).
    
    Args:
        url: API URL to validate
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    return validate_url(url, allow_private_ips=False)

