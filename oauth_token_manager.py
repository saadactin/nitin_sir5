"""
OAuth Token Manager for API Sync
Automatically handles token generation and refresh
"""
import requests
import time
import logging
from datetime import datetime, timedelta
import threading

logger = logging.getLogger(__name__)


class OAuthTokenManager:
    """Manages OAuth tokens with automatic refresh"""
    
    def __init__(self, token_url, username, password, refresh_interval=3600):
        """
        Initialize OAuth Token Manager
        
        Args:
            token_url: URL to get OAuth token (e.g., http://localhost:4010/pass)
            username: Username for authentication
            password: Password for authentication
            refresh_interval: Token refresh interval in seconds (default: 3600 = 1 hour)
        """
        self.token_url = token_url
        self.username = username
        self.password = password
        self.refresh_interval = refresh_interval
        
        self.current_token = None
        self.token_expiry = None
        self.lock = threading.Lock()
        
        # Get initial token
        self.refresh_token()
        
        # Start background refresh thread
        self.refresh_thread = threading.Thread(target=self._auto_refresh_loop, daemon=True)
        self.refresh_thread.start()
        
        logger.info(f"OAuth Token Manager initialized for {token_url}")
    
    def refresh_token(self):
        """Get a new OAuth token"""
        try:
            logger.info(f"Requesting new OAuth token from {self.token_url}")
            
            # Make POST request to get token
            response = requests.post(
                self.token_url,
                json={
                    "username": self.username,
                    "password": self.password
                },
                timeout=10
            )
            
            response.raise_for_status()
            data = response.json()
            
            # Extract token (adjust based on your API response structure)
            # Common patterns: {"token": "..."}, {"access_token": "..."}, {"oauth_token": "..."}
            token = data.get('token') or data.get('access_token') or data.get('oauth_token')
            
            if not token:
                # If token is in a different field, try to find it
                logger.warning(f"Token field not found in standard locations. Response: {data}")
                # Use the first string value that looks like a token
                for key, value in data.items():
                    if isinstance(value, str) and len(value) > 20:
                        token = value
                        logger.info(f"Using '{key}' field as token")
                        break
            
            if token:
                with self.lock:
                    self.current_token = token
                    self.token_expiry = datetime.now() + timedelta(seconds=self.refresh_interval)
                
                logger.info(f"OAuth token refreshed successfully (expires in {self.refresh_interval}s)")
                return token
            else:
                logger.error(f"ERROR: No token found in response: {data}")
                return None
                
        except Exception as e:
            logger.error(f"ERROR: Failed to refresh OAuth token: {e}")
            return None
    
    def get_token(self):
        """Get current valid token (refreshes if expired)"""
        with self.lock:
            # Check if token is expired or will expire in next 60 seconds
            if not self.current_token or not self.token_expiry or \
               datetime.now() >= (self.token_expiry - timedelta(seconds=60)):
                logger.info("Token expired or expiring soon, refreshing...")
                self.refresh_token()
            
            return self.current_token
    
    def _auto_refresh_loop(self):
        """Background thread to auto-refresh token"""
        while True:
            try:
                # Wait until 60 seconds before expiry
                if self.token_expiry:
                    wait_time = (self.token_expiry - datetime.now() - timedelta(seconds=60)).total_seconds()
                    if wait_time > 0:
                        time.sleep(wait_time)
                    else:
                        time.sleep(60)  # Default wait if expiry calculation is off
                else:
                    time.sleep(self.refresh_interval - 60)
                
                # Refresh token
                self.refresh_token()
                
            except Exception as e:
                logger.error(f"Error in auto-refresh loop: {e}")
                time.sleep(60)  # Wait a minute before retrying


# Global token managers cache
_token_managers = {}
_token_managers_lock = threading.Lock()


def get_token_manager(token_url, username, password, refresh_interval=3600):
    """
    Get or create a token manager for given credentials
    Reuses existing managers for same token_url
    """
    with _token_managers_lock:
        key = f"{token_url}:{username}"
        
        if key not in _token_managers:
            logger.info(f"Creating new token manager for {token_url}")
            _token_managers[key] = OAuthTokenManager(
                token_url, username, password, refresh_interval
            )
        
        return _token_managers[key]


def get_oauth_token(token_url, username, password, refresh_interval=3600):
    """
    Get current OAuth token (creates manager if needed)
    
    Args:
        token_url: URL to get token
        username: Username
        password: Password
        refresh_interval: Token lifetime in seconds (default: 3600 = 1 hour)
    
    Returns:
        Current valid OAuth token
    """
    manager = get_token_manager(token_url, username, password, refresh_interval)
    return manager.get_token()

