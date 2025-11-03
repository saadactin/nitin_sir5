"""
Zoho OAuth Token Manager
Handles automatic token refresh for Zoho CRM API
"""
import requests
import logging
import json
from datetime import datetime, timedelta
from threading import Lock
import time

logger = logging.getLogger(__name__)

class ZohoOAuthManager:
    """
    Manages Zoho OAuth tokens with automatic refresh
    Tokens are refreshed every hour (3600 seconds) as per Zoho's token expiration
    """
    
    # Token refresh URL for Zoho India
    TOKEN_URL = "https://accounts.zoho.in/oauth/v2/token"
    
    # Cache for tokens (in-memory, but also persisted in database)
    _token_cache = {}
    _token_lock = Lock()
    
    @staticmethod
    def refresh_token(refresh_token, client_id, client_secret):
        """
        Refresh Zoho access token using refresh token
        
        Args:
            refresh_token: Zoho refresh token
            client_id: Zoho client ID
            client_secret: Zoho client secret
            
        Returns:
            dict: {
                'access_token': str,
                'api_domain': str,
                'expires_in': int (seconds),
                'token_type': str,
                'scope': str
            } or None if failed
        """
        try:
            logger.info("Refreshing Zoho OAuth token...")
            
            # Prepare request data
            data = {
                'refresh_token': refresh_token,
                'client_id': client_id,
                'client_secret': client_secret,
                'grant_type': 'refresh_token'
            }
            
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            
            # Make token refresh request
            response = requests.post(
                ZohoOAuthManager.TOKEN_URL,
                data=data,
                headers=headers,
                timeout=30
            )
            
            if response.status_code != 200:
                error_msg = f"Token refresh failed with status {response.status_code}: {response.text}"
                logger.error(error_msg)
                
                # Handle rate limiting (429 or 400 with "too many requests")
                if response.status_code == 429 or ("too many requests" in response.text.lower() or "Access Denied" in response.text):
                    logger.warning("Rate limited by Zoho. Please wait before retrying.")
                    error_msg += "\n\n⚠️ Rate Limited: Zoho has temporarily blocked requests. Please wait 5-10 minutes before trying again."
                
                return None
            
            token_data = response.json()
            
            # Check for errors in response
            if 'error' in token_data:
                error_msg = f"Token refresh error: {token_data.get('error', 'Unknown error')}"
                logger.error(error_msg)
                return None
            
            # Extract token information
            access_token = token_data.get('access_token')
            api_domain = token_data.get('api_domain', 'https://www.zohoapis.in')
            expires_in = token_data.get('expires_in', 3600)  # Default to 1 hour
            token_type = token_data.get('token_type', 'Bearer')
            scope = token_data.get('scope', '')
            
            logger.info(f"✅ Token refreshed successfully. Expires in {expires_in} seconds")
            logger.info(f"   API Domain: {api_domain}")
            
            return {
                'access_token': access_token,
                'api_domain': api_domain,
                'expires_in': expires_in,
                'token_type': token_type,
                'scope': scope,
                'refreshed_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.exception(f"Error refreshing Zoho token: {e}")
            return None
    
    @staticmethod
    def get_valid_token(source_id, refresh_token, client_id, client_secret, 
                       stored_access_token=None, stored_expiry=None, stored_api_domain=None):
        """
        Get a valid access token, refreshing if necessary
        
        Args:
            source_id: Data source ID
            refresh_token: Zoho refresh token
            client_id: Zoho client ID
            client_secret: Zoho client secret
            stored_access_token: Currently stored access token (optional)
            stored_expiry: Token expiry timestamp (optional)
            stored_api_domain: API domain from stored token (optional)
            
        Returns:
            dict: {
                'access_token': str,
                'api_domain': str,
                'expires_in': int,
                'token_type': str,
                'needs_refresh': bool  # Whether token was refreshed
            } or None if failed
        """
        with ZohoOAuthManager._token_lock:
            cache_key = f"source_{source_id}"
            
            # Check if token is still valid (with 5 minute buffer)
            current_time = datetime.now()
            needs_refresh = True
            
            if stored_access_token and stored_expiry:
                try:
                    expiry_time = datetime.fromisoformat(stored_expiry) if isinstance(stored_expiry, str) else stored_expiry
                    # Add 5 minute buffer before actual expiry
                    buffer_time = expiry_time - timedelta(minutes=5)
                    
                    if current_time < buffer_time:
                        logger.info(f"Token for source {source_id} is still valid (expires at {expiry_time})")
                        return {
                            'access_token': stored_access_token,
                            'api_domain': stored_api_domain or 'https://www.zohoapis.in',
                            'expires_in': int((expiry_time - current_time).total_seconds()),
                            'token_type': 'Bearer',
                            'needs_refresh': False
                        }
                    else:
                        logger.info(f"Token for source {source_id} is expiring soon (expires at {expiry_time}), refreshing...")
                except Exception as e:
                    logger.warning(f"Error parsing token expiry: {e}, refreshing token")
            
            # Token needs refresh
            logger.info(f"Refreshing token for source {source_id}...")
            token_result = ZohoOAuthManager.refresh_token(refresh_token, client_id, client_secret)
            
            if not token_result:
                logger.error(f"Failed to refresh token for source {source_id}")
                return None
            
            # Update cache
            ZohoOAuthManager._token_cache[cache_key] = {
                'access_token': token_result['access_token'],
                'api_domain': token_result['api_domain'],
                'expires_at': datetime.now() + timedelta(seconds=token_result['expires_in']),
                'token_type': token_result.get('token_type', 'Bearer')
            }
            
            return {
                'access_token': token_result['access_token'],
                'api_domain': token_result['api_domain'],
                'expires_in': token_result['expires_in'],
                'token_type': token_result.get('token_type', 'Bearer'),
                'needs_refresh': True
            }
    
    @staticmethod
    def clear_cache(source_id=None):
        """Clear token cache for a specific source or all sources"""
        with ZohoOAuthManager._token_lock:
            if source_id:
                cache_key = f"source_{source_id}"
                if cache_key in ZohoOAuthManager._token_cache:
                    del ZohoOAuthManager._token_cache[cache_key]
                    logger.info(f"Cleared token cache for source {source_id}")
            else:
                ZohoOAuthManager._token_cache.clear()
                logger.info("Cleared all token caches")

