"""
Test cases for Zoho OAuth integration
"""
import unittest
import sys
import os
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from zoho_oauth_manager import ZohoOAuthManager


class TestZohoOAuthManager(unittest.TestCase):
    """Test Zoho OAuth token manager"""
    
    @patch('zoho_oauth_manager.requests.post')
    def test_refresh_token_success(self, mock_post):
        """Test successful token refresh"""
        # Mock successful response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "1000.test_access_token",
            "api_domain": "https://www.zohoapis.in",
            "expires_in": 3600,
            "token_type": "Bearer",
            "scope": "ZohoCRM.modules.ALL"
        }
        mock_post.return_value = mock_response
        
        result = ZohoOAuthManager.refresh_token(
            refresh_token="1000.test_refresh_token",
            client_id="1000.test_client_id",
            client_secret="test_client_secret"
        )
        
        self.assertIsNotNone(result)
        self.assertEqual(result['access_token'], "1000.test_access_token")
        self.assertEqual(result['api_domain'], "https://www.zohoapis.in")
        self.assertEqual(result['expires_in'], 3600)
        self.assertEqual(result['token_type'], "Bearer")
        
        # Verify request was made correctly
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        self.assertEqual(call_args[0][0], "https://accounts.zoho.in/oauth/v2/token")
        self.assertEqual(call_args[1]['headers']['Content-Type'], "application/x-www-form-urlencoded")
        self.assertEqual(call_args[1]['data']['grant_type'], "refresh_token")
    
    @patch('zoho_oauth_manager.requests.post')
    def test_refresh_token_failure(self, mock_post):
        """Test token refresh failure"""
        # Mock failed response
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.json.return_value = {"error": "invalid_grant"}
        mock_post.return_value = mock_response
        
        result = ZohoOAuthManager.refresh_token(
            refresh_token="invalid_token",
            client_id="test_client_id",
            client_secret="test_client_secret"
        )
        
        self.assertIsNone(result)
    
    def test_get_valid_token_with_valid_token(self):
        """Test getting token when stored token is still valid"""
        # Create a token that expires in 1 hour
        expiry_time = datetime.now() + timedelta(hours=1)
        expiry_str = expiry_time.isoformat()
        
        result = ZohoOAuthManager.get_valid_token(
            source_id=1,
            refresh_token="test_refresh",
            client_id="test_client_id",
            client_secret="test_secret",
            stored_access_token="valid_token",
            stored_expiry=expiry_str,
            stored_api_domain="https://www.zohoapis.in"
        )
        
        self.assertIsNotNone(result)
        self.assertEqual(result['access_token'], "valid_token")
        self.assertFalse(result['needs_refresh'])
    
    @patch('zoho_oauth_manager.ZohoOAuthManager.refresh_token')
    def test_get_valid_token_with_expired_token(self, mock_refresh):
        """Test getting token when stored token is expired"""
        # Token expired 1 hour ago
        expiry_time = datetime.now() - timedelta(hours=1)
        expiry_str = expiry_time.isoformat()
        
        # Mock refresh to return new token
        mock_refresh.return_value = {
            "access_token": "new_token",
            "api_domain": "https://www.zohoapis.in",
            "expires_in": 3600,
            "token_type": "Bearer"
        }
        
        result = ZohoOAuthManager.get_valid_token(
            source_id=1,
            refresh_token="test_refresh",
            client_id="test_client_id",
            client_secret="test_secret",
            stored_access_token="expired_token",
            stored_expiry=expiry_str,
            stored_api_domain="https://www.zohoapis.in"
        )
        
        self.assertIsNotNone(result)
        self.assertEqual(result['access_token'], "new_token")
        self.assertTrue(result['needs_refresh'])
        mock_refresh.assert_called_once()


class TestZohoOAuthIntegration(unittest.TestCase):
    """Integration tests (require actual credentials in environment)"""
    
    @unittest.skipUnless(
        os.getenv('TEST_ZOHO_REFRESH_TOKEN'),
        "Skip integration tests - no Zoho credentials"
    )
    def test_real_token_refresh(self):
        """Test actual token refresh with real credentials"""
        refresh_token = os.getenv('TEST_ZOHO_REFRESH_TOKEN')
        client_id = os.getenv('TEST_ZOHO_CLIENT_ID')
        client_secret = os.getenv('TEST_ZOHO_CLIENT_SECRET')
        
        if not all([refresh_token, client_id, client_secret]):
            self.skipTest("Missing Zoho credentials")
        
        result = ZohoOAuthManager.refresh_token(
            refresh_token=refresh_token,
            client_id=client_id,
            client_secret=client_secret
        )
        
        self.assertIsNotNone(result, "Token refresh should succeed")
        self.assertIn('access_token', result)
        self.assertIn('api_domain', result)
        self.assertEqual(result['expires_in'], 3600)


if __name__ == '__main__':
    # Run tests
    unittest.main(verbosity=2)

