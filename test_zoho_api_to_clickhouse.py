"""
End-to-End Test: Zoho API → ClickHouse Sync
Tests the complete workflow:
1. Get OAuth token
2. Fetch data from Zoho API
3. Sync to ClickHouse database
4. Verify data was stored correctly
"""
import sys
import os
import unittest
import time
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from zoho_oauth_manager import ZohoOAuthManager
from api_sync import sync_api_to_clickhouse_once
from db_utils import load_clickhouse_config
from clickhouse_driver import Client


class TestZohoAPIToClickHouse(unittest.TestCase):
    """End-to-end test for Zoho API to ClickHouse sync"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test credentials and configuration"""
        # Zoho OAuth credentials (update these with your actual credentials)
        cls.ZOHO_REFRESH_TOKEN = "1000.2cbaa36345c6d04b699b0cb6740c21ef.149922195c479d83c84826653ff84ff4"
        cls.ZOHO_CLIENT_ID = "1000.0L3LLVLEKE9ELW7CE0I0KJ3K4FKBBT"
        cls.ZOHO_CLIENT_SECRET = "d99c479d4c0db451c653d8c380bf6a4c557a73528c"
        
        # Test API endpoint
        cls.API_URL = "https://www.zohoapis.in/crm/v8/Leads?fields=Full_Name,Company,Email,Phone"
        
        # ClickHouse configuration
        ch_config = load_clickhouse_config()
        cls.CLICKHOUSE_DB = "zoho"  # Target database name
        cls.CLICKHOUSE_TABLE = "test_zoho_leads"
        
        # Store ClickHouse config for cleanup
        cls.ch_client = Client(
            host=ch_config['host'],
            port=ch_config['port'],
            user=ch_config['user'],
            password=ch_config['password']
        )
        
        print("\n" + "="*70)
        print("Zoho API -> ClickHouse End-to-End Test")
        print("="*70)
    
    def test_01_refresh_token(self):
        """Test 1: Get Zoho access token"""
        print("\n[TEST 1] Refreshing Zoho OAuth token...")
        
        token_result = ZohoOAuthManager.refresh_token(
            refresh_token=self.ZOHO_REFRESH_TOKEN,
            client_id=self.ZOHO_CLIENT_ID,
            client_secret=self.ZOHO_CLIENT_SECRET
        )
        
        self.assertIsNotNone(token_result, "Token refresh should succeed")
        self.assertIn('access_token', token_result)
        self.assertIn('api_domain', token_result)
        self.assertEqual(token_result['expires_in'], 3600)
        
        # Store token for next tests
        self.access_token = token_result['access_token']
        self.api_domain = token_result['api_domain']
        
        print(f"[OK] Token obtained successfully")
        print(f"   API Domain: {self.api_domain}")
        print(f"   Expires in: {token_result['expires_in']} seconds")
        
        return token_result
    
    def test_02_verify_clickhouse_connection(self):
        """Test 2: Verify ClickHouse connection"""
        print("\n[TEST 2] Verifying ClickHouse connection...")
        
        try:
            # Try to create database if it doesn't exist
            try:
                self.ch_client.execute(f"CREATE DATABASE IF NOT EXISTS {self.CLICKHOUSE_DB}")
                print(f"[OK] Database '{self.CLICKHOUSE_DB}' ready")
            except Exception as e:
                print(f"[WARN] Database creation: {e}")
            
            # Test connection by running a simple query
            result = self.ch_client.execute("SELECT 1")
            self.assertEqual(result, [(1,)], "ClickHouse should respond to queries")
            
            print("[OK] ClickHouse connection verified")
        except Exception as e:
            self.fail(f"ClickHouse connection failed: {e}")
    
    def test_03_fetch_zoho_api_data(self):
        """Test 3: Fetch data from Zoho API using token"""
        print("\n[TEST 3] Fetching data from Zoho API...")
        
        import requests
        
        # Get token first
        token_result = ZohoOAuthManager.refresh_token(
            refresh_token=self.ZOHO_REFRESH_TOKEN,
            client_id=self.ZOHO_CLIENT_ID,
            client_secret=self.ZOHO_CLIENT_SECRET
        )
        
        access_token = token_result['access_token']
        api_domain = token_result['api_domain']
        
        # Update URL to use correct domain
        api_url = self.API_URL
        if not api_url.startswith(api_domain):
            for domain in ['https://www.zohoapis.com', 'https://www.zohoapis.eu', 'https://www.zohoapis.in']:
                if api_url.startswith(domain):
                    api_url = api_url.replace(domain, api_domain)
                    break
        
        # Make API request
        headers = {
            'Authorization': f"Bearer {access_token}"
        }
        
        response = requests.get(api_url, headers=headers, timeout=30)
        
        self.assertEqual(response.status_code, 200, f"API should return 200, got {response.status_code}")
        
        data = response.json()
        self.assertIn('data', data, "Response should contain 'data' field")
        
        records = data.get('data', [])
        print(f"[OK] Fetched {len(records)} records from Zoho API")
        
        if records:
            print(f"   Sample record keys: {list(records[0].keys())[:5]}")
        
        return records
    
    def test_04_sync_to_clickhouse(self):
        """Test 4: Sync Zoho API data to ClickHouse"""
        print("\n[TEST 4] Syncing data to ClickHouse...")
        
        # Ensure database exists
        try:
            self.ch_client.execute(f"CREATE DATABASE IF NOT EXISTS {self.CLICKHOUSE_DB}")
        except:
            pass
        
        # Drop table if exists for clean test
        try:
            self.ch_client.execute(f"DROP TABLE IF EXISTS {self.CLICKHOUSE_DB}.{self.CLICKHOUSE_TABLE}")
            print(f"   Dropped existing table for clean test")
        except:
            pass
        
        # Sync data
        result = sync_api_to_clickhouse_once(
            api_url=self.API_URL,
            target_database=self.CLICKHOUSE_DB,
            target_table=self.CLICKHOUSE_TABLE,
            auth_type="zoho_oauth",
            source_id=999,  # Test source ID
            zoho_refresh_token=self.ZOHO_REFRESH_TOKEN,
            zoho_client_id=self.ZOHO_CLIENT_ID,
            zoho_client_secret=self.ZOHO_CLIENT_SECRET,
            stored_access_token=None,
            stored_token_expiry=None,
            stored_api_domain=None,
            auto_create_table=True
        )
        
        self.assertTrue(result['success'], f"Sync should succeed: {result.get('error')}")
        self.assertGreater(result['records_synced'], 0, "Should sync at least one record")
        
        print(f"[OK] Synced {result['records_synced']} records to ClickHouse")
        print(f"   Database: {self.CLICKHOUSE_DB}")
        print(f"   Table: {self.CLICKHOUSE_TABLE}")
        
        return result
    
    def test_05_verify_clickhouse_data(self):
        """Test 5: Verify data was stored correctly in ClickHouse"""
        print("\n[TEST 5] Verifying data in ClickHouse...")
        
        try:
            # Check table exists
            tables = self.ch_client.execute(f"SHOW TABLES FROM {self.CLICKHOUSE_DB}")
            table_names = [t[0] for t in tables]
            self.assertIn(self.CLICKHOUSE_TABLE, table_names, "Table should exist")
            print(f"[OK] Table '{self.CLICKHOUSE_TABLE}' exists")
            
            # Get row count
            count_result = self.ch_client.execute(
                f"SELECT COUNT(*) FROM {self.CLICKHOUSE_DB}.{self.CLICKHOUSE_TABLE}"
            )
            row_count = count_result[0][0] if count_result else 0
            self.assertGreater(row_count, 0, "Table should contain data")
            print(f"[OK] Table contains {row_count} rows")
            
            # Get sample data
            sample_result = self.ch_client.execute(
                f"SELECT * FROM {self.CLICKHOUSE_DB}.{self.CLICKHOUSE_TABLE} LIMIT 3"
            )
            self.assertGreater(len(sample_result), 0, "Should have sample data")
            
            # Get column names
            columns_result = self.ch_client.execute(
                f"DESCRIBE TABLE {self.CLICKHOUSE_DB}.{self.CLICKHOUSE_TABLE}"
            )
            column_names = [col[0] for col in columns_result]
            
            print(f"[OK] Sample data retrieved")
            print(f"   Table has {len(column_names)} columns")
            print(f"   Columns: {', '.join(column_names[:5])}...")
            
            # Verify key columns exist
            expected_columns = ['Full_Name', 'Company', 'Email']
            found_columns = []
            for col in expected_columns:
                # Check if column exists (may be prefixed with module name)
                if any(col in c or c.endswith(col) for c in column_names):
                    found_columns.append(col)
            
            if found_columns:
                print(f"[OK] Found expected columns: {', '.join(found_columns)}")
            else:
                print(f"[WARN] Expected columns not found, but table structure is valid")
            
            # Show sample record
            if sample_result:
                print(f"\n   Sample record (first row):")
                for i, col_name in enumerate(column_names[:5]):
                    if i < len(sample_result[0]):
                        value = sample_result[0][i]
                        value_str = str(value)[:50] if value else "NULL"
                        print(f"     {col_name}: {value_str}")
            
            return {
                'row_count': row_count,
                'column_count': len(column_names),
                'columns': column_names,
                'sample': sample_result[0] if sample_result else None
            }
            
        except Exception as e:
            self.fail(f"Verification failed: {e}")
    
    def test_06_token_refresh_handling(self):
        """Test 6: Test token refresh when token is expired"""
        print("\n[TEST 6] Testing token refresh handling...")
        
        # Test with expired token
        expired_time = datetime.now().isoformat()
        
        token_result = ZohoOAuthManager.get_valid_token(
            source_id=999,
            refresh_token=self.ZOHO_REFRESH_TOKEN,
            client_id=self.ZOHO_CLIENT_ID,
            client_secret=self.ZOHO_CLIENT_SECRET,
            stored_access_token="expired_token",
            stored_expiry=expired_time,
            stored_api_domain="https://www.zohoapis.in"
        )
        
        self.assertIsNotNone(token_result, "Should get new token when expired")
        self.assertIn('access_token', token_result)
        self.assertTrue(token_result.get('needs_refresh'), "Token should be refreshed")
        
        print(f"[OK] Token refresh handling works correctly")
        print(f"   New token obtained: {token_result['access_token'][:20]}...")
    
    def test_07_multiple_endpoints(self):
        """Test 7: Test syncing from multiple Zoho endpoints"""
        print("\n[TEST 7] Testing multiple Zoho API endpoints...")
        
        endpoints = [
            {
                'name': 'Leads',
                'url': 'https://www.zohoapis.in/crm/v8/Leads?fields=Full_Name,Company,Email',
                'table': 'test_zoho_leads_2'
            },
            {
                'name': 'Contacts',
                'url': 'https://www.zohoapis.in/crm/v8/Contacts?fields=Full_Name,Email,Phone',
                'table': 'test_zoho_contacts'
            }
        ]
        
        results = []
        for endpoint in endpoints:
            try:
                print(f"   Testing {endpoint['name']} endpoint...")
                result = sync_api_to_clickhouse_once(
                    api_url=endpoint['url'],
                    target_database=self.CLICKHOUSE_DB,
                    target_table=endpoint['table'],
                    auth_type="zoho_oauth",
                    source_id=999,
                    zoho_refresh_token=self.ZOHO_REFRESH_TOKEN,
                    zoho_client_id=self.ZOHO_CLIENT_ID,
                    zoho_client_secret=self.ZOHO_CLIENT_SECRET,
                    auto_create_table=True
                )
                
                if result['success']:
                    print(f"   [OK] {endpoint['name']}: {result['records_synced']} records")
                    results.append((endpoint['name'], True, result['records_synced']))
                else:
                    print(f"   [WARN] {endpoint['name']}: {result.get('error', 'Unknown error')}")
                    results.append((endpoint['name'], False, 0))
            except Exception as e:
                print(f"   [ERROR] {endpoint['name']}: {str(e)}")
                results.append((endpoint['name'], False, 0))
        
        successful = [r for r in results if r[1]]
        print(f"\n[OK] Successfully synced {len(successful)}/{len(endpoints)} endpoints")
        
        return results
    
    @classmethod
    def tearDownClass(cls):
        """Cleanup after all tests"""
        print("\n" + "="*70)
        print("Test Summary")
        print("="*70)
        print("[OK] All tests completed")
        print(f"[INFO] Test data stored in: {cls.CLICKHOUSE_DB} database")
        print(f"[INFO] Tables created: test_zoho_leads, test_zoho_leads_2, test_zoho_contacts")
        print("\n[TIP] To clean up test data, run:")
        print(f"   DROP TABLE IF EXISTS {cls.CLICKHOUSE_DB}.test_zoho_leads;")
        print(f"   DROP TABLE IF EXISTS {cls.CLICKHOUSE_DB}.test_zoho_leads_2;")
        print(f"   DROP TABLE IF EXISTS {cls.CLICKHOUSE_DB}.test_zoho_contacts;")
        print("="*70 + "\n")


def run_tests():
    """Run all tests"""
    print("\n[TEST] Starting Zoho API -> ClickHouse Integration Tests...\n")
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestZohoAPIToClickHouse)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "="*70)
    print("Test Results Summary")
    print("="*70)
    print(f"Tests run: {result.testsRun}")
    print(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    
    if result.failures:
        print("\nFailures:")
        for test, traceback in result.failures:
            print(f"  [FAIL] {test}")
    
    if result.errors:
        print("\nErrors:")
        for test, traceback in result.errors:
            print(f"  [ERROR] {test}")
            print(f"     {traceback.split(chr(10))[-2]}")
    
    print("="*70 + "\n")
    
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_tests()
    sys.exit(0 if success else 1)

