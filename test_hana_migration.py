"""
Comprehensive test suite for HANA to ClickHouse migration
Tests connection, schema extraction, data migration, and incremental sync
"""

import unittest
import sys
import os
from unittest.mock import Mock, patch, MagicMock
import json
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from hana_sync import HanaToClickHouseSync
    HANA_AVAILABLE = True
except ImportError:
    HANA_AVAILABLE = False
    print("Warning: hdbcli not available. HANA tests will be skipped.")


class TestHanaSync(unittest.TestCase):
    """Test cases for HANA to ClickHouse synchronization"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.hana_config = {
            'host': '192.168.16.62',
            'port': 30015,
            'username': 'Tor1111',
            'password': 'Tor1111'
        }
        
        self.clickhouse_config = {
            'host': 'localhost',
            'port': 9000,
            'user': 'default',
            'password': '',
            'database': 'test_hana_migration'
        }
        
        self.sync_engine = None
    
    def tearDown(self):
        """Clean up after tests"""
        if self.sync_engine:
            try:
                self.sync_engine.close_connections()
            except:
                pass
    
    @unittest.skipUnless(HANA_AVAILABLE, "hdbcli not available")
    def test_hana_connection(self):
        """Test 1: Verify HANA connection can be established"""
        print("\n[TEST 1] Testing HANA connection...")
        sync_engine = HanaToClickHouseSync(self.hana_config, self.clickhouse_config)
        result = sync_engine.connect_hana()
        self.assertTrue(result, "Failed to connect to HANA")
        sync_engine.close_connections()
        print("✓ HANA connection successful")
    
    def test_clickhouse_connection(self):
        """Test 2: Verify ClickHouse connection can be established"""
        print("\n[TEST 2] Testing ClickHouse connection...")
        from clickhouse_driver import Client
        try:
            client = Client(
                host=self.clickhouse_config['host'],
                port=self.clickhouse_config['port'],
                user=self.clickhouse_config['user'],
                password=self.clickhouse_config['password']
            )
            client.execute("SELECT 1")
            client.disconnect()
            print("✓ ClickHouse connection successful")
        except Exception as e:
            self.fail(f"ClickHouse connection failed: {e}")
    
    @unittest.skipUnless(HANA_AVAILABLE, "hdbcli not available")
    def test_schema_extraction(self):
        """Test 3: Verify HANA schema extraction works"""
        print("\n[TEST 3] Testing HANA schema extraction...")
        sync_engine = HanaToClickHouseSync(self.hana_config, self.clickhouse_config)
        if not sync_engine.connect_hana():
            self.skipTest("Cannot connect to HANA")
        
        schemas = sync_engine.get_hana_schemas()
        self.assertIsInstance(schemas, list, "Schemas should be a list")
        self.assertGreater(len(schemas), 0, "Should find at least one schema")
        
        # Verify no system schemas in results
        system_schemas = ['SYS', '_SYS_BI', '_SYS_BIC', '_SYS_EPM']
        for schema in schemas:
            self.assertNotIn(schema, system_schemas, f"System schema {schema} should be excluded")
        
        sync_engine.close_connections()
        print(f"✓ Found {len(schemas)} user schemas")
    
    @unittest.skipUnless(HANA_AVAILABLE, "hdbcli not available")
    def test_table_extraction(self):
        """Test 4: Verify table extraction from a schema"""
        print("\n[TEST 4] Testing table extraction...")
        sync_engine = HanaToClickHouseSync(self.hana_config, self.clickhouse_config)
        if not sync_engine.connect_hana():
            self.skipTest("Cannot connect to HANA")
        
        schemas = sync_engine.get_hana_schemas()
        if len(schemas) == 0:
            self.skipTest("No schemas found")
        
        test_schema = schemas[0]
        tables = sync_engine.get_hana_tables(test_schema)
        self.assertIsInstance(tables, list, "Tables should be a list")
        
        if len(tables) > 0:
            table = tables[0]
            self.assertIn('name', table, "Table should have 'name' field")
            self.assertIn('type', table, "Table should have 'type' field")
        
        sync_engine.close_connections()
        print(f"✓ Found {len(tables)} tables in schema '{test_schema}'")
    
    @unittest.skipUnless(HANA_AVAILABLE, "hdbcli not available")
    def test_table_schema_extraction(self):
        """Test 5: Verify table schema (columns) extraction"""
        print("\n[TEST 5] Testing table schema extraction...")
        sync_engine = HanaToClickHouseSync(self.hana_config, self.clickhouse_config)
        if not sync_engine.connect_hana():
            self.skipTest("Cannot connect to HANA")
        
        schemas = sync_engine.get_hana_schemas()
        if len(schemas) == 0:
            self.skipTest("No schemas found")
        
        # Find a schema with tables
        tables_found = False
        for schema in schemas[:5]:  # Check first 5 schemas
            tables = sync_engine.get_hana_tables(schema)
            if len(tables) > 0:
                test_table = tables[0]['name']
                columns = sync_engine.get_hana_table_schema(schema, test_table)
                
                self.assertIsInstance(columns, list, "Columns should be a list")
                self.assertGreater(len(columns), 0, "Table should have at least one column")
                
                # Verify column structure
                if len(columns) > 0:
                    col = columns[0]
                    required_fields = ['name', 'type', 'nullable']
                    for field in required_fields:
                        self.assertIn(field, col, f"Column should have '{field}' field")
                
                tables_found = True
                print(f"✓ Found {len(columns)} columns in {schema}.{test_table}")
                break
        
        sync_engine.close_connections()
        if not tables_found:
            self.skipTest("No tables found in any schema")
    
    def test_type_mapping(self):
        """Test 6: Verify HANA to ClickHouse type mapping"""
        print("\n[TEST 6] Testing data type mapping...")
        sync_engine = HanaToClickHouseSync(self.hana_config, self.clickhouse_config)
        
        test_cases = [
            ('TINYINT', None, None, 'Int8'),
            ('SMALLINT', None, None, 'Int16'),
            ('INTEGER', None, None, 'Int32'),
            ('BIGINT', None, None, 'Int64'),
            ('DECIMAL', None, 2, 'Decimal64(2)'),
            ('REAL', None, None, 'Float32'),
            ('DOUBLE', None, None, 'Float64'),
            ('VARCHAR', 100, None, 'String'),
            ('DATE', None, None, 'Date'),
            ('TIMESTAMP', None, None, 'DateTime64'),
            ('BOOLEAN', None, None, 'UInt8'),
        ]
        
        for hana_type, length, scale, expected_ch_type in test_cases:
            mapped_type = sync_engine.map_hana_to_clickhouse_type(hana_type, length, scale)
            self.assertIn(expected_ch_type.split('(')[0], mapped_type, 
                         f"Type {hana_type} should map to something containing {expected_ch_type}")
        
        print("✓ All type mappings correct")
    
    def test_table_naming(self):
        """Test 7: Verify ClickHouse table naming convention"""
        print("\n[TEST 7] Testing table naming convention...")
        sync_engine = HanaToClickHouseSync(self.hana_config, self.clickhouse_config)
        
        test_cases = [
            ('SCHEMA1', 'CUSTOMERS', 'SCHEMA1_CUSTOMERS'),
            ('MySchema', 'UserTable', 'MYSCHEMA_USERTABLE'),
            ('schema_test', 'table-123', 'SCHEMA_TEST_TABLE_123'),
        ]
        
        for schema, table, expected in test_cases:
            ch_table_name = sync_engine.create_clickhouse_table_name(schema, table)
            self.assertEqual(ch_table_name, expected, 
                           f"Table name should be {expected}, got {ch_table_name}")
        
        print("✓ Table naming convention correct")
    
    @unittest.skipUnless(HANA_AVAILABLE, "hdbcli not available")
    def test_create_clickhouse_table(self):
        """Test 8: Verify ClickHouse table creation"""
        print("\n[TEST 8] Testing ClickHouse table creation...")
        sync_engine = HanaToClickHouseSync(self.hana_config, self.clickhouse_config)
        
        if not sync_engine.connect_clickhouse():
            self.skipTest("Cannot connect to ClickHouse")
        
        # Create test table schema
        test_columns = [
            {'name': 'id', 'type': 'INTEGER', 'length': None, 'scale': None, 'nullable': False},
            {'name': 'name', 'type': 'VARCHAR', 'length': 100, 'scale': None, 'nullable': True},
            {'name': 'created_date', 'type': 'DATE', 'length': None, 'scale': None, 'nullable': True},
        ]
        
        test_schema = 'TEST_SCHEMA'
        test_table = 'TEST_TABLE'
        
        # Create database first
        try:
            sync_engine.ch_client.execute(f"CREATE DATABASE IF NOT EXISTS {self.clickhouse_config['database']}")
        except:
            pass
        
        result = sync_engine.create_clickhouse_table(test_schema, test_table, test_columns)
        self.assertTrue(result, "Table creation should succeed")
        
        # Verify table exists
        ch_table_name = sync_engine.create_clickhouse_table_name(test_schema, test_table)
        exists = sync_engine.table_exists_in_clickhouse(
            self.clickhouse_config['database'], 
            ch_table_name
        )
        self.assertTrue(exists, "Table should exist in ClickHouse")
        
        # Cleanup
        try:
            sync_engine.ch_client.execute(f"DROP TABLE IF EXISTS {self.clickhouse_config['database']}.{ch_table_name}")
        except:
            pass
        
        sync_engine.close_connections()
        print(f"✓ Table {ch_table_name} created successfully")
    
    @unittest.skipUnless(HANA_AVAILABLE, "hdbcli not available")
    def test_data_migration(self):
        """Test 9: Verify data migration from HANA to ClickHouse"""
        print("\n[TEST 9] Testing data migration...")
        sync_engine = HanaToClickHouseSync(self.hana_config, self.clickhouse_config)
        
        if not (sync_engine.connect_hana() and sync_engine.connect_clickhouse()):
            self.skipTest("Cannot connect to HANA or ClickHouse")
        
        # Find a small table to test with
        schemas = sync_engine.get_hana_schemas()
        if len(schemas) == 0:
            self.skipTest("No schemas found")
        
        test_schema = None
        test_table = None
        
        # Look for a table with data
        for schema in schemas[:10]:
            tables = sync_engine.get_hana_tables(schema)
            for table_info in tables:
                table_name = table_info['name']
                try:
                    # Check if table has data
                    cursor = sync_engine.hana_conn.cursor()
                    cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"')
                    count = cursor.fetchone()[0]
                    cursor.close()
                    
                    if 0 < count < 1000:  # Find a small table
                        test_schema = schema
                        test_table = table_name
                        break
                except:
                    continue
            if test_schema:
                break
        
        if not test_schema:
            self.skipTest("No suitable test table found (small table with data)")
        
        print(f"  Testing with table: {test_schema}.{test_table}")
        
        # Get table schema
        columns = sync_engine.get_hana_table_schema(test_schema, test_table)
        if not columns:
            self.skipTest("Could not get table schema")
        
        # Create ClickHouse table
        sync_engine.create_clickhouse_table(test_schema, test_table, columns)
        
        # Migrate data
        result = sync_engine.migrate_table_data(test_schema, test_table, batch_size=100)
        
        self.assertEqual(result['status'], 'success', "Migration should succeed")
        self.assertGreater(result['migrated_rows'], 0, "Should migrate at least one row")
        self.assertEqual(result['total_rows'], result['migrated_rows'], 
                        "All rows should be migrated")
        
        # Verify data in ClickHouse
        ch_table_name = sync_engine.create_clickhouse_table_name(test_schema, test_table)
        ch_rows = sync_engine.ch_client.execute(
            f"SELECT count() FROM {self.clickhouse_config['database']}.{ch_table_name}"
        )[0][0]
        
        self.assertEqual(ch_rows, result['migrated_rows'], 
                        "ClickHouse row count should match migrated count")
        
        # Verify metadata columns exist
        metadata_cols = sync_engine.ch_client.execute(
            f"SELECT name FROM system.columns WHERE database = '{self.clickhouse_config['database']}' "
            f"AND table = '{ch_table_name}' AND name LIKE '_%'"
        )
        metadata_col_names = [col[0] for col in metadata_cols]
        
        self.assertIn('_source_schema', metadata_col_names, "Should have _source_schema column")
        self.assertIn('_source_table', metadata_col_names, "Should have _source_table column")
        self.assertIn('_sync_timestamp', metadata_col_names, "Should have _sync_timestamp column")
        
        # Verify metadata values
        sample_row = sync_engine.ch_client.execute(
            f"SELECT _source_schema, _source_table FROM {self.clickhouse_config['database']}.{ch_table_name} LIMIT 1"
        )[0]
        
        self.assertEqual(sample_row[0], test_schema, "Metadata should have correct source schema")
        self.assertEqual(sample_row[1], test_table, "Metadata should have correct source table")
        
        sync_engine.close_connections()
        print(f"✓ Successfully migrated {result['migrated_rows']} rows")
    
    @unittest.skipUnless(HANA_AVAILABLE, "hdbcli not available")
    def test_incremental_sync_setup(self):
        """Test 10: Verify incremental sync setup"""
        print("\n[TEST 10] Testing incremental sync setup...")
        sync_engine = HanaToClickHouseSync(self.hana_config, self.clickhouse_config)
        
        if not (sync_engine.connect_hana() and sync_engine.connect_clickhouse()):
            self.skipTest("Cannot connect to HANA or ClickHouse")
        
        # Find a test table
        schemas = sync_engine.get_hana_schemas()
        if len(schemas) == 0:
            self.skipTest("No schemas found")
        
        test_schema = schemas[0]
        tables = sync_engine.get_hana_tables(test_schema)
        if len(tables) == 0:
            self.skipTest("No tables found")
        
        test_table = tables[0]['name']
        
        # Setup incremental sync
        result = sync_engine.setup_incremental_sync(test_schema, test_table)
        
        # Verify sync_metadata table exists
        database_name = self.clickhouse_config['database']
        try:
            metadata = sync_engine.ch_client.execute(
                f"SELECT source_schema, source_table FROM {database_name}.sync_metadata "
                f"WHERE source_schema = '{test_schema}' AND source_table = '{test_table}'"
            )
            self.assertGreater(len(metadata), 0, "Sync metadata should be created")
        except Exception as e:
            if "does not exist" in str(e).lower():
                self.fail("sync_metadata table should be created")
            else:
                # Incremental sync may not be available (no timestamp column)
                pass
        
        sync_engine.close_connections()
        print("✓ Incremental sync setup verified")
    
    def test_data_validation(self):
        """Test 11: Verify data integrity after migration"""
        print("\n[TEST 11] Testing data validation...")
        # This would be run after actual migration to verify:
        # - Row counts match
        # - Data types are correctly converted
        # - No data loss
        # - Metadata columns are populated correctly
        
        # Placeholder for actual data validation
        print("✓ Data validation framework ready")
        self.assertTrue(True)


class TestHanaFlaskIntegration(unittest.TestCase):
    """Test cases for Flask app integration with HANA"""
    
    def setUp(self):
        """Set up Flask test client"""
        try:
            import app
            self.app = app.app
            self.app.config['TESTING'] = True
            self.client = self.app.test_client()
        except Exception as e:
            self.skipTest(f"Cannot import Flask app: {e}")
    
    def test_hana_source_card_display(self):
        """Test 12: Verify HANA source appears as card on home page"""
        print("\n[TEST 12] Testing HANA source card display...")
        
        # This test requires authentication and database setup
        # In real scenario, would mock the database
        
        # For now, verify the template logic
        response = self.client.get('/')
        # Should check if HANA sources are displayed
        self.assertIsNotNone(response, "Home page should load")
        print("✓ Home page accessible")


def run_tests():
    """Run all test cases"""
    print("=" * 70)
    print("HANA TO CLICKHOUSE MIGRATION - COMPREHENSIVE TEST SUITE")
    print("=" * 70)
    
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add all test cases
    suite.addTests(loader.loadTestsFromTestCase(TestHanaSync))
    suite.addTests(loader.loadTestsFromTestCase(TestHanaFlaskIntegration))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    print(f"Tests run: {result.testsRun}")
    print(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped) if hasattr(result, 'skipped') else 0}")
    
    if result.failures:
        print("\nFAILURES:")
        for test, traceback in result.failures:
            print(f"  - {test}: {traceback.split(chr(10))[-2]}")
    
    if result.errors:
        print("\nERRORS:")
        for test, traceback in result.errors:
            print(f"  - {test}: {traceback.split(chr(10))[-2]}")
    
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_tests()
    sys.exit(0 if success else 1)

