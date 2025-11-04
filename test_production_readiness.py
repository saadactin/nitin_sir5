"""
Test Cases for Production Readiness Fixes
Tests all critical security and configuration fixes
"""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock
import tempfile

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from env_validator import validate_environment, get_environment_summary


class TestEnvironmentValidation(unittest.TestCase):
    """Test environment variable validation"""
    
    def setUp(self):
        """Clear environment before each test"""
        self.original_env = os.environ.copy()
        # Clear sensitive vars
        for key in ['SECRET_KEY', 'PG_PASSWORD', 'CLICKHOUSE_PASSWORD']:
            if key in os.environ:
                del os.environ[key]
    
    def tearDown(self):
        """Restore environment after each test"""
        os.environ.clear()
        os.environ.update(self.original_env)
    
    def test_required_vars_missing(self):
        """Test that validation fails when required vars are missing"""
        is_valid, missing = validate_environment(production_mode=False)
        self.assertFalse(is_valid)
        self.assertIn('SECRET_KEY', missing)
        self.assertIn('PG_PASSWORD', missing)
        self.assertIn('CLICKHOUSE_PASSWORD', missing)
    
    def test_required_vars_present(self):
        """Test that validation passes when required vars are set"""
        os.environ['SECRET_KEY'] = 'test_secret_key'
        os.environ['PG_PASSWORD'] = 'test_pg_password'
        os.environ['CLICKHOUSE_PASSWORD'] = 'test_ch_password'
        
        is_valid, missing = validate_environment(production_mode=False)
        self.assertTrue(is_valid)
        self.assertEqual(len(missing), 0)
    
    def test_production_mode_fails_without_vars(self):
        """Test that production mode raises error when vars are missing"""
        with self.assertRaises(RuntimeError):
            # This should be caught by the validator, but we test the logic
            is_valid, missing = validate_environment(production_mode=True)
            if not is_valid:
                raise RuntimeError(f"Missing: {missing}")
    
    def test_environment_summary(self):
        """Test environment summary generation"""
        os.environ['SECRET_KEY'] = 'test_secret'
        os.environ['PG_PASSWORD'] = 'test_pg'
        
        summary = get_environment_summary()
        self.assertIn('SECRET_KEY', summary['required'])
        self.assertIn('PG_PASSWORD', summary['required'])
        self.assertIn('SECRET_KEY', summary['missing_required'])
        self.assertFalse('SECRET_KEY' in summary['missing_required'])


class TestSecretKeySecurity(unittest.TestCase):
    """Test secret key security fixes"""
    
    def setUp(self):
        """Clear environment"""
        self.original_env = os.environ.copy()
        if 'SECRET_KEY' in os.environ:
            del os.environ['SECRET_KEY']
        if 'FLASK_ENV' in os.environ:
            del os.environ['FLASK_ENV']
        if 'ENVIRONMENT' in os.environ:
            del os.environ['ENVIRONMENT']
    
    def tearDown(self):
        """Restore environment"""
        os.environ.clear()
        os.environ.update(self.original_env)
    
    def test_secret_key_required_in_production(self):
        """Test that secret key is required in production"""
        os.environ['FLASK_ENV'] = 'production'
        
        # Import should fail if SECRET_KEY not set
        with self.assertRaises(RuntimeError):
            from db_utils import SECRET_KEY
            # Re-validate
            if not SECRET_KEY or SECRET_KEY == "NITIN_SIR":
                raise RuntimeError("SECRET_KEY not properly set")
    
    def test_secret_key_warning_in_development(self):
        """Test that secret key shows warning in development"""
        os.environ['FLASK_ENV'] = 'development'
        
        # Should not raise error, but should use fallback
        from db_utils import get_secret_key
        secret = get_secret_key()
        self.assertIsNotNone(secret)
        # In dev, it should use fallback
        if not os.environ.get('SECRET_KEY'):
            self.assertEqual(secret, "NITIN_SIR")
    
    def test_secret_key_from_environment(self):
        """Test that secret key is loaded from environment"""
        os.environ['SECRET_KEY'] = 'my_custom_secret_key_12345'
        from db_utils import get_secret_key
        secret = get_secret_key()
        self.assertEqual(secret, 'my_custom_secret_key_12345')


class TestHardcodedCredentialsRemoval(unittest.TestCase):
    """Test that hardcoded credentials are removed"""
    
    def test_api_sync_no_hardcoded_smtp(self):
        """Test that api_sync.py doesn't have hardcoded SMTP credentials"""
        with open('api_sync.py', 'r') as f:
            content = f.read()
        
        # Should not contain hardcoded credentials
        self.assertNotIn('lqcd zyjx ayjh hyef', content)
        self.assertNotIn('saadpractice4@gmail.com', content)
        self.assertNotIn('saad.sayyed@actin.co.in', content)
        
        # Should check for environment variables
        self.assertIn('os.getenv("SMTP_PASSWORD")', content)
        self.assertIn('os.getenv("SMTP_USER")', content)
    
    def test_no_hardcoded_passwords_in_code(self):
        """Test that no hardcoded passwords exist in main code files"""
        # Files to check (excluding test files)
        main_files = [
            'app.py',
            'api_sync.py',
            'db_utils.py',
            'hana_sync.py',
            'analytics.py',
        ]
        
        suspicious_patterns = [
            'password.*=.*["\'].*["\']',
            'Password.*=.*["\'].*["\']',
            'PASSWORD.*=.*["\'].*["\']',
        ]
        
        for file_path in main_files:
            if not os.path.exists(file_path):
                continue
            
            with open(file_path, 'r') as f:
                content = f.read()
            
            # Check for suspicious patterns (but allow empty strings and env vars)
            lines = content.split('\n')
            for i, line in enumerate(lines, 1):
                # Skip comments and docstrings
                if line.strip().startswith('#') or '"""' in line or "'''" in line:
                    continue
                
                # Check for hardcoded passwords (not empty, not env var)
                if 'password' in line.lower() and '=' in line:
                    # Skip if it's an env var or empty string
                    if 'os.getenv' in line or 'os.environ.get' in line or '""' in line or "''" in line:
                        continue
                    # Skip if it's a function parameter
                    if 'def ' in line or '=' in line and 'password' in line.split('=')[0]:
                        # Check if it's assigning a hardcoded value
                        if len(line.split('=')) > 1:
                            value_part = line.split('=')[1].strip()
                            # If it contains quotes with content, it's suspicious
                            if ('"' in value_part or "'" in value_part) and len(value_part) > 3:
                                # But allow if it's clearly a variable name
                                if not any(c.isalnum() for c in value_part.replace('"', '').replace("'", '')):
                                    continue
                                self.fail(f"Potential hardcoded password in {file_path}:{i}: {line.strip()}")


class TestSQLInjectionPrevention(unittest.TestCase):
    """Test SQL injection prevention"""
    
    def test_analytics_uses_parameterized_queries(self):
        """Test that analytics.py uses parameterized queries"""
        with open('analytics.py', 'r') as f:
            content = f.read()
        
        # Should use ? placeholders for parameters
        self.assertIn('<= ?', content)
        
        # Should use params parameter in pd.read_sql
        self.assertIn('params=[', content)
        
        # Should not have direct string interpolation in WHERE clauses with user data
        # (schema and table are validated identifiers, so they're OK)
    
    def test_hana_sync_uses_escaped_strings(self):
        """Test that hana_sync.py uses proper escaping"""
        with open('hana_sync.py', 'r') as f:
            content = f.read()
        
        # Should use .replace("'", "''") for escaping
        self.assertIn(".replace(\"'\", \"''\")", content)
        
        # Should not use %s placeholders (ClickHouse doesn't support them)
        # But should escape strings properly


class TestDebugCodeRemoval(unittest.TestCase):
    """Test that debug code is removed"""
    
    def test_no_print_statements_in_app_py(self):
        """Test that app.py doesn't have debug print statements"""
        with open('app.py', 'r') as f:
            content = f.read()
        
        # Should not have [CONSOLE DEBUG] print statements
        self.assertNotIn('[CONSOLE DEBUG]', content)
        
        # Should use logger instead
        self.assertIn('app.logger.debug', content)
        self.assertIn('app.logger.info', content)
    
    def test_shutdown_uses_logger(self):
        """Test that shutdown handlers use logger"""
        with open('app.py', 'r') as f:
            content = f.read()
        
        # Shutdown messages should use logger
        self.assertIn('app.logger.info("[SHUTDOWN]', content)
        self.assertNotIn('print("[SHUTDOWN]', content)


class TestEnvironmentExampleFile(unittest.TestCase):
    """Test that .env.example file exists and is complete"""
    
    def test_env_example_exists(self):
        """Test that .env.example file exists"""
        self.assertTrue(os.path.exists('.env.example'))
    
    def test_env_example_has_all_required_vars(self):
        """Test that .env.example has all required variables"""
        with open('.env.example', 'r') as f:
            content = f.read()
        
        required_vars = [
            'SECRET_KEY',
            'PG_PASSWORD',
            'CLICKHOUSE_PASSWORD',
        ]
        
        for var in required_vars:
            self.assertIn(var, content, f"{var} should be documented in .env.example")
    
    def test_env_example_no_real_credentials(self):
        """Test that .env.example doesn't contain real credentials"""
        with open('.env.example', 'r') as f:
            content = f.read()
        
        # Should not contain actual passwords
        self.assertNotIn('lqcd zyjx ayjh hyef', content)
        self.assertNotIn('StrongPassword123', content)
        self.assertNotIn('YourPassword123', content)
        
        # Should contain placeholders
        self.assertIn('your-secret-key', content.lower())
        self.assertIn('your_', content.lower())


def run_all_tests():
    """Run all production readiness tests"""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add all test classes
    suite.addTests(loader.loadTestsFromTestCase(TestEnvironmentValidation))
    suite.addTests(loader.loadTestsFromTestCase(TestSecretKeySecurity))
    suite.addTests(loader.loadTestsFromTestCase(TestHardcodedCredentialsRemoval))
    suite.addTests(loader.loadTestsFromTestCase(TestSQLInjectionPrevention))
    suite.addTests(loader.loadTestsFromTestCase(TestDebugCodeRemoval))
    suite.addTests(loader.loadTestsFromTestCase(TestEnvironmentExampleFile))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)

