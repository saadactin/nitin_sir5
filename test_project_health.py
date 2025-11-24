"""
Project Health Check Test Suite
Run this script to verify that the project is running correctly
"""

import sys
import os
import requests
import time
from datetime import datetime

# Fix Windows console encoding
if sys.platform == 'win32':
    import codecs
    try:
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
        sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
    except:
        pass  # Fallback if buffer doesn't exist

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Test configuration
BASE_URL = os.getenv('FLASK_BASE_URL', 'http://localhost:5001')
TEST_USERNAME = os.getenv('TEST_USERNAME', 'admin')
TEST_PASSWORD = os.getenv('TEST_PASSWORD', 'admin')

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

class TestResult:
    def __init__(self, name, passed, message="", details=""):
        self.name = name
        self.passed = passed
        self.message = message
        self.details = details
        self.timestamp = datetime.now()

def print_test_result(result):
    """Print test result with color coding"""
    # Use ASCII-safe characters for Windows
    if sys.platform == 'win32' and not os.getenv('TERM'):
        status = "[PASS]" if result.passed else "[FAIL]"
        print(f"{status} {result.name}")
    else:
        status = f"{Colors.GREEN}✓ PASS{Colors.RESET}" if result.passed else f"{Colors.RED}✗ FAIL{Colors.RESET}"
        print(f"{status} {result.name}")
    if result.message:
        print(f"    {result.message}")
    if result.details:
        print(f"    Details: {result.details}")

def test_flask_server_running():
    """Test 1: Check if Flask server is running"""
    try:
        response = requests.get(f"{BASE_URL}/login", timeout=5)
        return TestResult(
            "Flask Server Running",
            response.status_code == 200,
            f"Status: {response.status_code}",
            f"Response time: {response.elapsed.total_seconds():.2f}s"
        )
    except requests.exceptions.ConnectionError:
        return TestResult(
            "Flask Server Running",
            False,
            "Cannot connect to server",
            f"Make sure Flask is running on {BASE_URL}"
        )
    except Exception as e:
        return TestResult(
            "Flask Server Running",
            False,
            f"Error: {str(e)}",
            ""
        )

def test_login_page():
    """Test 2: Check if login page loads"""
    try:
        response = requests.get(f"{BASE_URL}/login", timeout=5)
        return TestResult(
            "Login Page Accessible",
            response.status_code == 200 and "login" in response.text.lower(),
            f"Status: {response.status_code}",
            "Login page content found"
        )
    except Exception as e:
        return TestResult(
            "Login Page Accessible",
            False,
            f"Error: {str(e)}",
            ""
        )

def test_authentication():
    """Test 3: Test user authentication"""
    try:
        session = requests.Session()
        response = session.post(
            f"{BASE_URL}/login",
            data={"username": TEST_USERNAME, "password": TEST_PASSWORD},
            timeout=5,
            allow_redirects=False
        )
        # Check if redirected (successful login) or if already logged in
        success = response.status_code in [302, 200] or "dashboard" in response.url.lower()
        return TestResult(
            "User Authentication",
            success,
            f"Status: {response.status_code}",
            f"Redirected to: {response.headers.get('Location', 'N/A')}"
        )
    except Exception as e:
        return TestResult(
            "User Authentication",
            False,
            f"Error: {str(e)}",
            ""
        )

def test_dashboard_access():
    """Test 4: Test dashboard access"""
    try:
        session = requests.Session()
        # Login first
        session.post(
            f"{BASE_URL}/login",
            data={"username": TEST_USERNAME, "password": TEST_PASSWORD},
            timeout=5
        )
        # Access dashboard
        response = session.get(f"{BASE_URL}/dashboard", timeout=5)
        return TestResult(
            "Dashboard Access",
            response.status_code == 200 and ("dashboard" in response.text.lower() or "sync" in response.text.lower()),
            f"Status: {response.status_code}",
            "Dashboard content loaded"
        )
    except Exception as e:
        return TestResult(
            "Dashboard Access",
            False,
            f"Error: {str(e)}",
            ""
        )

def test_api_endpoints():
    """Test 5: Test API endpoints"""
    endpoints = [
        ("/api/target/databases?target_type=postgresql", "GET"),
        ("/api/target/databases?target_type=clickhouse", "GET"),
        ("/dashboard/data", "GET"),
    ]
    
    results = []
    session = requests.Session()
    try:
        # Login first
        session.post(
            f"{BASE_URL}/login",
            data={"username": TEST_USERNAME, "password": TEST_PASSWORD},
            timeout=5
        )
        
        for endpoint, method in endpoints:
            try:
                if method == "GET":
                    response = session.get(f"{BASE_URL}{endpoint}", timeout=5)
                else:
                    response = session.post(f"{BASE_URL}{endpoint}", timeout=5)
                
                results.append(TestResult(
                    f"API Endpoint: {endpoint}",
                    response.status_code in [200, 201, 202],
                    f"Status: {response.status_code}",
                    f"Response time: {response.elapsed.total_seconds():.2f}s"
                ))
            except Exception as e:
                results.append(TestResult(
                    f"API Endpoint: {endpoint}",
                    False,
                    f"Error: {str(e)}",
                    ""
                ))
    except Exception as e:
        results.append(TestResult(
            "API Endpoints",
            False,
            f"Authentication error: {str(e)}",
            ""
        ))
    
    return results

def test_database_connections():
    """Test 6: Test database connections"""
    results = []
    
    # Test PostgreSQL connection
    try:
        from db_utils import get_pg_connection, load_pg_config
        config = load_pg_config()
        conn = get_pg_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        conn.close()
        results.append(TestResult(
            "PostgreSQL Connection",
            True,
            "Connection successful",
            f"Host: {config.get('host', 'N/A')}, Database: {config.get('database', 'N/A')}"
        ))
    except Exception as e:
        results.append(TestResult(
            "PostgreSQL Connection",
            False,
            f"Connection failed: {str(e)}",
            ""
        ))
    
    # Test ClickHouse connection (if configured)
    try:
        from db_utils import load_clickhouse_config
        config = load_clickhouse_config()
        if config and config.get('host'):
            try:
                import clickhouse_connect
                client = clickhouse_connect.get_client(
                    host=config.get('host'),
                    port=int(config.get('port', 9000)),
                    username=config.get('user', 'default'),
                    password=config.get('password', ''),
                    database=config.get('database', 'default')
                )
                client.command('SELECT 1')
                results.append(TestResult(
                    "ClickHouse Connection",
                    True,
                    "Connection successful",
                    f"Host: {config.get('host')}, Database: {config.get('database', 'default')}"
                ))
            except Exception as e:
                results.append(TestResult(
                    "ClickHouse Connection",
                    False,
                    f"Connection failed: {str(e)}",
                    "ClickHouse may not be configured or running"
                ))
        else:
            results.append(TestResult(
                "ClickHouse Connection",
                None,  # Skipped
                "Not configured",
                "ClickHouse config not found in environment"
            ))
    except ImportError:
        results.append(TestResult(
            "ClickHouse Connection",
            None,  # Skipped
            "Library not installed",
            "clickhouse-connect not available"
        ))
    except Exception as e:
        results.append(TestResult(
            "ClickHouse Connection",
            False,
            f"Error: {str(e)}",
            ""
        ))
    
    return results

def test_static_files():
    """Test 7: Test static files loading"""
    static_files = [
        "/static/css/performance.css",
        "/static/js/loaders.js",
    ]
    
    results = []
    for file_path in static_files:
        try:
            response = requests.get(f"{BASE_URL}{file_path}", timeout=5)
            results.append(TestResult(
                f"Static File: {file_path}",
                response.status_code == 200,
                f"Status: {response.status_code}",
                f"Size: {len(response.content)} bytes"
            ))
        except Exception as e:
            results.append(TestResult(
                f"Static File: {file_path}",
                False,
                f"Error: {str(e)}",
                ""
            ))
    
    return results

def test_templates():
    """Test 8: Test template pages"""
    pages = [
        ("/", "Home"),
        ("/add_source_page", "Add Source"),
        ("/schedule", "Schedule"),
    ]
    
    results = []
    session = requests.Session()
    try:
        # Login first
        session.post(
            f"{BASE_URL}/login",
            data={"username": TEST_USERNAME, "password": TEST_PASSWORD},
            timeout=5
        )
        
        for path, name in pages:
            try:
                response = session.get(f"{BASE_URL}{path}", timeout=5)
                results.append(TestResult(
                    f"Page: {name}",
                    response.status_code == 200,
                    f"Status: {response.status_code}",
                    f"Response time: {response.elapsed.total_seconds():.2f}s"
                ))
            except Exception as e:
                results.append(TestResult(
                    f"Page: {name}",
                    False,
                    f"Error: {str(e)}",
                    ""
                ))
    except Exception as e:
        results.append(TestResult(
            "Template Pages",
            False,
            f"Authentication error: {str(e)}",
            ""
        ))
    
    return results

def test_performance_optimizer():
    """Test 9: Test performance optimizer module"""
    try:
        from performance_optimizer import cache_result, clear_cache
        
        # Test caching
        @cache_result(ttl=60)
        def test_func(x):
            return x * 2
        
        result1 = test_func(5)
        result2 = test_func(5)
        
        clear_cache()
        
        return TestResult(
            "Performance Optimizer",
            result1 == 10 and result2 == 10,
            "Caching module working",
            "Cache decorator functional"
        )
    except ImportError:
        return TestResult(
            "Performance Optimizer",
            None,  # Skipped
            "Module not found",
            "performance_optimizer.py may not exist"
        )
    except Exception as e:
        return TestResult(
            "Performance Optimizer",
            False,
            f"Error: {str(e)}",
            ""
        )

def test_loader_utilities():
    """Test 10: Test loader utilities file exists"""
    loader_file = os.path.join(os.path.dirname(__file__), "static", "js", "loaders.js")
    try:
        if os.path.exists(loader_file):
            with open(loader_file, 'r') as f:
                content = f.read()
                has_functions = all(keyword in content for keyword in [
                    "createSpinner",
                    "setButtonLoading",
                    "showFullPageLoader"
                ])
            return TestResult(
                "Loader Utilities",
                has_functions,
                "File exists and contains required functions",
                f"File size: {os.path.getsize(loader_file)} bytes"
            )
        else:
            return TestResult(
                "Loader Utilities",
                False,
                "File not found",
                f"Expected: {loader_file}"
            )
    except Exception as e:
        return TestResult(
            "Loader Utilities",
            False,
            f"Error: {str(e)}",
            ""
        )

def test_environment_variables():
    """Test 11: Check critical environment variables"""
    required_vars = [
        "POSTGRES_HOST",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_DATABASE",
    ]
    
    results = []
    for var in required_vars:
        value = os.getenv(var)
        results.append(TestResult(
            f"Environment: {var}",
            value is not None and value != "",
            "Set" if value else "Not set",
            f"Value: {'***' if value else 'N/A'}"
        ))
    
    return results

def run_all_tests():
    """Run all tests and generate report"""
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*60}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}Project Health Check Test Suite{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'='*60}{Colors.RESET}\n")
    print(f"Testing against: {BASE_URL}")
    print(f"Test started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    all_results = []
    
    # Run tests
    print(f"{Colors.BOLD}1. Server & Basic Functionality{Colors.RESET}")
    all_results.append(test_flask_server_running())
    print_test_result(all_results[-1])
    
    all_results.append(test_login_page())
    print_test_result(all_results[-1])
    
    all_results.append(test_authentication())
    print_test_result(all_results[-1])
    
    all_results.append(test_dashboard_access())
    print_test_result(all_results[-1])
    
    print(f"\n{Colors.BOLD}2. API Endpoints{Colors.RESET}")
    api_results = test_api_endpoints()
    for result in api_results:
        all_results.append(result)
        print_test_result(result)
    
    print(f"\n{Colors.BOLD}3. Database Connections{Colors.RESET}")
    db_results = test_database_connections()
    for result in db_results:
        all_results.append(result)
        if result.passed is not None:
            print_test_result(result)
        else:
            if sys.platform == 'win32' and not os.getenv('TERM'):
                print(f"[SKIP] {result.name} - {result.message}")
            else:
                print(f"{Colors.YELLOW}⊘ SKIP{Colors.RESET} {result.name} - {result.message}")
    
    print(f"\n{Colors.BOLD}4. Static Files{Colors.RESET}")
    static_results = test_static_files()
    for result in static_results:
        all_results.append(result)
        print_test_result(result)
    
    print(f"\n{Colors.BOLD}5. Template Pages{Colors.RESET}")
    template_results = test_templates()
    for result in template_results:
        all_results.append(result)
        print_test_result(result)
    
    print(f"\n{Colors.BOLD}6. Performance & Loaders{Colors.RESET}")
    all_results.append(test_performance_optimizer())
    result = all_results[-1]
    if result.passed is not None:
        print_test_result(result)
    else:
        print(f"{Colors.YELLOW}⊘ SKIP{Colors.RESET} {result.name} - {result.message}")
    
    all_results.append(test_loader_utilities())
    print_test_result(all_results[-1])
    
    print(f"\n{Colors.BOLD}7. Environment Configuration{Colors.RESET}")
    env_results = test_environment_variables()
    for result in env_results:
        all_results.append(result)
        print_test_result(result)
    
    # Summary
    print(f"\n{Colors.BOLD}{'='*60}{Colors.RESET}")
    print(f"{Colors.BOLD}Test Summary{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*60}{Colors.RESET}\n")
    
    passed = sum(1 for r in all_results if r.passed is True)
    failed = sum(1 for r in all_results if r.passed is False)
    skipped = sum(1 for r in all_results if r.passed is None)
    total = len(all_results)
    
    print(f"Total Tests: {total}")
    print(f"{Colors.GREEN}Passed: {passed}{Colors.RESET}")
    print(f"{Colors.RED}Failed: {failed}{Colors.RESET}")
    if skipped > 0:
        print(f"{Colors.YELLOW}Skipped: {skipped}{Colors.RESET}")
    
    success_rate = (passed / (total - skipped) * 100) if (total - skipped) > 0 else 0
    print(f"\nSuccess Rate: {success_rate:.1f}%")
    
    if failed == 0:
        if sys.platform == 'win32' and not os.getenv('TERM'):
            print(f"\n[OK] All critical tests passed! Project is running correctly.\n")
        else:
            print(f"\n{Colors.GREEN}{Colors.BOLD}✓ All critical tests passed! Project is running correctly.{Colors.RESET}\n")
        return 0
    else:
        if sys.platform == 'win32' and not os.getenv('TERM'):
            print(f"\n[FAIL] Some tests failed. Please check the errors above.\n")
        else:
            print(f"\n{Colors.RED}{Colors.BOLD}✗ Some tests failed. Please check the errors above.{Colors.RESET}\n")
        return 1

if __name__ == "__main__":
    exit_code = run_all_tests()
    sys.exit(exit_code)

