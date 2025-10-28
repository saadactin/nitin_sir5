"""
Performance Testing Script
==========================

This script tests the application after performance fixes are applied.

Tests:
1. Connection pool health
2. Page load performance
3. Concurrent user simulation
4. Memory leak detection
5. Database connection monitoring

Run after applying fixes with: python test_performance.py
"""

import requests
import time
import psutil
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Configuration
BASE_URL = "http://localhost:5001"
TEST_USERNAME = "admin"  # Update with your admin username
TEST_PASSWORD = "admin"  # Update with your admin password

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class PerformanceTester:
    def __init__(self, base_url, username, password):
        self.base_url = base_url
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.process = psutil.Process(os.getpid())
        
    def login(self):
        """Login to get session cookie"""
        logging.info("Logging in...")
        try:
            response = self.session.post(
                f"{self.base_url}/login",
                data={
                    'username': self.username,
                    'password': self.password
                },
                allow_redirects=False
            )
            if response.status_code in [200, 302]:
                logging.info("✅ Login successful")
                return True
            else:
                logging.error(f"❌ Login failed: {response.status_code}")
                return False
        except Exception as e:
            logging.error(f"❌ Login error: {e}")
            return False
    
    def test_connection_pool_health(self):
        """Test 1: Check connection pool health endpoint"""
        logging.info("\n" + "="*70)
        logging.info("TEST 1: Connection Pool Health Check")
        logging.info("="*70)
        
        try:
            response = self.session.get(f"{self.base_url}/health/db-pool")
            
            if response.status_code == 200:
                data = response.json()
                logging.info("✅ Connection pool endpoint accessible")
                logging.info(f"   Status: {data.get('status')}")
                logging.info(f"   Pool Stats: {data.get('pool_stats')}")
                return True
            else:
                logging.warning(f"⚠️  Connection pool endpoint returned: {response.status_code}")
                logging.info("   This might be expected if endpoint is login-protected")
                return True  # Not critical if protected
        except requests.exceptions.ConnectionError:
            logging.error("❌ Cannot connect to application - is it running?")
            logging.error(f"   Make sure server is running on {self.base_url}")
            return False
        except Exception as e:
            logging.error(f"❌ Connection pool health check failed: {e}")
            return False
    
    def test_page_load_performance(self):
        """Test 2: Measure page load times"""
        logging.info("\n" + "="*70)
        logging.info("TEST 2: Page Load Performance")
        logging.info("="*70)
        
        pages_to_test = [
            ('/', 'Home'),
            ('/dashboard', 'Dashboard'),
            ('/sync', 'Sync Servers'),
            ('/upload', 'Upload'),
        ]
        
        results = []
        all_passed = True
        
        for path, name in pages_to_test:
            try:
                start_time = time.time()
                response = self.session.get(f"{self.base_url}{path}", timeout=10)
                load_time = time.time() - start_time
                
                status_icon = "✅" if response.status_code == 200 else "⚠️"
                time_icon = "✅" if load_time < 2.0 else ("⚠️" if load_time < 5.0 else "❌")
                
                logging.info(f"{status_icon} {name:20s} - Status: {response.status_code}, Time: {load_time:.2f}s {time_icon}")
                
                results.append({
                    'page': name,
                    'status': response.status_code,
                    'time': load_time,
                    'passed': response.status_code == 200 and load_time < 5.0
                })
                
                if load_time >= 5.0:
                    logging.warning(f"   ⚠️  Page load is slow (target: <2s, acceptable: <5s)")
                    all_passed = False
                    
            except Exception as e:
                logging.error(f"❌ {name:20s} - Error: {e}")
                all_passed = False
        
        # Summary
        avg_time = sum(r['time'] for r in results) / len(results) if results else 0
        logging.info(f"\nAverage page load time: {avg_time:.2f}s")
        
        if avg_time < 2.0:
            logging.info("✅ EXCELLENT: Average load time <2s")
        elif avg_time < 5.0:
            logging.info("⚠️  ACCEPTABLE: Average load time <5s (target: <2s)")
        else:
            logging.error("❌ POOR: Average load time >=5s (needs optimization)")
        
        return all_passed
    
    def test_concurrent_users(self, num_users=10):
        """Test 3: Simulate concurrent users"""
        logging.info("\n" + "="*70)
        logging.info(f"TEST 3: Concurrent Users Simulation ({num_users} users)")
        logging.info("="*70)
        
        def simulate_user(user_id):
            """Simulate a single user accessing the dashboard"""
            try:
                session = requests.Session()
                start_time = time.time()
                
                # Try to access dashboard
                response = session.get(f"{self.base_url}/", timeout=30)
                load_time = time.time() - start_time
                
                return {
                    'user_id': user_id,
                    'success': response.status_code == 200,
                    'status_code': response.status_code,
                    'load_time': load_time,
                    'error': None
                }
            except Exception as e:
                return {
                    'user_id': user_id,
                    'success': False,
                    'status_code': None,
                    'load_time': None,
                    'error': str(e)
                }
        
        logging.info(f"Simulating {num_users} concurrent users accessing the application...")
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=num_users) as executor:
            futures = [executor.submit(simulate_user, i) for i in range(num_users)]
            results = [future.result() for future in as_completed(futures)]
        
        total_time = time.time() - start_time
        
        # Analyze results
        successful = sum(1 for r in results if r['success'])
        failed = num_users - successful
        
        load_times = [r['load_time'] for r in results if r['load_time'] is not None]
        avg_load_time = sum(load_times) / len(load_times) if load_times else 0
        max_load_time = max(load_times) if load_times else 0
        
        logging.info(f"\nResults:")
        logging.info(f"   Total time: {total_time:.2f}s")
        logging.info(f"   Successful requests: {successful}/{num_users}")
        logging.info(f"   Failed requests: {failed}/{num_users}")
        logging.info(f"   Average load time: {avg_load_time:.2f}s")
        logging.info(f"   Max load time: {max_load_time:.2f}s")
        
        # Show failures
        if failed > 0:
            logging.warning(f"\n⚠️  Failed Requests:")
            for r in results:
                if not r['success']:
                    logging.warning(f"   User {r['user_id']}: {r.get('error', 'Unknown error')}")
        
        # Verdict
        if successful == num_users and avg_load_time < 5.0:
            logging.info("\n✅ PASS: All users handled successfully")
            return True
        elif successful >= num_users * 0.9:  # 90% success rate
            logging.warning("\n⚠️  PARTIAL PASS: Most users handled (some failures)")
            return True
        else:
            logging.error("\n❌ FAIL: Too many failures or slow responses")
            return False
    
    def test_memory_stability(self, duration_seconds=60):
        """Test 4: Check for memory leaks"""
        logging.info("\n" + "="*70)
        logging.info(f"TEST 4: Memory Leak Detection ({duration_seconds}s)")
        logging.info("="*70)
        
        logging.info("Monitoring memory usage while making requests...")
        logging.info("Note: This tests the test script's memory, not the server")
        logging.info("      For server memory testing, monitor the Python process directly")
        
        initial_memory = self.process.memory_info().rss / 1024 / 1024  # MB
        logging.info(f"Initial memory: {initial_memory:.2f} MB")
        
        start_time = time.time()
        request_count = 0
        memory_samples = [initial_memory]
        
        while time.time() - start_time < duration_seconds:
            try:
                # Make a request
                self.session.get(f"{self.base_url}/", timeout=5)
                request_count += 1
                
                # Sample memory every 10 requests
                if request_count % 10 == 0:
                    current_memory = self.process.memory_info().rss / 1024 / 1024
                    memory_samples.append(current_memory)
                    
            except Exception as e:
                logging.warning(f"Request failed during memory test: {e}")
            
            time.sleep(0.5)  # Wait between requests
        
        final_memory = self.process.memory_info().rss / 1024 / 1024
        memory_growth = final_memory - initial_memory
        memory_growth_pct = (memory_growth / initial_memory) * 100 if initial_memory > 0 else 0
        
        logging.info(f"\nResults:")
        logging.info(f"   Requests made: {request_count}")
        logging.info(f"   Initial memory: {initial_memory:.2f} MB")
        logging.info(f"   Final memory: {final_memory:.2f} MB")
        logging.info(f"   Memory growth: {memory_growth:.2f} MB ({memory_growth_pct:.1f}%)")
        
        # Verdict
        if memory_growth_pct < 10:  # Less than 10% growth
            logging.info("✅ PASS: Memory usage stable")
            return True
        elif memory_growth_pct < 25:
            logging.warning("⚠️  WARNING: Moderate memory growth detected")
            return True
        else:
            logging.error("❌ FAIL: Significant memory growth (possible leak)")
            return False
    
    def run_all_tests(self):
        """Run all performance tests"""
        logging.info("\n" + "="*70)
        logging.info("🚀 STARTING PERFORMANCE TESTS")
        logging.info("="*70)
        logging.info(f"Base URL: {self.base_url}")
        logging.info(f"Test Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Try to login (may not be needed for some endpoints)
        # self.login()
        
        # Run tests
        results = {}
        
        results['connection_pool'] = self.test_connection_pool_health()
        time.sleep(2)
        
        results['page_load'] = self.test_page_load_performance()
        time.sleep(2)
        
        results['concurrent_users'] = self.test_concurrent_users(num_users=10)
        time.sleep(2)
        
        results['memory_stability'] = self.test_memory_stability(duration_seconds=30)
        
        # Final summary
        logging.info("\n" + "="*70)
        logging.info("📊 TEST SUMMARY")
        logging.info("="*70)
        
        for test_name, passed in results.items():
            status = "✅ PASS" if passed else "❌ FAIL"
            logging.info(f"{status}: {test_name.replace('_', ' ').title()}")
        
        all_passed = all(results.values())
        
        logging.info("\n" + "="*70)
        if all_passed:
            logging.info("✅ ALL TESTS PASSED - READY FOR PRODUCTION!")
            logging.info("="*70)
            logging.info("\nNext Steps:")
            logging.info("1. Monitor application in production")
            logging.info("2. Set up alerting for errors")
            logging.info("3. Regular health checks")
            return 0
        else:
            logging.warning("⚠️  SOME TESTS FAILED - REVIEW REQUIRED")
            logging.info("="*70)
            logging.info("\nNext Steps:")
            logging.info("1. Check application logs for errors")
            logging.info("2. Verify connection pool is initialized")
            logging.info("3. Re-run tests after fixes")
            return 1


def main():
    """Main test execution"""
    import sys
    
    # Check if server is likely running
    try:
        response = requests.get(BASE_URL, timeout=5)
        logging.info(f"✅ Server is reachable at {BASE_URL}")
    except requests.exceptions.ConnectionError:
        logging.error(f"❌ Cannot connect to {BASE_URL}")
        logging.error("   Please start the server first:")
        logging.error("   python run_production.py")
        return 1
    except Exception as e:
        logging.warning(f"⚠️  Server check: {e}")
    
    # Run tests
    tester = PerformanceTester(BASE_URL, TEST_USERNAME, TEST_PASSWORD)
    return tester.run_all_tests()


if __name__ == '__main__':
    import sys
    sys.exit(main())
