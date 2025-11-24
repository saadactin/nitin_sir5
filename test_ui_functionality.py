"""
UI Functionality Tests
Tests UI components, loaders, and frontend functionality
"""

import sys
import os
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

BASE_URL = os.getenv('FLASK_BASE_URL', 'http://localhost:5001')
TEST_USERNAME = os.getenv('TEST_USERNAME', 'admin')
TEST_PASSWORD = os.getenv('TEST_PASSWORD', 'admin')

def test_ui_with_selenium():
    """Test UI functionality using Selenium"""
    print("\n" + "="*50)
    print("UI FUNCTIONALITY TESTS")
    print("="*50 + "\n")
    
    try:
        # Try to initialize browser (headless Chrome)
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        driver = webdriver.Chrome(options=options)
    except WebDriverException:
        print("⚠ Selenium/ChromeDriver not available. Skipping UI tests.")
        print("Install: pip install selenium")
        print("Download ChromeDriver: https://chromedriver.chromium.org/")
        return 0
    
    results = []
    
    try:
        # Test 1: Login page loads
        print("Testing login page...")
        driver.get(f"{BASE_URL}/login")
        wait = WebDriverWait(driver, 10)
        
        try:
            username_field = wait.until(EC.presence_of_element_located((By.NAME, "username")))
            password_field = driver.find_element(By.NAME, "password")
            results.append(("Login Form Elements", True))
            print("  ✓ Login form elements found")
        except TimeoutException:
            results.append(("Login Form Elements", False))
            print("  ✗ Login form elements not found")
        
        # Test 2: Login functionality
        print("Testing login...")
        try:
            username_field.send_keys(TEST_USERNAME)
            password_field.send_keys(TEST_PASSWORD)
            login_button = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
            login_button.click()
            
            # Wait for redirect
            wait.until(lambda d: "dashboard" in d.current_url.lower() or "index" in d.current_url.lower())
            results.append(("Login Functionality", True))
            print("  ✓ Login successful")
        except TimeoutException:
            results.append(("Login Functionality", False))
            print("  ✗ Login failed or timeout")
        
        # Test 3: Dashboard loads
        print("Testing dashboard...")
        try:
            driver.get(f"{BASE_URL}/dashboard")
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            results.append(("Dashboard Loads", True))
            print("  ✓ Dashboard loaded")
        except:
            results.append(("Dashboard Loads", False))
            print("  ✗ Dashboard failed to load")
        
        # Test 4: Check for loader utilities
        print("Testing loader utilities...")
        try:
            driver.execute_script("return typeof window.LoaderUtils !== 'undefined'")
            has_loaders = driver.execute_script("return typeof window.LoaderUtils !== 'undefined'")
            results.append(("Loader Utilities Available", has_loaders))
            if has_loaders:
                print("  ✓ Loader utilities found")
            else:
                print("  ✗ Loader utilities not found")
        except:
            results.append(("Loader Utilities Available", False))
            print("  ✗ Error checking loader utilities")
        
        # Test 5: Check for performance CSS
        print("Testing performance CSS...")
        try:
            driver.get(f"{BASE_URL}/dashboard")
            stylesheets = driver.find_elements(By.TAG_NAME, "link")
            has_perf_css = any("performance.css" in link.get_attribute("href") for link in stylesheets)
            results.append(("Performance CSS Loaded", has_perf_css))
            if has_perf_css:
                print("  ✓ Performance CSS found")
            else:
                print("  ✗ Performance CSS not found")
        except:
            results.append(("Performance CSS Loaded", False))
            print("  ✗ Error checking CSS")
        
    finally:
        driver.quit()
    
    # Summary
    print("\n" + "="*50)
    passed = sum(1 for _, status in results if status)
    total = len(results)
    print(f"Results: {passed}/{total} passed")
    
    return 0 if passed == total else 1

if __name__ == "__main__":
    sys.exit(test_ui_with_selenium())

