# Testing Guide

This guide explains how to test the project to verify it's running correctly.

## Quick Start

### Option 1: Quick Health Check (Fast)
```bash
python test_quick_check.py
```
This runs a minimal check to verify:
- Server is running
- Login page is accessible
- Database connection works

### Option 2: Full Test Suite (Comprehensive)
```bash
python test_project_health.py
```
This runs comprehensive tests covering:
- Server functionality
- Authentication
- API endpoints
- Database connections
- Static files
- Template pages
- Performance modules
- Loader utilities
- Environment configuration

### Option 3: Using Test Runner Scripts

**Windows:**
```cmd
run_tests.bat
```

**Linux/Mac:**
```bash
chmod +x run_tests.sh
./run_tests.sh
```

## Test Files

### 1. `test_quick_check.py`
- **Purpose**: Fast verification that project is running
- **Duration**: ~5 seconds
- **Use Case**: Quick status check before deployment

### 2. `test_project_health.py`
- **Purpose**: Comprehensive health check
- **Duration**: ~30-60 seconds
- **Use Case**: Full system verification

### 3. `test_ui_functionality.py`
- **Purpose**: UI and frontend testing (requires Selenium)
- **Duration**: ~1-2 minutes
- **Use Case**: Verify UI components and loaders work

## Configuration

### Environment Variables

Set these environment variables to customize test behavior:

```bash
# Flask server URL (default: http://localhost:5001)
export FLASK_BASE_URL=http://localhost:5001

# Test credentials (default: admin/admin)
export TEST_USERNAME=admin
export TEST_PASSWORD=admin
```

### Windows
```cmd
set FLASK_BASE_URL=http://localhost:5001
set TEST_USERNAME=admin
set TEST_PASSWORD=admin
```

## Test Categories

### 1. Server & Basic Functionality
- ✓ Flask server running
- ✓ Login page accessible
- ✓ User authentication
- ✓ Dashboard access

### 2. API Endpoints
- ✓ Target databases API (PostgreSQL)
- ✓ Target databases API (ClickHouse)
- ✓ Dashboard data API

### 3. Database Connections
- ✓ PostgreSQL connection
- ✓ ClickHouse connection (if configured)

### 4. Static Files
- ✓ Performance CSS
- ✓ Loader utilities JavaScript

### 5. Template Pages
- ✓ Home page
- ✓ Add Source page
- ✓ Schedule page

### 6. Performance & Loaders
- ✓ Performance optimizer module
- ✓ Loader utilities file

### 7. Environment Configuration
- ✓ PostgreSQL environment variables
- ✓ Required configuration present

## Understanding Test Results

### Status Indicators

- **✓ PASS** (Green): Test passed successfully
- **✗ FAIL** (Red): Test failed - needs attention
- **⊘ SKIP** (Yellow): Test skipped (optional feature not configured)

### Exit Codes

- **0**: All tests passed
- **1**: One or more tests failed

## Troubleshooting

### Server Not Running
```
Error: Cannot connect to server
```
**Solution**: Start Flask server
```bash
python app.py
# or
flask run --port=5001
```

### Database Connection Failed
```
Error: Connection failed
```
**Solution**: 
1. Check PostgreSQL is running
2. Verify `.env` file has correct credentials
3. Check firewall/network settings

### Authentication Failed
```
Error: Login failed
```
**Solution**:
1. Verify test credentials in environment variables
2. Check if admin user exists
3. Run: `python -c "from auth import init_admin_user; init_admin_user()"`

### Missing Dependencies
```
Error: Module not found
```
**Solution**: Install required packages
```bash
pip install requests
# For UI tests:
pip install selenium
```

## Continuous Integration

### GitHub Actions Example

```yaml
name: Health Check
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.9'
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install requests
      - name: Start Flask server
        run: |
          python app.py &
          sleep 5
      - name: Run health checks
        run: python test_project_health.py
        env:
          FLASK_BASE_URL: http://localhost:5001
```

## Best Practices

1. **Run tests before deployment**: Always run full test suite before deploying
2. **Fix failures immediately**: Don't ignore test failures
3. **Update tests**: Add new tests when adding features
4. **Monitor regularly**: Run quick check daily, full suite weekly
5. **Document failures**: Keep track of common issues and solutions

## Adding New Tests

To add a new test:

1. Open `test_project_health.py`
2. Create a new test function:
```python
def test_new_feature():
    """Test description"""
    try:
        # Your test code
        return TestResult(
            "Feature Name",
            True,  # or False
            "Success message",
            "Details"
        )
    except Exception as e:
        return TestResult(
            "Feature Name",
            False,
            f"Error: {str(e)}",
            ""
        )
```

3. Add to `run_all_tests()` function:
```python
all_results.append(test_new_feature())
print_test_result(all_results[-1])
```

## Performance Benchmarks

Expected response times:
- Server response: < 1 second
- Database query: < 0.5 seconds
- API endpoint: < 2 seconds
- Page load: < 3 seconds

If tests are slower, investigate performance issues.

## Support

For issues or questions:
1. Check test output for specific error messages
2. Review server logs
3. Verify environment configuration
4. Check database connectivity

## Example Output

```
============================================================
Project Health Check Test Suite
============================================================

Testing against: http://localhost:5001
Test started at: 2024-01-15 10:30:00

1. Server & Basic Functionality
✓ PASS Flask Server Running
    Status: 200
    Details: Response time: 0.15s
✓ PASS Login Page Accessible
    Status: 200
    Details: Login page content found
✓ PASS User Authentication
    Status: 302
    Details: Redirected to: /dashboard
✓ PASS Dashboard Access
    Status: 200
    Details: Dashboard content loaded

...

============================================================
Test Summary
============================================================

Total Tests: 25
Passed: 23
Failed: 2
Skipped: 0

Success Rate: 92.0%

✓ All critical tests passed! Project is running correctly.
```

