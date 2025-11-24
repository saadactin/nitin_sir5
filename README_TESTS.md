# Project Health Check Tests

Quick reference for running tests to verify the project is working.

## Quick Test (30 seconds)

```bash
python test_quick_check.py
```

## Full Test Suite (2 minutes)

```bash
python test_project_health.py
```

## What Gets Tested

✅ Flask server is running  
✅ Login page loads  
✅ User authentication works  
✅ Dashboard is accessible  
✅ API endpoints respond  
✅ Database connections work  
✅ Static files load  
✅ Templates render  
✅ Performance modules exist  
✅ Loader utilities available  

## Before Running Tests

1. **Start Flask server:**
   ```bash
   python app.py
   # Server should be on http://localhost:5001
   ```

2. **Verify environment:**
   - `.env` file exists with database credentials
   - PostgreSQL is running
   - Admin user exists (default: admin/admin)

## Test Results

- **✓ PASS** = Working correctly
- **✗ FAIL** = Needs attention
- **⊘ SKIP** = Optional feature not configured

## Troubleshooting

**Server not running?**
```bash
python app.py
```

**Database connection failed?**
- Check PostgreSQL is running
- Verify `.env` credentials

**Authentication failed?**
- Default: username=`admin`, password=`admin`
- Or set: `export TEST_USERNAME=your_username`
- Or set: `export TEST_PASSWORD=your_password`

## Windows Users

Use the batch file:
```cmd
run_tests.bat
```

## Linux/Mac Users

Use the shell script:
```bash
chmod +x run_tests.sh
./run_tests.sh
```

For detailed documentation, see `TESTING_GUIDE.md`

