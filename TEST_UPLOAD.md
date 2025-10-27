# Test Multi-File Upload Feature

## Quick Test Instructions

### 1. Create Test Files

Create these test files in a folder:

**test_data1.csv:**
```csv
id,name,email,created_at
1,John Doe,john@example.com,2024-01-15
2,Jane Smith,jane@example.com,2024-01-16
3,Bob Johnson,bob@example.com,2024-01-17
```

**test_data2.csv:**
```csv
product_id,product_name,price,stock
101,Widget A,29.99,150
102,Widget B,39.99,200
103,Widget C,49.99,75
```

**test_data3.xlsx:**
(Create in Excel with this content)
```
order_id | customer_id | amount | order_date
1        | 1           | 299.99 | 2024-10-01
2        | 2           | 499.99 | 2024-10-02
3        | 3           | 199.99 | 2024-10-03
```

### 2. Start the Application

```powershell
# Activate virtual environment
.\venv\Scripts\Activate.ps1

# Install openpyxl (if not already installed)
pip install openpyxl

# Start the application
python app.py
```

### 3. Test Upload

1. Open browser: http://127.0.0.1:5001
2. Login as Admin or Operator
3. Click "Upload Files" in sidebar
4. Select target PostgreSQL database
5. Upload all 3 test files at once
6. Verify results

### 4. Expected Results

✅ **test_data1.csv:**
- Table created: `test_data1`
- 3 rows inserted
- Message: "Successfully loaded to table 'public.test_data1' (3 rows)"

✅ **test_data2.csv:**
- Table created: `test_data2`
- 3 rows inserted
- Message: "Successfully loaded to table 'public.test_data2' (3 rows)"

✅ **test_data3.xlsx:**
- Table created: `test_data3`
- 3 rows inserted
- Message: "Successfully loaded to table 'public.test_data3' (3 rows)"

### 5. Verify in PostgreSQL

```sql
-- Check tables were created
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public' 
AND table_name IN ('test_data1', 'test_data2', 'test_data3');

-- View data
SELECT * FROM public.test_data1;
SELECT * FROM public.test_data2;
SELECT * FROM public.test_data3;

-- Check row counts
SELECT 
  'test_data1' as table_name, COUNT(*) as rows FROM test_data1
UNION ALL
SELECT 'test_data2', COUNT(*) FROM test_data2
UNION ALL
SELECT 'test_data3', COUNT(*) FROM test_data3;
```

### 6. Test Scenarios

#### Test A: Single CSV File
- Upload only `test_data1.csv`
- Expected: Success with 3 rows

#### Test B: Single Excel File
- Upload only `test_data3.xlsx`
- Expected: Success with 3 rows

#### Test C: Multiple CSV Files
- Upload `test_data1.csv` and `test_data2.csv`
- Expected: Both succeed, 2 tables created

#### Test D: Mixed CSV + Excel
- Upload all 3 files together
- Expected: All 3 succeed, 3 tables created

#### Test E: Drag and Drop
- Drag all 3 files from explorer to upload zone
- Expected: Files appear in preview, upload succeeds

#### Test F: File Preview
- Select files but don't upload
- Remove `test_data2.csv` from preview
- Upload remaining 2 files
- Expected: Only 2 tables created

#### Test G: Replace Existing Table
- Upload `test_data1.csv` twice
- Expected: First upload creates table, second replaces it (data unchanged since same file)

#### Test H: Invalid File Type
- Try uploading a `.txt` or `.pdf` file
- Expected: Error message "File type not allowed"

### 7. Cleanup After Testing

```sql
-- Drop test tables
DROP TABLE IF EXISTS test_data1;
DROP TABLE IF EXISTS test_data2;
DROP TABLE IF EXISTS test_data3;
```

---

## Automated Test Script (Optional)

Create `test_upload.py`:

```python
import requests
import os

# Configuration
BASE_URL = 'http://127.0.0.1:5001'
LOGIN_URL = f'{BASE_URL}/login'
UPLOAD_URL = f'{BASE_URL}/upload'

# Login credentials (change these)
USERNAME = 'admin'
PASSWORD = 'your_password'

# Test database
TARGET_DB = 'test1'

def test_upload():
    # Create session
    session = requests.Session()
    
    # Login
    login_data = {
        'username': USERNAME,
        'password': PASSWORD
    }
    response = session.post(LOGIN_URL, data=login_data)
    
    if response.status_code != 200:
        print("❌ Login failed")
        return
    
    print("✅ Login successful")
    
    # Prepare files
    files = [
        ('files', ('test_data1.csv', open('test_data1.csv', 'rb'), 'text/csv')),
        ('files', ('test_data2.csv', open('test_data2.csv', 'rb'), 'text/csv')),
        ('files', ('test_data3.xlsx', open('test_data3.xlsx', 'rb'), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'))
    ]
    
    # Upload
    data = {'pg_database': TARGET_DB}
    response = session.post(UPLOAD_URL, files=files, data=data)
    
    # Close files
    for _, file_tuple in files:
        file_tuple[1].close()
    
    if response.status_code == 200:
        print("✅ Upload successful")
        print(response.text)
    else:
        print(f"❌ Upload failed: {response.status_code}")
        print(response.text)

if __name__ == '__main__':
    test_upload()
```

Run:
```powershell
python test_upload.py
```

---

## Troubleshooting Test Issues

### Issue: "openpyxl not found"
```powershell
pip install openpyxl
```

### Issue: "No PostgreSQL databases found"
- Check `.env` file has correct PostgreSQL credentials
- Ensure PostgreSQL is running
- Verify database exists:
  ```sql
  SELECT datname FROM pg_database;
  ```

### Issue: "Permission denied"
- Login as Admin or Operator role (not Viewer)

### Issue: Excel file won't upload
- Make sure file has `.xlsx` or `.xls` extension
- Try saving from Excel again (might be corrupted)
- Check file isn't open in Excel

### Issue: Progress bar stuck
- Wait patiently (large files take time)
- Check browser console (F12) for JavaScript errors
- Refresh page and try again

---

## Success Criteria

✅ All 3 test files upload successfully  
✅ 3 tables created in PostgreSQL  
✅ Correct number of rows in each table  
✅ Progress bar shows during upload  
✅ Detailed results displayed after upload  
✅ File preview shows selected files  
✅ Drag and drop works  
✅ Can remove files before upload  
✅ Error handling works for invalid files  

---

**Ready to test!** 🧪
