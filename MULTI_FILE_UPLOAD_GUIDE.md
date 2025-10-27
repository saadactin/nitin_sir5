# 📤 Multi-File Upload Feature - Complete Guide

## 🎯 Feature Overview

The **Multi-File Upload** feature allows you to upload multiple CSV and Excel files simultaneously to your PostgreSQL database with visual progress tracking and detailed results.

### ✨ Key Features:
- ✅ **Multiple File Upload** - Upload any number of files at once
- ✅ **CSV & Excel Support** - Supports `.csv`, `.xlsx`, and `.xls` files
- ✅ **Drag & Drop Interface** - Modern file selection with drag-and-drop
- ✅ **Visual Progress Bar** - Real-time upload progress tracking
- ✅ **Detailed Results** - See success/failure status for each file
- ✅ **File Preview** - Review selected files before uploading
- ✅ **Automatic Table Creation** - Each file creates a separate table
- ✅ **Safe Replacements** - Existing tables are replaced (not merged)

---

## 🚀 How to Use

### Step 1: Access Upload Page

1. Login to the dashboard
2. Click **"Upload Files"** in the sidebar (or navigate to `/upload`)
3. Ensure you have **Admin** or **Operator** role

### Step 2: Select Target Database

1. Choose the **Target PostgreSQL Database** from the dropdown
2. This is where all uploaded files will be loaded
3. All files in a single upload session go to the same database

### Step 3: Select Files

**Method A: Click to Browse**
1. Click the upload dropzone area
2. Browse and select files from your computer
3. Hold `Ctrl` (Windows) or `Cmd` (Mac) to select multiple files

**Method B: Drag & Drop**
1. Open your file explorer
2. Select multiple CSV/Excel files
3. Drag them into the upload dropzone
4. Drop to add them

### Step 4: Review Selected Files

- See all selected files in the preview list
- Each file shows:
  - **File name**
  - **File type** (CSV or Excel badge)
  - **File size**
- Remove individual files by clicking the ❌ icon
- Clear all files with "Clear All" button

### Step 5: Upload & Track Progress

1. Click **"Upload & Sync"** button
2. Watch the progress bar fill as files are processed
3. See which file is currently being uploaded
4. Wait for completion (page will show results)

### Step 6: View Results

After upload completes, you'll see:
- ✅ **Success:** File uploaded, table created with row count
- ❌ **Failed:** Error message explaining what went wrong
- Summary flash message at the top

---

## 📊 Supported File Formats

### CSV Files (`.csv`)
- **Requirements:**
  - First row must contain column headers
  - UTF-8 or ASCII encoding recommended
  - Comma-separated values
  
- **Example:**
  ```csv
  id,name,email,created_at
  1,John Doe,john@example.com,2024-01-15
  2,Jane Smith,jane@example.com,2024-01-16
  ```

### Excel Files (`.xlsx`, `.xls`)
- **Requirements:**
  - Only the **first sheet** is imported
  - First row must contain column headers
  - Data should start from row 1 (no empty rows at top)
  
- **Supported:**
  - `.xlsx` (Excel 2007+)
  - `.xls` (Excel 97-2003)
  
- **Note:** Formulas are evaluated and only values are imported

---

## 🗄️ Table Creation Rules

### Table Naming Convention

1. **Based on filename:**
   - File: `sales_data.csv` → Table: `sales_data`
   - File: `Customer List 2024.xlsx` → Table: `customer_list_2024`

2. **Character sanitization:**
   - Removes special characters (except `_` and `-`)
   - Converts to lowercase (PostgreSQL convention)
   - Examples:
     - `Sales (Q1).csv` → `salesq1`
     - `Employee@Report#2024.xlsx` → `employeereport2024`

3. **Collision handling:**
   - If table already exists → **Replaced** (old data lost!)
   - No merge or append - complete replacement

### Column Data Types

- **Automatic detection:** Pandas infers data types from content
- **Type mapping:**
  - Numbers → `INTEGER` or `FLOAT`
  - Dates → `TIMESTAMP` or `DATE`
  - Text → `TEXT` or `VARCHAR`
  - Booleans → `BOOLEAN`

### Schema Location

- All tables created in **`public`** schema
- Access as: `public.table_name`
- Example query:
  ```sql
  SELECT * FROM public.sales_data;
  ```

---

## 📋 Upload Results

### Success Result Example:
```
✅ sales_data.csv
Successfully loaded to table 'public.sales_data' (150 rows)
```

### Failure Result Examples:

**Invalid File Type:**
```
❌ document.pdf
File type not allowed. Only CSV, XLS, XLSX are supported.
```

**Empty File:**
```
❌ empty_data.csv
Error: No columns to parse from file
```

**Corrupt Excel File:**
```
❌ corrupted.xlsx
Error: File is not a zip file
```

**Invalid CSV:**
```
❌ bad_encoding.csv
Error: 'utf-8' codec can't decode byte 0xff
```

---

## ⚠️ Important Notes

### Data Loss Warning
⚠️ **If a table with the same name exists, it will be REPLACED!**
- All existing data in that table will be **deleted**
- No backup is created automatically
- No merge or append operations

**Best Practice:** 
- Use unique filenames
- Or manually backup existing tables first:
  ```sql
  CREATE TABLE backup_sales_data AS SELECT * FROM sales_data;
  ```

### File Size Limits
- **Browser limit:** ~2GB per file (browser dependent)
- **Server limit:** Depends on Flask configuration
- **Recommendation:** Keep files under 100MB for best performance
- Large files may take several minutes to process

### Performance Considerations
- **10 files × 1MB each:** ~10-30 seconds
- **1 file × 50MB:** ~60-120 seconds
- **100 files × 100KB each:** ~60-90 seconds

Progress bar provides visual feedback during long uploads.

### Encoding Issues
If you encounter encoding errors:
1. Open CSV in a text editor
2. Save as UTF-8 encoding
3. Try upload again

Or use Excel:
1. Open CSV in Excel
2. Save As → Excel Workbook (.xlsx)
3. Upload the Excel file instead

---

## 🔧 Technical Details

### Backend Implementation

**File Processing Flow:**
```
1. Receive files from form (multipart/form-data)
2. Validate file extensions
3. For each file:
   - Read into pandas DataFrame
     - CSV: pd.read_csv()
     - Excel: pd.read_excel()
   - Sanitize filename to table name
   - Upload to PostgreSQL via SQLAlchemy
     - Schema: public
     - Mode: replace (if_exists='replace')
4. Collect results (success/failure)
5. Return summary and detailed results
```

**Progress Tracking:**
- Server-side progress stored in `upload_progress` dictionary
- Keyed by unique session ID
- Tracks:
  - Total files
  - Completed files
  - Current file being processed
  - Status (processing/complete/error)
  - Individual file results

**API Endpoint:**
```python
GET /api/upload-progress/<session_id>
Returns: JSON with progress details
```

### Frontend Features

**Drag & Drop:**
- HTML5 Drag and Drop API
- Visual feedback on dragover
- Supports multiple files

**File Preview:**
- Client-side file list rendering
- Remove individual files before upload
- File size display (KB)
- File type badges (CSV/Excel color-coded)

**Progress Bar:**
- CSS transitions for smooth animation
- Updates during upload
- Shows percentage and current file

---

## 🛠️ Troubleshooting

### Issue: "No PostgreSQL databases found"

**Cause:** No databases configured in system

**Solution:**
1. Check `config/db_connections.yaml`
2. Ensure PostgreSQL section has valid database
3. Or create database manually:
   ```sql
   CREATE DATABASE my_database;
   ```

---

### Issue: "File type not allowed"

**Cause:** Trying to upload unsupported file type

**Solution:**
- Only upload `.csv`, `.xlsx`, or `.xls` files
- Check file extension is correct
- Rename if needed (e.g., `.txt` → `.csv`)

---

### Issue: "Failed to read CSV: 'utf-8' codec can't decode"

**Cause:** CSV file has non-UTF-8 encoding

**Solution:**
1. **Option A:** Convert to UTF-8
   - Open in Notepad++
   - Encoding → Convert to UTF-8
   - Save
   
2. **Option B:** Use Excel
   - Open CSV in Excel
   - Save As → Excel Workbook (.xlsx)
   - Upload Excel file instead

---

### Issue: "Table already exists" (not an error, but data loss concern)

**Behavior:** System replaces existing table

**Prevention:**
1. **Backup first:**
   ```sql
   CREATE TABLE backup_mytable AS SELECT * FROM mytable;
   ```

2. **Use different filename:**
   - Instead of `sales.csv`
   - Use `sales_2024_10_27.csv`

3. **Manual merge (if needed):**
   ```sql
   -- After upload, merge with backup
   INSERT INTO sales 
   SELECT * FROM backup_sales 
   WHERE id NOT IN (SELECT id FROM sales);
   ```

---

### Issue: Upload seems stuck at 90%

**Cause:** Large file still processing on server

**Solution:**
- Wait patiently (may take several minutes for large files)
- Check browser console for errors (F12)
- If truly stuck (>10 minutes), refresh and try smaller files

---

### Issue: Excel file has multiple sheets

**Behavior:** Only first sheet is imported

**Solution:**
1. **Option A:** Split into multiple files
   - In Excel, copy each sheet to new workbook
   - Save each as separate file
   - Upload all files together

2. **Option B:** Rearrange sheets
   - Move desired sheet to first position
   - Save and upload

---

## 📊 Usage Examples

### Example 1: Upload Monthly Sales Reports

**Scenario:** You have 12 CSV files (one per month)

```
january_sales.csv
february_sales.csv
...
december_sales.csv
```

**Steps:**
1. Select all 12 files at once
2. Choose target database: `sales_db`
3. Click "Upload & Sync"
4. Result: 12 tables created:
   - `january_sales`
   - `february_sales`
   - ...
   - `december_sales`

**Query all months:**
```sql
SELECT 'January' as month, * FROM january_sales
UNION ALL
SELECT 'February' as month, * FROM february_sales
-- ... etc
```

---

### Example 2: Upload Mixed CSV and Excel Files

**Files:**
- `customers.csv` (10,000 rows)
- `orders.xlsx` (5,000 rows)
- `products.xls` (500 rows)

**Steps:**
1. Drag all 3 files into upload zone
2. Select database: `ecommerce_db`
3. Upload
4. Result: 3 tables with respective row counts

---

### Example 3: Replace Outdated Data

**Scenario:** Daily data refresh

**Files:**
- `daily_report_2024_10_27.csv`

**Steps:**
1. Rename file to: `daily_report.csv` (consistent name)
2. Upload to `analytics_db`
3. Table `daily_report` is replaced with today's data
4. Applications querying `daily_report` get fresh data

**Automation tip:** Schedule this upload with a script:
```python
import requests

files = {'files': open('daily_report.csv', 'rb')}
data = {'pg_database': 'analytics_db'}
response = requests.post('http://localhost:5001/upload', files=files, data=data)
```

---

## 🔒 Security & Permissions

### Role Requirements
- **Admin:** ✅ Full access to upload
- **Operator:** ✅ Full access to upload
- **Viewer:** ❌ Cannot upload files

### File Validation
- ✅ Extension check (only allowed types)
- ✅ Filename sanitization (removes dangerous characters)
- ✅ Uses `secure_filename()` from Werkzeug
- ❌ No file size limit enforcement (consider adding for production)

### SQL Injection Protection
- ✅ Uses SQLAlchemy ORM (parameterized queries)
- ✅ Table names sanitized (alphanumeric + _ - only)
- ✅ Schema hardcoded to `public` (no user input)

---

## 🎨 UI Features

### File Type Badges
- **CSV files:** Blue badge with "CSV" text
- **Excel files:** Green badge with "XLSX" or "XLS" text

### Progress Indicators
- **Upload button:** Changes to spinning icon during upload
- **Progress bar:** Animated gradient (blue)
- **Percentage display:** Updates in real-time
- **Current file:** Shows which file is being processed

### Responsive Design
- Works on desktop and tablet
- Mobile: Drag-and-drop may be limited (use click to browse)

### Dark Mode Support
- All UI elements adapt to dark mode
- Maintains readability in both themes

---

## 📝 Best Practices

### 1. File Organization
✅ **Good:**
```
customers_2024.csv
orders_2024.csv
products_2024.csv
```

❌ **Avoid:**
```
data.csv
info.xlsx
file (1).csv
```

### 2. Column Headers
✅ **Good:**
```csv
customer_id,first_name,last_name,email
1,John,Doe,john@example.com
```

❌ **Avoid:**
```csv
ID,First Name,Last Name,E-mail Address
1,John,Doe,john@example.com
```
(Spaces and special characters in column names)

### 3. Data Types
✅ **Good:**
- Dates: `2024-10-27` or `2024-10-27 14:30:00`
- Numbers: `1234.56` (no commas or currency symbols)
- Booleans: `true`/`false` or `1`/`0`

❌ **Avoid:**
- Dates: `Oct 27, 2024` or `27/10/24` (ambiguous)
- Numbers: `$1,234.56` or `1.234,56` (formatting)
- Booleans: `yes`/`no` (better as text)

### 4. Empty Values
- Leave cells empty (blank) for NULL values
- Don't use "N/A", "null", or "-"
- Pandas will interpret blanks as NULL properly

---

## 🔄 Integration with Sync Dashboard

### Relationship to SQL Server Sync

**Different purposes:**
- **SQL Server Sync:** Continuous synchronization from SQL Server to PostgreSQL
- **File Upload:** One-time import of CSV/Excel data to PostgreSQL

**Can be used together:**
1. Use SQL Server Sync for operational databases
2. Use File Upload for:
   - Manual data corrections
   - Importing external data sources
   - Loading reference/lookup tables
   - Historical data backups

### Querying Uploaded Data

After upload, query like any other table:

```sql
-- View uploaded data
SELECT * FROM public.sales_data LIMIT 10;

-- Join with synced SQL Server data
SELECT 
  s.order_id,
  s.order_date,
  c.customer_name
FROM public.sales_data s
JOIN sqlserver_db.customers c ON s.customer_id = c.id;
```

---

## 📊 Monitoring & Maintenance

### View Uploaded Tables

```sql
-- List all tables in public schema
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public'
ORDER BY table_name;

-- Check table row counts
SELECT 
  schemaname,
  tablename,
  n_live_tup as row_count
FROM pg_stat_user_tables
WHERE schemaname = 'public'
ORDER BY row_count DESC;
```

### Clean Up Old Tables

```sql
-- Drop specific table
DROP TABLE IF EXISTS public.old_data;

-- Drop multiple tables
DROP TABLE IF EXISTS 
  public.january_sales,
  public.february_sales;
```

### Table Sizes

```sql
-- Check disk usage
SELECT 
  tablename,
  pg_size_pretty(pg_total_relation_size('public.' || tablename)) as size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size('public.' || tablename) DESC;
```

---

## 🚀 Advanced Usage

### Automating Uploads (API)

Use `curl` or scripting for automated uploads:

```bash
# Upload single file
curl -X POST http://localhost:5001/upload \
  -F "files=@sales_data.csv" \
  -F "pg_database=analytics_db" \
  -b "session_cookie=your_session_cookie"

# Upload multiple files
curl -X POST http://localhost:5001/upload \
  -F "files=@file1.csv" \
  -F "files=@file2.xlsx" \
  -F "files=@file3.csv" \
  -F "pg_database=analytics_db" \
  -b "session_cookie=your_session_cookie"
```

### Python Script for Bulk Upload

```python
import requests
import glob

# Configuration
BASE_URL = 'http://localhost:5001'
SESSION_COOKIE = 'your_session_cookie'
TARGET_DB = 'analytics_db'
FILES_DIR = './data_files/*.csv'

# Gather all files
files_to_upload = glob.glob(FILES_DIR)
files = [('files', open(f, 'rb')) for f in files_to_upload]

# Upload
response = requests.post(
    f'{BASE_URL}/upload',
    files=files,
    data={'pg_database': TARGET_DB},
    cookies={'session': SESSION_COOKIE}
)

print(response.text)

# Close files
for _, f in files:
    f.close()
```

---

## 📞 Support & Troubleshooting

### Get Help

1. **Check this guide first** for common issues
2. **View upload results** on the page for specific error messages
3. **Check PostgreSQL logs** for database-level errors:
   ```bash
   # Linux
   tail -f /var/log/postgresql/postgresql-*.log
   
   # Windows
   # Check: C:\Program Files\PostgreSQL\XX\data\log\
   ```

4. **Browser console** (F12) for JavaScript errors

### Common Error Messages

| Error | Meaning | Solution |
|-------|---------|----------|
| "No PostgreSQL databases found" | No databases configured | Add database in config or create manually |
| "File type not allowed" | Unsupported file extension | Use .csv, .xlsx, or .xls only |
| "Failed to read CSV" | Invalid CSV format | Check file encoding and structure |
| "Failed to read Excel" | Corrupt Excel file | Try opening and re-saving in Excel |
| "Permission denied" | Insufficient role | Login as Admin or Operator |

---

## ✅ Summary

The Multi-File Upload feature provides:
- **Flexibility:** CSV and Excel support
- **Convenience:** Multiple files at once
- **Transparency:** Clear progress and results
- **Safety:** File validation and error handling
- **Integration:** Works alongside SQL Server sync

**Perfect for:**
- Importing external data sources
- Loading reference tables
- Data migration projects
- Manual data corrections
- Historical data backups

---

**Last Updated:** October 27, 2025  
**Version:** 2.0 - Multi-File Upload with Progress Tracking
