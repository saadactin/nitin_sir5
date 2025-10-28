# Add Source Feature - Multi-Source System Support

## 🎯 Overview
The **Add Source** feature enables adding multiple source systems (SQL Server, SAP HANA) and syncing data to different target systems (PostgreSQL, ClickHouse). This expands beyond the original SQL Server-only architecture.

## ✨ Features Added

### 1. **Source Type Selection Page**
- **File**: `templates/add_source.html`
- **Route**: `/add-source`
- **Features**:
  - Visual cards for SQL Server and SAP HANA selection
  - Color-coded badges showing supported targets
  - Mobile-responsive design
  - Interactive hover effects
  - Information section with source details

### 2. **SQL Server Source Configuration**
- **File**: `templates/add_sql_source.html`
- **Route**: `/add-source/sql`
- **Features**:
  - Source naming (friendly name)
  - Server address (supports named instances like `SERVER\SQLEXPRESS`)
  - Username/password authentication
  - Target system selection (PostgreSQL or ClickHouse)
  - Dynamic database loading based on target
  - Connection testing before saving
  - Form validation and error handling
  - Mobile-responsive layout

### 3. **SAP HANA Source Configuration**
- **File**: `templates/add_hana_source.html`
- **Route**: `/add-source/hana`
- **Features**:
  - Source naming
  - Host address, port, and instance number
  - Username/password authentication
  - Target system selection
  - Dynamic database loading
  - Port format guidance (3NN15 pattern)
  - Mobile-responsive layout
  - HANA-specific connection notes

### 4. **Backend Routes (app.py)**

#### Main Routes:
```python
@app.route("/add-source")
def add_source_page()
```
Shows source type selection page.

```python
@app.route("/add-source/sql", methods=["GET", "POST"])
def add_sql_source()
```
Handles SQL Server source configuration and creation.

```python
@app.route("/add-source/hana", methods=["GET", "POST"])
def add_hana_source()
```
Handles SAP HANA source configuration and creation.

```python
@app.route("/api/target/databases")
def get_target_databases()
```
API endpoint to fetch databases from PostgreSQL or ClickHouse based on target type.

### 5. **Database Schema**

New table: `data_sources`

```sql
CREATE TABLE IF NOT EXISTS data_sources (
    id SERIAL PRIMARY KEY,
    source_name VARCHAR(255) UNIQUE NOT NULL,
    source_type VARCHAR(50) NOT NULL,         -- 'sql_server' or 'sap_hana'
    server_address TEXT NOT NULL,
    username TEXT NOT NULL,
    password TEXT NOT NULL,
    target_type VARCHAR(50) NOT NULL,          -- 'postgresql' or 'clickhouse'
    target_database VARCHAR(255) NOT NULL,
    connection_details JSONB,                  -- For HANA-specific fields
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

**Field Descriptions**:
- `source_name`: User-friendly name (e.g., "Production SQL Server")
- `source_type`: Type of source system
- `server_address`: Hostname or IP (for HANA: "host:port")
- `username/password`: Authentication credentials
- `target_type`: Where to sync data to
- `target_database`: Specific database in target system
- `connection_details`: JSON for source-specific config (e.g., HANA instance number)
- `is_active`: Enable/disable source without deletion

## 📁 Files Modified

### Templates:
1. ✅ `templates/sidebar.html` - Added "Add Source" navigation button
2. ✅ `templates/add_source.html` - Source type selection page (NEW)
3. ✅ `templates/add_sql_source.html` - SQL Server configuration (NEW)
4. ✅ `templates/add_hana_source.html` - SAP HANA configuration (NEW)

### Backend:
1. ✅ `app.py` - Added 4 new routes and helper functions

## 🔧 Technical Implementation

### SQL Server Source Creation Flow:
1. User enters source details and credentials
2. System tests SQL Server connection using existing `test_sql_connection()`
3. User selects target (PostgreSQL/ClickHouse)
4. JavaScript fetches available databases via `/api/target/databases`
5. User selects target database
6. Form submits and creates record in `data_sources` table
7. Success message shown and redirect to Add Source page

### SAP HANA Source Creation Flow:
1. User enters HANA connection details (host, port, instance, credentials)
2. Connection testing deferred (requires `hdbcli` library)
3. User selects target and database (same as SQL Server)
4. Record saved with HANA-specific details in `connection_details` JSON field
5. Connection validated during first sync attempt

### Dynamic Database Loading:
```javascript
// JavaScript function in both templates
function loadTargetDatabases() {
  const targetType = document.getElementById('target_type').value;
  fetch(`/api/target/databases?target_type=${targetType}`)
    .then(response => response.json())
    .then(data => {
      // Populate database dropdown
    });
}
```

## 🎨 Design Patterns

### Mobile Responsive:
- **Breakpoints**: 
  - Mobile: `<640px`
  - Tablet: `640px - 1024px`
  - Desktop: `>1024px`
- **Features**:
  - Hamburger menu integration
  - Touch-friendly buttons (44px minimum)
  - Responsive grids (1/2 columns)
  - Stacked forms on mobile
  - Full-width buttons on mobile

### Color Coding:
- **SQL Server**: Blue theme (`text-blue-600`, `bg-blue-100`)
- **SAP HANA**: Green theme (`text-green-600`, `bg-green-100`)
- Consistent with Tailwind color palette

### Error Handling:
- Form validation (required fields)
- Connection testing before save
- Flash messages for success/error
- Form preservation on error (values retained)
- Diagnostic messages for named instances

## 🔒 Security

### Access Control:
```python
@require_role(["admin", "operator"])
```
All routes restricted to admin and operator roles only.

### Password Storage:
⚠️ **Current Implementation**: Passwords stored in plain text in PostgreSQL
⚠️ **Recommendation**: Implement encryption before production

### SQL Injection Prevention:
- Using parameterized queries with `%s` placeholders
- `psycopg2` automatic escaping

## 📊 Integration with Existing System

### Does NOT Interfere With:
- ✅ Existing SQL Server sync logic (uses separate table)
- ✅ Current `sqlservers` YAML configuration
- ✅ Existing sync workers and schedulers
- ✅ Dashboard and analytics pages

### Future Integration Points:
- Sync worker needs update to read from `data_sources` table
- Scheduler needs update to support multiple source types
- Dashboard needs update to display sources from both systems
- HANA sync logic needs implementation (requires `hdbcli`)

## 🚀 Usage Instructions

### For Users:

1. **Access Feature**:
   - Click "Add Source" in sidebar (admin/operator only)

2. **Select Source Type**:
   - Choose "SQL Server" or "SAP HANA"

3. **Configure SQL Server**:
   - Enter source name (e.g., "Production SQL")
   - Enter server address (e.g., `localhost` or `SERVER\INSTANCE`)
   - Enter credentials
   - Test connection (recommended)
   - Select target (PostgreSQL or ClickHouse)
   - Select target database from dropdown
   - Click "Add SQL Server Source"

4. **Configure SAP HANA**:
   - Enter source name (e.g., "SAP Production HANA")
   - Enter host, port (default 30015), instance number
   - Enter credentials
   - Select target and database
   - Click "Add SAP HANA Source"

### For Developers:

#### Add New Source Type:
1. Create template: `templates/add_<type>_source.html`
2. Add route in `app.py`:
   ```python
   @app.route("/add-source/<type>", methods=["GET", "POST"])
   @require_role(["admin", "operator"])
   def add_<type>_source():
       # Implementation
   ```
3. Add card in `templates/add_source.html`
4. Update database schema if needed

#### Query Sources:
```python
import psycopg2
from db_utils import load_pg_config

pg_conf = load_pg_config()
conn = psycopg2.connect(...)
cursor = conn.cursor()

# Get all active SQL Server sources
cursor.execute("""
    SELECT * FROM data_sources 
    WHERE source_type = 'sql_server' 
    AND is_active = TRUE
""")
sources = cursor.fetchall()
```

## 📝 Next Steps (Recommended)

### Phase 1: Source Management
- [ ] Create "View Sources" page listing all configured sources
- [ ] Add "Edit Source" functionality
- [ ] Add "Delete Source" functionality
- [ ] Add "Activate/Deactivate" toggle

### Phase 2: Sync Integration
- [ ] Update sync workers to read from `data_sources` table
- [ ] Create HANA-specific sync logic (requires `hdbcli`)
- [ ] Add source type routing in sync manager
- [ ] Update scheduler to support multi-source

### Phase 3: Monitoring
- [ ] Add source-specific sync history
- [ ] Create source health dashboard
- [ ] Add source connection status monitoring
- [ ] Implement alerting for source failures

### Phase 4: Security Enhancement
- [ ] Implement password encryption (AES-256)
- [ ] Add credential rotation capability
- [ ] Implement connection pooling per source
- [ ] Add audit logging for source changes

## 🐛 Known Limitations

1. **HANA Connection Testing**:
   - Requires `hdbcli` library (not installed by default)
   - Connection validated only during first sync

2. **Password Security**:
   - Currently stored in plain text
   - Should implement encryption before production

3. **No Source Listing**:
   - Can add sources but no UI to view/manage them
   - Recommend building management page

4. **Sync Not Integrated**:
   - Sources saved but not yet used by sync workers
   - Existing sync only reads from YAML config

## ✅ Testing Checklist

### SQL Server Source:
- [ ] Add source with default instance
- [ ] Add source with named instance (`SERVER\INSTANCE`)
- [ ] Test connection before saving
- [ ] Verify PostgreSQL target database loading
- [ ] Verify ClickHouse target database loading
- [ ] Check error handling for invalid credentials
- [ ] Verify mobile responsive layout
- [ ] Test with existing source name (duplicate check)

### SAP HANA Source:
- [ ] Add source with default port (30015)
- [ ] Add source with custom port
- [ ] Add source with instance number
- [ ] Verify target database loading
- [ ] Check mobile responsive layout
- [ ] Test HANA-specific field validation

### Security:
- [ ] Verify only admin/operator can access
- [ ] Test viewer role cannot access
- [ ] Verify unauthenticated redirect

### Database:
- [ ] Verify `data_sources` table creation
- [ ] Check data persistence after restart
- [ ] Verify unique constraint on `source_name`

## 📞 Support

### Common Issues:

**Issue**: "Connection failed" for SQL Server
- ✅ Check server address is correct
- ✅ For named instances, ensure SQL Browser service running
- ✅ Verify firewall allows connection
- ✅ Check credentials are valid

**Issue**: Target databases not loading
- ✅ Check PostgreSQL/ClickHouse services running
- ✅ Verify environment variables set correctly
- ✅ Check browser console for JavaScript errors

**Issue**: "Source already exists"
- ✅ Use unique source name
- ✅ Check `data_sources` table for existing entries

## 📚 Related Documentation
- `MOBILE_RESPONSIVE_GUIDE.md` - UI responsive design
- `PERFORMANCE_AUDIT_REPORT.md` - Performance considerations
- `RBAC_QUICK_REFERENCE.md` - Role-based access control
- `SQL_SERVER_CONNECTIONS.md` - SQL Server connection details

---

**Feature Status**: ✅ **Fully Implemented**  
**Last Updated**: January 2025  
**Author**: GitHub Copilot  
**Version**: 1.0.0
