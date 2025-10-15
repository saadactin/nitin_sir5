"""
Utility script to fix SQLAlchemy execution issues and SQL2019_Second named instance connections
"""

import re
import os
import sys

def fix_sqlalchemy_file(file_path):
    """
    Find and fix all instances of connection.execute("...") to use text()
    """
    print(f"Processing {file_path}...")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Check if we need to import text
    needs_import = 'connection.execute("' in content and 'from sqlalchemy import text' not in content
    
    # Fix direct string execution
    pattern = r'connection\.execute\("(.*?)"\)'
    replacement = r'connection.execute(text("\1"))'
    
    new_content = re.sub(pattern, replacement, content)
    
    # Add import if needed
    if needs_import and new_content != content:
        import_line = 'from sqlalchemy import text\n'
        # Find a good place to add the import
        import_pattern = r'import\s+.*?\n'
        match = re.search(import_pattern, content)
        if match:
            position = match.end()
            new_content = new_content[:position] + import_line + new_content[position:]
        else:
            # Add at the top if no imports found
            new_content = import_line + new_content
    
    if new_content != content:
        print(f"  - Fixed SQLAlchemy text() issues in {file_path}")
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
    else:
        print(f"  - No changes needed for {file_path}")

def fix_sql2019_second_connection(file_path):
    """
    Enhance the SQL2019_Second connection code
    """
    print(f"Enhancing SQL2019_Second connection handling in {file_path}...")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Check if the special handling for SQL2019_Second already exists
    if "Special handling for SQL2019_Second instance which needs direct port specification" in content:
        print("  - SQL2019_Second special handling already exists")
        return
    
    # Add specialized handling for SQL2019_Second
    if "def get_sql_connection(conf, database=None):" in content:
        old_function = """def get_sql_connection(conf, database=None):
    \"\"\"
    Get a pyodbc connection to SQL Server.
    Handles named instances and escapes backslashes.
    Supports both SQL Server authentication and Windows authentication.
    Port is automatically detected from the instance name.
    \"\"\"
    server = conf['server']"""
        
        new_function = """def get_sql_connection(conf, database=None):
    \"\"\"
    Get a pyodbc connection to SQL Server.
    Handles named instances and escapes backslashes.
    Supports both SQL Server authentication and Windows authentication.
    Port is automatically detected from the instance name.
    \"\"\"
    server = conf['server']
    original_server = server
    
    # Special handling for SQL2019_Second instance which needs direct port specification
    if '\\\\' in server and 'SQL2019_SECOND' in server.upper():
        server_name = server.split('\\\\')[0]
        server = f"{server_name},14344"
        logging.info(f"Using direct port connection for SQL2019_Second: {server}")"""
        
        new_content = content.replace(old_function, new_function)
        
        if new_content != content:
            print(f"  - Enhanced SQL2019_Second connection handling in {file_path}")
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
        else:
            print(f"  - Could not find the exact function pattern to replace in {file_path}")
    else:
        print(f"  - get_sql_connection function not found in {file_path}")

if __name__ == "__main__":
    print("=== SQL Connection Fix Script ===")
    
    # Fix SQLAlchemy text() usage
    files_to_check = [
        'app.py', 
        'hybrid_sync.py', 
        'db_utils.py', 
        'metrics.py', 
        'monitoring.py',
        'analytics.py',
        'analytics_advanced.py'
    ]
    
    for file in files_to_check:
        file_path = os.path.join(os.path.dirname(__file__), file)
        if os.path.exists(file_path):
            fix_sqlalchemy_file(file_path)
    
    # Fix SQL2019_Second connection handling
    fix_sql2019_second_connection(os.path.join(os.path.dirname(__file__), 'hybrid_sync.py'))
    
    print("\nAll fixes applied. Please restart your application to apply the changes.")