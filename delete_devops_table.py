"""
Delete DevOps Tables from ClickHouse
====================================
This script deletes the 4 DevOps tables created in ClickHouse:
- DEVOPS_WORKITEMS_MAIN
- DEVOPS_WORKITEMS_UPDATES
- DEVOPS_WORKITEMS_COMMENTS
- DEVOPS_WORKITEMS_RELATIONS

The database name is read from .env file (CLICKHOUSE_DB).
"""

import os
import sys
from dotenv import load_dotenv
from clickhouse_connect import get_client

# Load environment variables
load_dotenv()

# Get ClickHouse configuration from .env
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASS = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DATABASE")

# Validate required environment variables
if not all([CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASS, CLICKHOUSE_DB]):
    print("❌ ERROR: Missing required environment variables in .env file:")
    missing = []
    if not CLICKHOUSE_HOST:
        missing.append("CLICKHOUSE_HOST")
    if not CLICKHOUSE_USER:
        missing.append("CLICKHOUSE_USER")
    if not CLICKHOUSE_PASS:
        missing.append("CLICKHOUSE_PASS")
    if not CLICKHOUSE_DB:
        missing.append("CLICKHOUSE_DB")
    for var in missing:
        print(f"   - {var}")
    print("\nPlease ensure all required variables are set in your .env file.")
    sys.exit(1)

# Table names to delete
TABLES_TO_DELETE = [
    "DEVOPS_WORKITEMS_MAIN",
    "DEVOPS_WORKITEMS_UPDATES",
    "DEVOPS_WORKITEMS_COMMENTS",
    "DEVOPS_WORKITEMS_RELATIONS"
]

def log(message):
    """Log with timestamp."""
    from datetime import datetime
    timestamp = datetime.now().strftime("%H:%M:%S")
    try:
        print(f"[{timestamp}] {message}")
    except UnicodeEncodeError:
        # Handle Windows console encoding issues
        import re
        emoji_replacements = {
            '✅': '[OK]',
            '❌': '[ERROR]',
            '⚠️': '[WARN]',
            '🗑️': '[DELETE]'
        }
        safe_message = message
        for emoji, replacement in emoji_replacements.items():
            safe_message = safe_message.replace(emoji, replacement)
        safe_message = re.sub(r'[^\x00-\x7F]+', '', safe_message)
        print(f"[{timestamp}] {safe_message}")

if __name__ == "__main__":
    log("=" * 70)
    log("🗑️  Starting DevOps Tables Deletion")
    log("=" * 70)
    log(f"Database: {CLICKHOUSE_DB}")
    log(f"Host: {CLICKHOUSE_HOST}")
    log(f"Tables to delete: {', '.join(TABLES_TO_DELETE)}")
    log("=" * 70)
    
    try:
        # Connect to ClickHouse
        client = get_client(
            host=CLICKHOUSE_HOST,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASS,
            database=CLICKHOUSE_DB,
        )
        log("✅ Connected to ClickHouse")
        
        deleted_count = 0
        not_found_count = 0
        error_count = 0
        
        # Delete each table
        for table_name in TABLES_TO_DELETE:
            try:
                # Check if table exists
                result = client.query(f"EXISTS TABLE {table_name}")
                table_exists = result.result_rows[0][0] == 1 if result.result_rows else False
                
                if table_exists:
                    # Get row count before deletion
                    try:
                        count_result = client.query(f"SELECT count() FROM {table_name}")
                        row_count = count_result.result_rows[0][0] if count_result.result_rows else 0
                        log(f"   📊 {table_name}: {row_count:,} row(s) found")
                    except:
                        pass
                    
                    # Delete the table
                    client.command(f"DROP TABLE IF EXISTS {table_name}")
                    log(f"   ✅ Deleted {table_name}")
                    deleted_count += 1
                else:
                    log(f"   ⚠️  {table_name}: Table does not exist (skipping)")
                    not_found_count += 1
                    
            except Exception as e:
                log(f"   ❌ Error deleting {table_name}: {e}")
                error_count += 1
        
        # Summary
        log("")
        log("=" * 70)
        log("📊 Deletion Summary")
        log("=" * 70)
        log(f"   ✅ Successfully deleted: {deleted_count} table(s)")
        if not_found_count > 0:
            log(f"   ⚠️  Not found (skipped): {not_found_count} table(s)")
        if error_count > 0:
            log(f"   ❌ Errors: {error_count} table(s)")
        log("=" * 70)
        
        if deleted_count > 0:
            log("✅ DevOps tables deletion completed!")
        else:
            log("⚠️  No tables were deleted (they may not exist)")
        
        log("🏁 Done!")
        
    except Exception as e:
        log(f"❌ ERROR: Failed to connect or delete tables: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

