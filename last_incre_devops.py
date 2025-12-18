"""
Azure DevOps to ClickHouse - INCREMENTAL SYNC
=============================================
This script performs incremental sync - only fetches CHANGED work items since last sync.
Requires that last_full_devops.py has been run at least once.

This file is COMPLETELY INDEPENDENT - no external dependencies needed.
"""

# ==================== IMPORTS ====================
import requests
import json
import re
import base64
import sys
import os
from urllib.parse import quote
from datetime import datetime, date, time, timedelta
from collections.abc import MutableMapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from clickhouse_connect import get_client
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ==================== CONFIGURATION - LOAD FROM .env FILE ====================
ACCESS_TOKEN = os.getenv("AZURE_DEVOPS_ACCESS_TOKEN")
ORGANIZATION = os.getenv("AZURE_DEVOPS_ORGANIZATION")
API_BASE_URL = f"https://dev.azure.com/{ORGANIZATION}" if ORGANIZATION else None

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASS = os.getenv("CLICKHOUSE_PASS")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB")

API_VERSION = os.getenv("AZURE_DEVOPS_API_VERSION", "7.1")
PROJECT_NAME = os.getenv("AZURE_DEVOPS_PROJECT_NAME")

# Validate required environment variables
required_vars = {
    "AZURE_DEVOPS_ACCESS_TOKEN": ACCESS_TOKEN,
    "AZURE_DEVOPS_ORGANIZATION": ORGANIZATION,
    "AZURE_DEVOPS_PROJECT_NAME": PROJECT_NAME,
    "CLICKHOUSE_HOST": CLICKHOUSE_HOST,
    "CLICKHOUSE_USER": CLICKHOUSE_USER,
    "CLICKHOUSE_PASS": CLICKHOUSE_PASS,
    "CLICKHOUSE_DB": CLICKHOUSE_DB,
}

missing_vars = [var for var, value in required_vars.items() if not value]
if missing_vars:
    print("❌ ERROR: Missing required environment variables in .env file:")
    for var in missing_vars:
        print(f"   - {var}")
    print("\nPlease create a .env file with all required variables.")
    print("See the documentation for the required .env file format.")
    sys.exit(1)

# Table names
TABLE_MAIN = "DEVOPS_WORKITEMS_MAIN"
TABLE_UPDATES = "DEVOPS_WORKITEMS_UPDATES"
TABLE_COMMENTS = "DEVOPS_WORKITEMS_COMMENTS"
TABLE_RELATIONS = "DEVOPS_WORKITEMS_RELATIONS"
# ======================================================================

def log(message, flush=True):
    """Log with timestamp."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    try:
        print(f"[{timestamp}] {message}", flush=flush)
    except UnicodeEncodeError:
        # Handle Windows console encoding issues
        import sys
        import re
        # Replace emojis with ASCII equivalents or remove them
        emoji_replacements = {
            '🚀': '[START]',
            '📌': '[INFO]',
            '✅': '[OK]',
            '❌': '[ERROR]',
            '⚠️': '[WARN]',
            '📋': '[FETCH]',
            '📦': '[BATCH]',
            '💾': '[SAVE]',
            '📝': '[PROCESS]',
            '🔍': '[CHECK]',
            '🎉': '[SUCCESS]',
            '💡': '[TIP]',
            '🏁': '[DONE]',
            '🗑️': '[DELETE]',
            '📈': '[UP]',
            '📉': '[DOWN]',
            '➡️': '[NO CHANGE]',
            '🔄': '[SYNC]'
        }
        safe_message = message
        for emoji, replacement in emoji_replacements.items():
            safe_message = safe_message.replace(emoji, replacement)
        # Remove any remaining non-ASCII characters that can't be encoded
        safe_message = re.sub(r'[^\x00-\x7F]+', '', safe_message)
        print(f"[{timestamp}] {safe_message}", flush=flush)

def get_auth_headers():
    """Get authentication headers for Azure DevOps API."""
    credentials = base64.b64encode(f":{ACCESS_TOKEN}".encode()).decode()
    return {
        "Authorization": f"Basic {credentials}",
        "Content-Type": "application/json"
    }

def flatten_json(nested_dict, parent_key='', sep='_'):
    """Flatten nested JSON structure for easier analytics."""
    if nested_dict is None:
        return {}
    
    if not isinstance(nested_dict, (dict, list, MutableMapping)):
        return {"value": nested_dict}
    
    items = []
    
    def flatten(obj, parent_key='', sep='_'):
        if obj is None:
            if parent_key:
                items.append((parent_key, None))
            return
        
        if isinstance(obj, dict):
            if not obj:
                if parent_key:
                    items.append((parent_key, None))
                return
            for key, value in obj.items():
                new_key = f"{parent_key}{sep}{key}" if parent_key else key
                if isinstance(value, (dict, list)):
                    flatten(value, new_key, sep=sep)
                else:
                    items.append((new_key, value))
        elif isinstance(obj, list):
            if not obj:
                if parent_key:
                    items.append((parent_key, None))
                return
            for idx, value in enumerate(obj):
                new_key = f"{parent_key}{sep}{idx}" if parent_key else str(idx)
                if isinstance(value, (dict, list)):
                    flatten(value, new_key, sep=sep)
                else:
                    items.append((new_key, value))
        else:
            items.append((parent_key, obj))
    
    # Special handling for work items
    if isinstance(nested_dict, dict) and "fields" in nested_dict and "id" in nested_dict:
        work_item_id = nested_dict.get("id")
        items.append(("id", work_item_id))
        
        fields_dict = nested_dict.get("fields", {})
        if isinstance(fields_dict, str):
            try:
                fields_dict = json.loads(fields_dict)
            except:
                fields_dict = {}
        if not isinstance(fields_dict, dict):
            fields_dict = {}
        
        for key, value in fields_dict.items():
            clean_key = key.replace("System.", "").replace("Microsoft.VSTS.", "").replace("Custom.", "")
            if isinstance(value, (dict, list)):
                flatten(value, clean_key, sep=sep)
            else:
                items.append((clean_key, value))
        
        for key, value in nested_dict.items():
            if key not in ["fields", "id"]:
                new_key = key
                if isinstance(value, (dict, list)):
                    flatten(value, new_key, sep=sep)
                else:
                    items.append((new_key, value))
        
        return dict(items)
    
    flatten(nested_dict, parent_key, sep)
    return dict(items)

def sanitize_column_name(name: str, used_names: set) -> str:
    """Convert field names into ClickHouse-safe identifiers."""
    sanitized = re.sub(r"[^0-9a-zA-Z_]", "_", name or "field")
    if sanitized and sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    sanitized = sanitized.lower()[:255]
    base = sanitized or "field"
    counter = 1
    candidate = base
    while candidate in used_names:
        candidate_str = f"{base}_{counter}"
        if len(candidate_str) > 255:
            base_len = 255 - len(f"_{counter}")
            base = base[:base_len]
            candidate_str = f"{base}_{counter}"
        candidate = candidate_str
        counter += 1
    used_names.add(candidate)
    return candidate

def normalize_value(value):
    """Prepare values for insertion into ClickHouse."""
    if value is None:
        return None
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, (int, float)):
        return str(value)
    return str(value)

def expand_work_item_with_linked_data_parallel(work_item, headers):
    """Fetch data from URLs in _links object using PARALLEL requests."""
    if not isinstance(work_item, dict):
        return work_item
    
    links = work_item.get("_links", {})
    if not isinstance(links, dict):
        return work_item
    
    link_keys_to_expand = ["workItemUpdates", "workItemComments", "workItemRevisions", "workItemType"]
    urls_to_fetch = []
    
    for link_key in link_keys_to_expand:
        if link_key not in links:
            continue
        link_obj = links.get(link_key, {})
        if isinstance(link_obj, dict):
            url = link_obj.get("href") or link_obj.get("url")
        elif isinstance(link_obj, str):
            url = link_obj
        else:
            continue
        if url and url.startswith("http"):
            urls_to_fetch.append((link_key, url))
    
    if not urls_to_fetch:
        return work_item
    
    def fetch_url(link_key_url):
        link_key, url = link_key_url
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code == 200:
                return link_key, resp.json()
        except:
            pass
        return link_key, None
    
    if "_fetched_data" not in work_item:
        work_item["_fetched_data"] = {}
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_url, url_data): url_data for url_data in urls_to_fetch}
        for future in as_completed(futures):
            link_key, linked_data = future.result()
            if linked_data:
                raw_data = linked_data
                if isinstance(raw_data, dict):
                    work_item["_fetched_data"][link_key] = raw_data
                    if "value" in raw_data:
                        linked_data = raw_data.get("value")
                    elif "items" in raw_data:
                        linked_data = raw_data.get("items")
                    elif "results" in raw_data:
                        linked_data = raw_data.get("results")
                elif isinstance(raw_data, list):
                    work_item["_fetched_data"][link_key] = raw_data
    
    return work_item

def extract_core_workitem_fields(work_item):
    """Extract core fields from work item."""
    fields = work_item.get("fields", {})
    
    def get_field_value(*possible_names):
        for name in possible_names:
            if name in fields:
                return fields.get(name)
        return ""
    
    core_fields = {
        "id": str(work_item.get("id", "")),
        "rev": work_item.get("rev", 0),
        "url": work_item.get("url", ""),
        "project": fields.get("System.TeamProject", ""),
        "work_item_type": fields.get("System.WorkItemType", ""),
        "title": fields.get("System.Title", ""),
        "state": fields.get("System.State", ""),
        "reason": fields.get("System.Reason", ""),
        "assigned_to": fields.get("System.AssignedTo", {}).get("displayName", "") if isinstance(fields.get("System.AssignedTo"), dict) else "",
        "assigned_to_email": fields.get("System.AssignedTo", {}).get("uniqueName", "") if isinstance(fields.get("System.AssignedTo"), dict) else "",
        "created_by": fields.get("System.CreatedBy", {}).get("displayName", "") if isinstance(fields.get("System.CreatedBy"), dict) else "",
        "created_date": fields.get("System.CreatedDate", ""),
        "changed_by": fields.get("System.ChangedBy", {}).get("displayName", "") if isinstance(fields.get("System.ChangedBy"), dict) else "",
        "changed_date": fields.get("System.ChangedDate", ""),
        "area_path": fields.get("System.AreaPath", ""),
        "iteration_path": fields.get("System.IterationPath", ""),
        "priority": fields.get("Microsoft.VSTS.Common.Priority", ""),
        "severity": fields.get("Microsoft.VSTS.Common.Severity", ""),
        "story_points": fields.get("Microsoft.VSTS.Scheduling.StoryPoints", ""),
        "effort": fields.get("Microsoft.VSTS.Scheduling.Effort", ""),
        "description": str(fields.get("System.Description", ""))[:1000],
        "tags": fields.get("System.Tags", ""),
        "parent_id": fields.get("System.Parent", 0),
        "board_column": fields.get("System.BoardColumn", ""),
        "board_lane": fields.get("System.BoardLane", ""),
        "closed_date": get_field_value("Microsoft.VSTS.Common.ClosedDate", "System.ClosedDate", "Custom.ClosedDate"),
        "device": get_field_value("Custom.device", "Custom.Device", "device"),
        "scrum_team": get_field_value("Custom.scrumTeam", "Custom.ScrumTeam", "Custom.scrum_team", "scrumTeam"),
        "category": get_field_value("System.Category", "Custom.category", "Custom.Category", "category"),
        "customer": get_field_value("Custom.customer", "Custom.Customer", "customer"),
        "urgent": get_field_value("Custom.urgent", "Custom.Urgent", "urgent"),
        "total_efforts": get_field_value("Custom.totalEfforts", "Custom.TotalEfforts", "Custom.total_efforts", "Microsoft.VSTS.Scheduling.OriginalEstimate", "totalEfforts"),
        "actual_efforts": get_field_value("Custom.ActualEfforts", "Custom.actualEfforts", "Custom.actual_efforts", "Microsoft.VSTS.Scheduling.CompletedWork", "ActualEfforts"),
        "sprint_efforts": get_field_value("Custom.SprintEfforts", "Custom.sprintEfforts", "Custom.sprint_efforts", "SprintEfforts"),
        "remaining_efforts": get_field_value("Custom.RemainingEfforts", "Custom.remainingEfforts", "Custom.remaining_efforts", "Microsoft.VSTS.Scheduling.RemainingWork", "RemainingEfforts"),
    }
    return core_fields

def extract_updates_data(work_item, updates_data):
    """Extract updates/history data."""
    updates = []
    
    if not updates_data and "_fetched_data" in work_item and "workItemUpdates" in work_item["_fetched_data"]:
        fetched = work_item["_fetched_data"]["workItemUpdates"]
        if isinstance(fetched, dict):
            updates_data = fetched.get("value") or fetched.get("items")
        elif isinstance(fetched, list):
            updates_data = fetched
    
    if not updates_data or not isinstance(updates_data, list):
        return updates
    
    work_item_id = str(work_item.get("id", ""))
    
    for update in updates_data:
        if not isinstance(update, dict):
            continue
            
        fields = update.get("fields", {})
        rev = update.get("rev", 0)
        
        state_field = fields.get("System.State", {})
        assigned_field = fields.get("System.AssignedTo", {})
        
        state_new = ""
        state_old = ""
        if isinstance(state_field, dict):
            new_val = state_field.get("newValue", "")
            old_val = state_field.get("oldValue", "")
            state_new = str(new_val) if not isinstance(new_val, (dict, list)) else ""
            state_old = str(old_val) if not isinstance(old_val, (dict, list)) else ""
        
        assigned_new = ""
        assigned_old = ""
        if isinstance(assigned_field, dict):
            new_val = assigned_field.get("newValue", "")
            old_val = assigned_field.get("oldValue", "")
            if isinstance(new_val, dict):
                assigned_new = new_val.get("displayName", "") or new_val.get("uniqueName", "") or ""
            else:
                assigned_new = str(new_val) if not isinstance(new_val, (dict, list)) else ""
            if isinstance(old_val, dict):
                assigned_old = old_val.get("displayName", "") or old_val.get("uniqueName", "") or ""
            else:
                assigned_old = str(old_val) if not isinstance(old_val, (dict, list)) else ""
        
        update_record = {
            "work_item_id": work_item_id,
            "rev": rev,
            "revised_date": update.get("revisedDate", ""),
            "revised_by": update.get("revisedBy", {}).get("displayName", "") if isinstance(update.get("revisedBy"), dict) else "",
            "state_new": state_new[:200],
            "state_old": state_old[:200],
            "assigned_to_new": assigned_new[:200],
            "assigned_to_old": assigned_old[:200],
        }
        
        field_count = 0
        for field_name, field_data in fields.items():
            if field_count >= 5:
                break
            if isinstance(field_data, dict) and "newValue" in field_data:
                new_val = field_data.get("newValue", "")
                old_val = field_data.get("oldValue", "")
                if isinstance(new_val, (dict, list)) or isinstance(old_val, (dict, list)):
                    continue
                clean_field = field_name.replace("System.", "").replace("Microsoft.VSTS.", "").replace("Custom.", "")
                clean_field = re.sub(r"[^0-9a-zA-Z_]", "_", clean_field).lower()[:50]
                update_record[f"field_{clean_field}_new"] = str(new_val)[:500]
                update_record[f"field_{clean_field}_old"] = str(old_val)[:500]
                field_count += 1
        
        updates.append(update_record)
    
    return updates

def extract_comments_data(work_item, headers):
    """Extract comments data."""
    comments = []
    comments_data = None
    
    if "_fetched_data" in work_item and "workItemComments" in work_item["_fetched_data"]:
        fetched = work_item["_fetched_data"]["workItemComments"]
        if isinstance(fetched, dict):
            comments_data = fetched.get("value") or fetched.get("comments") or fetched.get("items")
        elif isinstance(fetched, list):
            comments_data = fetched
    
    if not comments_data:
        links = work_item.get("_links", {})
        comments_link = links.get("workItemComments", {})
        if isinstance(comments_link, dict):
            comments_url = comments_link.get("href")
            if comments_url:
                try:
                    resp = requests.get(comments_url, headers=headers, timeout=30)
                    if resp.status_code == 200:
                        comments_response = resp.json()
                        comments_data = comments_response.get("comments", []) or comments_response.get("value", [])
                except:
                    pass
    
    if not comments_data or not isinstance(comments_data, list):
        return comments
    
    work_item_id = str(work_item.get("id", ""))
    
    for comment in comments_data:
        if not isinstance(comment, dict):
            continue
        
        comment_record = {
            "work_item_id": work_item_id,
            "comment_id": comment.get("id", ""),
            "text": str(comment.get("text", ""))[:2000],
            "created_date": comment.get("createdDate", ""),
            "created_by": comment.get("createdBy", {}).get("displayName", "") if isinstance(comment.get("createdBy"), dict) else "",
            "modified_date": comment.get("modifiedDate", ""),
            "modified_by": comment.get("modifiedBy", {}).get("displayName", "") if isinstance(comment.get("modifiedBy"), dict) else "",
            "is_deleted": 1 if comment.get("isDeleted", False) else 0,
        }
        comments.append(comment_record)
    
    return comments

def extract_relations_data(work_item):
    """Extract relations data."""
    relations = []
    relations_list = work_item.get("relations", [])
    
    if not relations_list or not isinstance(relations_list, list):
        return relations
    
    work_item_id = str(work_item.get("id", ""))
    
    for relation in relations_list:
        if not isinstance(relation, dict):
            continue
        
        relation_record = {
            "work_item_id": work_item_id,
            "relation_type": relation.get("rel", ""),
            "related_work_item_id": relation.get("url", "").split("/")[-1] if relation.get("url") else "",
            "related_work_item_url": relation.get("url", ""),
            "attributes_name": relation.get("attributes", {}).get("name", "") if isinstance(relation.get("attributes"), dict) else "",
        }
        relations.append(relation_record)
    
    return relations

def get_last_sync_time(client, table_name):
    """Get the last sync time from ClickHouse."""
    try:
        result = client.query(f"SELECT max(load_time) FROM {table_name}")
        if result.result_rows and result.result_rows[0][0]:
            return result.result_rows[0][0]
    except:
        pass
    return None

def get_table_row_count(client, table_name):
    """Get current row count from a table."""
    try:
        result = client.query(f"SELECT count() FROM {table_name}")
        if result.result_rows:
            return result.result_rows[0][0]
    except:
        pass
    return 0

def check_full_sync_performed(client):
    """Check if full sync has been performed (tables exist and have data)."""
    try:
        count = get_table_row_count(client, TABLE_MAIN)
        if count > 0:
            return True, count
    except:
        pass
    return False, 0

def save_to_table_incremental(client, table_name, records, upsert_mode=False):
    """Save records with incremental update support."""
    if not records:
        return
    
    flattened_records = [flatten_json(record) for record in records]
    all_fields = set()
    for record in flattened_records:
        all_fields.update(record.keys())
    
    if table_name == TABLE_MAIN:
        id_field = "id"
    elif "work_item_id" in all_fields:
        id_field = "work_item_id"
    elif "comment_id" in all_fields:
        id_field = "comment_id"
    else:
        id_field = "id"
    
    fields = sorted(all_fields - {id_field})
    used_names = {id_field, "load_time"}
    column_map = {field: sanitize_column_name(field, used_names) for field in fields}
    id_column_name = sanitize_column_name(id_field, {"load_time"})
    
    try:
        describe = client.query(f"DESCRIBE TABLE {table_name}")
        existing_columns = {row[0] for row in describe.result_rows}
    except:
        existing_columns = {id_column_name, "load_time"}
        try:
            client.command(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    `{id_column_name}` Nullable(String),
                    load_time DateTime DEFAULT now()
                )
                ENGINE = MergeTree()
                ORDER BY load_time
            """)
        except:
            pass
    
    missing_columns = [col for col in column_map.values() if col not in existing_columns]
    if missing_columns:
        try:
            alter_statements = [f"ADD COLUMN `{col}` Nullable(String)" for col in missing_columns]
            client.command(f"ALTER TABLE {table_name} {', '.join(alter_statements)}")
            existing_columns.update(missing_columns)
        except:
            for col in missing_columns:
                try:
                    client.command(f"ALTER TABLE {table_name} ADD COLUMN `{col}` Nullable(String)")
                    existing_columns.add(col)
                except:
                    pass
    
    # For MAIN table with UPSERT: delete old records first
    if upsert_mode and table_name == TABLE_MAIN:
        work_item_ids = list(set([str(r.get(id_field, "")) for r in flattened_records if r.get(id_field)]))
        if work_item_ids:
            batch_size = 1000
            deleted_count = 0
            for i in range(0, len(work_item_ids), batch_size):
                batch_ids = work_item_ids[i:i + batch_size]
                ids_str = "', '".join(batch_ids)
                try:
                    client.command(f"ALTER TABLE {table_name} DELETE WHERE `{id_column_name}` IN ('{ids_str}')")
                    deleted_count += len(batch_ids)
                except Exception as e:
                    log(f"      ⚠️  Error deleting batch: {e}")
            if deleted_count > 0:
                log(f"      🗑️  Deleted {deleted_count} old record(s) from {table_name}")
    
    column_names = [id_column_name] + [column_map[field] for field in fields if column_map[field] in existing_columns]
    rows = []
    for record in flattened_records:
        record_id = record.get(id_field) or record.get("id") or ""
        row = [normalize_value(record_id)]
        for field in fields:
            if column_map[field] in existing_columns:
                row.append(normalize_value(record.get(field)))
        rows.append(row)
    
    if rows and column_names:
        try:
            column_names_with_time = ["load_time"] + column_names
            rows_with_time = [[datetime.now()] + row for row in rows]
            client.insert(table_name, rows_with_time, column_names=column_names_with_time)
        except Exception as e:
            try:
                client.insert(table_name, rows, column_names=column_names)
            except Exception as e2:
                log(f"      ⚠️  Error inserting into {table_name}: {e2}")

def process_work_item(item_data):
    """Process a single work item."""
    idx, work_item, headers = item_data
    try:
        work_item_id = work_item.get("id", "")
        expanded_item = expand_work_item_with_linked_data_parallel(work_item, headers)
        main_record = extract_core_workitem_fields(expanded_item)
        
        updates_url = expanded_item.get("_links", {}).get("workItemUpdates", {}).get("href", "")
        updates_data = None
        if updates_url:
            try:
                updates_resp = requests.get(updates_url, headers=headers, timeout=30)
                if updates_resp.status_code == 200:
                    updates_data = updates_resp.json().get("value", [])
            except:
                pass
        
        updates = extract_updates_data(expanded_item, updates_data)
        comments = extract_comments_data(expanded_item, headers)
        relations = extract_relations_data(expanded_item)
        
        return {
            "main": main_record,
            "updates": updates,
            "comments": comments,
            "relations": relations,
            "work_item_id": str(work_item_id),
            "idx": idx
        }
    except Exception as e:
        return {
            "error": str(e)[:100],
            "work_item_id": str(work_item.get("id", "unknown")),
            "idx": idx
        }

# ==================== MAIN EXECUTION ====================

if __name__ == "__main__":
    log("🔄 Starting INCREMENTAL SYNC - Keeping ClickHouse updated")
    log("=" * 70)
    log("📌 This will only fetch CHANGED work items since last sync")
    log("=" * 70)
    
    # Connect to ClickHouse
    client = get_client(
        host=CLICKHOUSE_HOST,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASS,
        database=CLICKHOUSE_DB,
    )
    log("✅ Connected to ClickHouse")
    
    # Check if full sync has been performed
    full_sync_done, main_count = check_full_sync_performed(client)
    if not full_sync_done:
        log("\n❌ ERROR: Full sync has not been performed yet!")
        log("   Please run last_full_devops.py FIRST to perform initial full sync.")
        log("   This script requires existing data in ClickHouse to work.")
        sys.exit(1)
    
    log(f"✅ Full sync detected: {main_count:,} work items in database")
    
    # Get auth headers
    headers = get_auth_headers()
    log("✅ Authentication ready")
    
    # Get last sync time
    last_sync = get_last_sync_time(client, TABLE_MAIN)
    project_name_encoded = quote(PROJECT_NAME, safe='')
    
    if last_sync:
        # Subtract 1 day buffer (use date-only format for WIQL)
        sync_date = (last_sync - timedelta(days=1)).strftime("%Y-%m-%d")
        log(f"📅 Last sync: {last_sync}")
        log(f"🔄 Fetching work items changed since: {sync_date}")
        
        wiql_query = {
            "query": f"""
                SELECT [System.Id] 
                FROM WorkItems 
                WHERE [System.TeamProject] = '{PROJECT_NAME}' 
                AND [System.ChangedDate] >= '{sync_date}'
                ORDER BY [System.Id]
            """
        }
    else:
        log("⚠️  No last sync time found - fetching all work items")
        sync_date = None
        wiql_query = {
            "query": f"""
                SELECT [System.Id] 
                FROM WorkItems 
                WHERE [System.TeamProject] = '{PROJECT_NAME}' 
                ORDER BY [System.Id]
            """
        }
    
    # Execute WIQL query
    wiql_url = f"https://dev.azure.com/{ORGANIZATION}/{project_name_encoded}/_apis/wit/wiql?api-version={API_VERSION}"
    resp = requests.post(wiql_url, json=wiql_query, headers=headers, timeout=30)
    
    if resp.status_code != 200:
        log(f"❌ Failed WIQL query: {resp.status_code}")
        log(f"   Response: {resp.text[:200]}")
        sys.exit(1)
    
    wiql_result = resp.json()
    work_item_refs = wiql_result.get("workItems", [])
    
    if not work_item_refs:
        log("✅ No changes detected - database is up to date!")
        log("🏁 Done!")
        sys.exit(0)
    
    total_work_items = len(work_item_refs)
    log(f"✅ Found {total_work_items:,} changed work items to process")
    log(f"📊 Will process in batches of 50 items")
    log(f"⚡ Using parallel processing (10 concurrent workers)")
    
    # Get initial row counts
    log(f"\n{'='*70}")
    log(f"📊 Getting initial row counts (BEFORE sync)...")
    log(f"{'='*70}")
    
    initial_counts = {}
    tables_to_check = [
        (TABLE_MAIN, "Main Work Items"),
        (TABLE_UPDATES, "Updates"),
        (TABLE_COMMENTS, "Comments"),
        (TABLE_RELATIONS, "Relations"),
    ]
    
    for table_name, description in tables_to_check:
        try:
            count = get_table_row_count(client, table_name)
            initial_counts[table_name] = count
            log(f"   📋 {table_name}: {count:,} row(s)")
        except Exception as e:
            initial_counts[table_name] = 0
            log(f"   ⚠️  {table_name}: Could not get count - {e}")
    
    # Track totals
    total_main_upserted = 0
    total_updates_added = 0
    total_comments_added = 0
    total_relations_added = 0
    
    # Process in batches
    batch_size = 50
    total_batches = (total_work_items + batch_size - 1) // batch_size
    
    for batch_start in range(0, total_work_items, batch_size):
        batch_end = min(batch_start + batch_size, total_work_items)
        batch_refs = work_item_refs[batch_start:batch_end]
        batch_num = batch_start // batch_size + 1
        
        log(f"\n{'='*70}")
        log(f"📦 Processing batch {batch_num}/{total_batches}")
        log(f"   Items: {batch_start+1}-{batch_end} of {total_work_items:,}")
        log(f"   Progress: {batch_start/total_work_items*100:.1f}% complete")
        log(f"{'='*70}")
        
        batch_ids = [str(ref["id"]) for ref in batch_refs]
        ids_str = ",".join(batch_ids)
        
        workitems_url = f"https://dev.azure.com/{ORGANIZATION}/{project_name_encoded}/_apis/wit/workitems?ids={ids_str}&$expand=all&api-version={API_VERSION}"
        
        try:
            resp = requests.get(workitems_url, headers=headers, timeout=60)
            if resp.status_code != 200:
                log(f"   ⚠️  Failed to fetch batch: {resp.status_code}")
                continue
            
            batch_result = resp.json()
            batch_items = batch_result.get("value", [])
            
            log(f"   ✅ Fetched {len(batch_items)} work items")
            log(f"   ⚡ Processing {len(batch_items)} work items in parallel (10 concurrent)...")
            
            batch_results = [None] * len(batch_items)
            completed_count = [0]
            
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(process_work_item, (idx, item, headers)): idx 
                          for idx, item in enumerate(batch_items)}
                for future in as_completed(futures):
                    result = future.result()
                    if result and "error" not in result:
                        batch_results[result["idx"]] = result
                        completed_count[0] += 1
                        if completed_count[0] % 10 == 0 or completed_count[0] == len(batch_items):
                            processed = batch_start + completed_count[0]
                            log(f"      ✓ Processed {processed}/{total_work_items:,} work items ({processed/total_work_items*100:.1f}%)")
                            sys.stdout.flush()
                    elif result and "error" in result:
                        log(f"      ⚠️  Error processing work item {result.get('work_item_id', 'unknown')}: {result.get('error', 'Unknown error')}")
            
            batch_main = []
            batch_updates = []
            batch_comments = []
            batch_relations = []
            
            for result in batch_results:
                if result and "error" not in result:
                    batch_main.append(result["main"])
                    batch_updates.extend(result["updates"])
                    batch_comments.extend(result["comments"])
                    batch_relations.extend(result["relations"])
            
            log(f"\n   💾 Saving batch {batch_num} to ClickHouse...")
            sys.stdout.flush()
            
            try:
                if batch_main:
                    log(f"      📝 Upserting {len(batch_main)} main records...")
                    sys.stdout.flush()
                    save_to_table_incremental(client, TABLE_MAIN, batch_main, upsert_mode=True)
                    total_main_upserted += len(batch_main)
                    log(f"      ✅ Updated {len(batch_main)} main records in {TABLE_MAIN}")
                    sys.stdout.flush()
                
                if batch_updates:
                    log(f"      📝 Appending {len(batch_updates)} update records...")
                    sys.stdout.flush()
                    save_to_table_incremental(client, TABLE_UPDATES, batch_updates, upsert_mode=False)
                    total_updates_added += len(batch_updates)
                    log(f"      ✅ Appended {len(batch_updates)} update records to {TABLE_UPDATES}")
                    sys.stdout.flush()
                
                if batch_comments:
                    log(f"      📝 Appending {len(batch_comments)} comment records...")
                    sys.stdout.flush()
                    save_to_table_incremental(client, TABLE_COMMENTS, batch_comments, upsert_mode=False)
                    total_comments_added += len(batch_comments)
                    log(f"      ✅ Appended {len(batch_comments)} comment records to {TABLE_COMMENTS}")
                    sys.stdout.flush()
                
                if batch_relations:
                    log(f"      📝 Appending {len(batch_relations)} relation records...")
                    sys.stdout.flush()
                    save_to_table_incremental(client, TABLE_RELATIONS, batch_relations, upsert_mode=False)
                    total_relations_added += len(batch_relations)
                    log(f"      ✅ Appended {len(batch_relations)} relation records to {TABLE_RELATIONS}")
                    sys.stdout.flush()
                
                log(f"   ✅ Batch {batch_num}/{total_batches} COMPLETE!")
                sys.stdout.flush()
                
            except Exception as e:
                log(f"      ⚠️  Error saving batch: {e}")
                import traceback
                traceback.print_exc()
                sys.stdout.flush()
        
        except Exception as e:
            log(f"   ⚠️  Error fetching batch {batch_num}: {e}")
            sys.stdout.flush()
            continue
    
    # Final verification
    log(f"\n{'='*70}")
    log(f"🔍 Final Verification & Row Count Changes")
    log(f"{'='*70}")
    
    final_counts = {}
    row_changes = {}
    
    for table_name, description in tables_to_check:
        try:
            final_count = get_table_row_count(client, table_name)
            initial_count = initial_counts.get(table_name, 0)
            change = final_count - initial_count
            final_counts[table_name] = final_count
            row_changes[table_name] = change
            
            change_symbol = "📈" if change > 0 else "📉" if change < 0 else "➡️"
            change_text = f"+{change:,}" if change > 0 else f"{change:,}" if change < 0 else "0"
            
            log(f"   {change_symbol} {table_name}:")
            log(f"      Before: {initial_count:,} rows")
            log(f"      After:  {final_count:,} rows")
            log(f"      Change: {change_text} rows")
        except Exception as e:
            log(f"   ❌ {table_name}: Error - {e}")
    
    log(f"\n{'='*70}")
    log(f"📊 SYNC SUMMARY")
    log(f"{'='*70}")
    log(f"   ✅ Processed: {total_work_items:,} changed work items")
    log(f"   📝 Main records upserted: {total_main_upserted:,}")
    log(f"   📝 Update records added: {total_updates_added:,}")
    log(f"   📝 Comment records added: {total_comments_added:,}")
    log(f"   📝 Relation records added: {total_relations_added:,}")
    log(f"   📅 Next sync will fetch items changed after: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"{'='*70}")
    
    log(f"\n📋 TABLE SUMMARY:")
    for table_name, description in tables_to_check:
        initial = initial_counts.get(table_name, 0)
        final = final_counts.get(table_name, 0)
        change = row_changes.get(table_name, 0)
        change_pct = (change / initial * 100) if initial > 0 else 0
        
        if change > 0:
            log(f"   ✅ {table_name}: {initial:,} → {final:,} (+{change:,} rows, +{change_pct:.2f}%)")
        elif change < 0:
            log(f"   ⚠️  {table_name}: {initial:,} → {final:,} ({change:,} rows, {change_pct:.2f}%)")
        else:
            log(f"   ➡️  {table_name}: {initial:,} → {final:,} (no change)")
    
    log(f"\n🎉 Incremental sync complete!")
    log(f"💡 TIP: Run this script periodically (e.g., every hour) to keep ClickHouse updated!")
    log(f"🏁 Done!")

