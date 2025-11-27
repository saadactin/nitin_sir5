"""
Zoho CRM Integration Module
Handles fetching data from Zoho CRM modules and storing in ClickHouse
"""
import requests
import json
import re
from functools import lru_cache
from datetime import datetime, date, time
import logging

logger = logging.getLogger(__name__)


def get_access_token(refresh_token, client_id, client_secret, api_domain="https://www.zohoapis.in"):
    """
    Generate short-lived access token from refresh token.
    
    Args:
        refresh_token: Zoho refresh token
        client_id: Zoho client ID
        client_secret: Zoho client secret
        api_domain: Zoho API domain (default: https://www.zohoapis.in)
    
    Returns:
        dict with access_token, expires_in, api_domain, token_type or None if failed
    """
    # Determine accounts domain from API domain
    accounts_domain_map = {
        "https://www.zohoapis.in": "https://accounts.zoho.in",
        "https://www.zohoapis.com": "https://accounts.zoho.com",
        "https://www.zohoapis.eu": "https://accounts.zoho.eu",
        "https://www.zohoapis.com.au": "https://accounts.zoho.com.au",
        "https://www.zohoapis.jp": "https://accounts.zoho.jp",
    }
    
    accounts_domain = accounts_domain_map.get(api_domain, "https://accounts.zoho.in")
    url = f"{accounts_domain}/oauth/v2/token"
    
    data = {
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "refresh_token"
    }
    
    logger.info(f"Requesting new Zoho access token from {accounts_domain}...")
    try:
        resp = requests.post(url, data=data, timeout=30)
        resp.raise_for_status()
        result = resp.json()
        
        token = result.get("access_token")
        if not token:
            logger.error("Failed to retrieve access token from response")
            return None
        
        # Extract API domain from response if available
        response_api_domain = result.get("api_domain")
        if response_api_domain:
            api_domain = response_api_domain
        
        logger.info("Access token retrieved successfully.")
        return {
            "access_token": token,
            "expires_in": result.get("expires_in", 3600),
            "api_domain": api_domain,
            "token_type": result.get("token_type", "Bearer")
        }
    except Exception as e:
        logger.error(f"Error getting access token: {e}")
        return None


def sanitize_column_name(name: str, used_names: set) -> str:
    """Convert Zoho field names into ClickHouse-safe identifiers."""
    sanitized = re.sub(r"[^0-9a-zA-Z_]", "_", name or "field")
    if sanitized and sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    sanitized = sanitized.lower()
    base = sanitized or "field"
    counter = 1
    candidate = base
    while candidate in used_names:
        candidate = f"{base}_{counter}"
        counter += 1
    used_names.add(candidate)
    return candidate


def normalize_value(value):
    """Prepare values for insertion into ClickHouse."""
    if value is None:
        return None
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


@lru_cache(maxsize=None)
def get_module_field_names(module: str, token: str, api_domain: str):
    """Retrieve all field API names for a module."""
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    url = f"{api_domain}/crm/v2/settings/modules/{module}"
    
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"Failed to fetch field metadata for {module}: {resp.status_code} - {resp.text}")

        payload = resp.json()
        fields = payload.get("modules", [{}])[0].get("fields", [])
        if not fields:
            fields = payload.get("fields", [])
        if not fields:
            raise RuntimeError(f"No fields returned for module {module}")
        
        field_names = {field.get("api_name") for field in fields if field.get("api_name")}
        field_names.add("id")
        return sorted(field_names)
    except Exception as e:
        logger.error(f"Error fetching field names for {module}: {e}")
        raise


def fetch_all_records(module, token, api_domain):
    """Fetch all records from Zoho CRM module (handles pagination)."""
    url = f"{api_domain}/crm/v2/{module}"
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    all_records = []
    page = 1

    try:
        field_names = get_module_field_names(module, token, api_domain)
    except RuntimeError as err:
        logger.warning(f"{err} — continuing without field metadata.")
        field_names = ["id"]

    while True:
        params = {"page": page, "per_page": 200}
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=60)
            if resp.status_code == 204:
                logger.info(f"No records found for {module}")
                break
            if resp.status_code != 200:
                logger.error(f"{module} fetch failed: {resp.status_code} - {resp.text}")
                break

            result = resp.json()
            data = result.get("data", [])
            if not data:
                break

            all_records.extend(data)
            logger.info(f"{module}: Retrieved {len(data)} records (total {len(all_records)})")
            
            if not result.get("info", {}).get("more_records"):
                break
            page += 1
        except Exception as e:
            logger.error(f"Error fetching page {page} for {module}: {e}")
            break

    logger.info(f"Completed fetching {len(all_records)} records for {module}.")
    return all_records


def save_to_clickhouse(client, module, records, database):
    """Insert records into ClickHouse."""
    if not records:
        logger.warning(f"No records found for {module}")
        return 0

    table = f"zoho_{module.lower()}"
    fields = sorted({key for record in records for key in record.keys() if key != "id"})
    used_names = {"id", "load_time"}
    column_map = {field: sanitize_column_name(field, used_names) for field in fields}

    column_defs = ",\n            ".join(
        f"`{column}` Nullable(String)" for column in column_map.values()
    )
    column_section = f",\n            {column_defs}" if column_defs else ""

    # Create table in specified database
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {database}.{table} (
            id String{column_section},
            load_time DateTime DEFAULT now()
        )
        ENGINE = MergeTree()
        ORDER BY load_time
    """)

    # Ensure all columns exist
    try:
        describe = client.query(f"DESCRIBE TABLE {database}.{table}")
        existing_columns = {row[0] for row in describe.result_rows}
    except Exception:
        existing_columns = {"id", "load_time"}

    for column in column_map.values():
        if column not in existing_columns:
            client.command(f"ALTER TABLE {database}.{table} ADD COLUMN `{column}` Nullable(String)")

    column_names = ["id"] + [column_map[field] for field in fields]
    rows = []
    for record in records:
        row = [str(record.get("id", ""))]
        for field in fields:
            row.append(normalize_value(record.get(field)))
        rows.append(row)

    if rows:
        client.insert(f"{database}.{table}", rows, column_names=column_names)
        logger.info(f"{module}: Inserted {len(rows)} records into ClickHouse table {database}.{table}.")
    
    return len(rows)


def get_available_modules(token, api_domain):
    """
    Fetch all available Zoho CRM modules.
    
    Args:
        token: Zoho access token
        api_domain: Zoho API domain
    
    Returns:
        list of module names or empty list if failed
    """
    url = f"{api_domain}/crm/v8/settings/modules"
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code != 200:
            logger.error(f"Failed to fetch modules: {resp.status_code} - {resp.text}")
            return []
        
        result = resp.json()
        modules = result.get("modules", [])
        
        # Extract module API names
        module_names = []
        for module in modules:
            api_name = module.get("api_name")
            if api_name:
                module_names.append({
                    "api_name": api_name,
                    "display_name": module.get("display_label", api_name),
                    "singular_label": module.get("singular_label", api_name),
                    "plural_label": module.get("plural_label", api_name)
                })
        
        return sorted(module_names, key=lambda x: x["display_name"])
    except Exception as e:
        logger.error(f"Error fetching modules: {e}")
        return []


def sync_zoho_modules(refresh_token, client_id, client_secret, api_domain, 
                     clickhouse_host, clickhouse_user, clickhouse_password, 
                     clickhouse_database, selected_modules):
    """
    Sync selected Zoho CRM modules to ClickHouse.
    
    Args:
        refresh_token: Zoho refresh token
        client_id: Zoho client ID
        client_secret: Zoho client secret
        api_domain: Zoho API domain
        clickhouse_host: ClickHouse host
        clickhouse_user: ClickHouse username
        clickhouse_password: ClickHouse password
        clickhouse_database: ClickHouse database name
        selected_modules: List of module API names to sync
    
    Returns:
        dict with success status, synced modules, and any errors
    """
    results = {
        "success": True,
        "synced_modules": [],
        "failed_modules": [],
        "total_records": 0,
        "errors": []
    }
    
    # Get access token
    token_result = get_access_token(refresh_token, client_id, client_secret, api_domain)
    if not token_result:
        results["success"] = False
        results["errors"].append("Failed to obtain access token")
        return results
    
    token = token_result["access_token"]
    api_domain = token_result.get("api_domain", api_domain)
    
    # Import clickhouse_connect only when needed
    try:
        from clickhouse_connect import get_client
    except ImportError:
        results["success"] = False
        results["errors"].append("clickhouse-connect package not installed. Please install it: pip install clickhouse-connect")
        return results
    
    # Connect to ClickHouse
    try:
        client = get_client(
            host=clickhouse_host,
            username=clickhouse_user,
            password=clickhouse_password,
            database=clickhouse_database,
        )
    except Exception as e:
        results["success"] = False
        results["errors"].append(f"Failed to connect to ClickHouse: {str(e)}")
        return results
    
    # Sync each selected module
    for module in selected_modules:
        try:
            logger.info(f"Syncing module: {module}")
            records = fetch_all_records(module, token, api_domain)
            record_count = save_to_clickhouse(client, module, records, clickhouse_database)
            
            results["synced_modules"].append({
                "module": module,
                "record_count": record_count
            })
            results["total_records"] += record_count
        except Exception as e:
            logger.error(f"Error syncing module {module}: {e}")
            results["failed_modules"].append({
                "module": module,
                "error": str(e)
            })
            results["errors"].append(f"{module}: {str(e)}")
    
    return results

