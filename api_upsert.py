"""
API Upserting Module - Handles INSERT + UPDATE for APIs with changing data
For APIs that return same IDs but with updated values (like real-time price data)
"""
import requests
import time
import logging
from datetime import datetime
from clickhouse_driver import Client
from db_utils import load_clickhouse_config
from api_data_detector import auto_detect_and_extract
from api_sync import flatten_record, infer_clickhouse_type, create_clickhouse_table_from_sample

logger = logging.getLogger(__name__)


def ensure_columns_exist(client, target_database, target_table, new_columns_dict):
    """
    Ensure all columns exist in the table. Add missing columns if needed.
    
    Args:
        client: ClickHouse client
        target_database: Database name
        target_table: Table name
        new_columns_dict: Dict of column_name -> sample_value to infer type
    """
    try:
        # Get existing columns from ClickHouse
        existing_columns_query = f"DESCRIBE TABLE {target_database}.{target_table}"
        existing_columns_result = client.execute(existing_columns_query)
        existing_column_names = {row[0] for row in existing_columns_result}
        
        # Find missing columns
        missing_columns = {}
        for col_name, sample_value in new_columns_dict.items():
            if col_name not in existing_column_names:
                # Infer type for the new column
                col_type = infer_clickhouse_type(sample_value)
                missing_columns[col_name] = col_type
        
        # Add missing columns
        if missing_columns:
            logger.info(f"Adding {len(missing_columns)} new column(s) to {target_database}.{target_table}")
            for col_name, col_type in missing_columns.items():
                try:
                    alter_query = f"ALTER TABLE {target_database}.{target_table} ADD COLUMN IF NOT EXISTS `{col_name}` {col_type}"
                    client.execute(alter_query)
                    logger.info(f"  Added column: `{col_name}` {col_type}")
                except Exception as e:
                    logger.error(f"  ERROR: Failed to add column `{col_name}`: {e}")
        
        return len(missing_columns) > 0
        
    except Exception as e:
        # Table might not exist yet, which is fine
        logger.debug(f"Could not check columns (table might not exist): {e}")
        return False


def upsert_api_to_clickhouse(api_url, target_database, target_table,
                              auth_type="none", auth_token="",
                              basic_username="", basic_password="",
                              apikey_header="X-API-Key",
                              custom_headers=None,
                              request_method="GET",
                              data_path="data",
                              poll_interval=5,
                              id_column="id",
                              auto_create_table=True,
                              oauth_token_url="",
                              oauth_username="",
                              oauth_password="",
                              oauth_refresh_interval=3600):
    """
    Continuously poll API and UPSERT data to ClickHouse
    For APIs that return same IDs with updated values (e.g., cryptocurrency prices)
    
    Strategy: Use ReplacingMergeTree which automatically handles updates
    """
    
    logger.info(f"Starting UPSERT mode API polling from: {api_url}")
    logger.info(f"Target: {target_database}.{target_table}")
    logger.info(f"Polling interval: {poll_interval} seconds")
    logger.info(f"ID column: {id_column}")
    logger.info(f"UPSERT mode: Will INSERT new records and UPDATE existing ones!")
    
    # Initialize OAuth if needed
    token_manager = None
    if auth_type == "oauth" and oauth_token_url and oauth_username and oauth_password:
        from oauth_token_manager import get_token_manager
        logger.info(f"OAuth enabled - Token URL: {oauth_token_url}")
        token_manager = get_token_manager(
            oauth_token_url, oauth_username, oauth_password, oauth_refresh_interval
        )
    
    try:
        # Connect to ClickHouse with error handling
        try:
            ch_conf = load_clickhouse_config()
            client = Client(
                host=ch_conf['host'],
                port=ch_conf['port'],
                user=ch_conf['user'],
                password=ch_conf['password']
            )
            # Test connection
            client.execute("SELECT 1")
            logger.info("ClickHouse connection established successfully")
        except Exception as ch_conn_err:
            logger.error(f"ERROR: Cannot connect to ClickHouse database")
            logger.error(f"Host: {ch_conf.get('host', 'N/A')}")
            logger.error(f"Port: {ch_conf.get('port', 'N/A')}")
            logger.error(f"User: {ch_conf.get('user', 'N/A')}")
            logger.error(f"Error: {str(ch_conn_err)}")
            logger.error("Please check:")
            logger.error("  1. ClickHouse server is running")
            logger.error("  2. Connection settings in config are correct")
            logger.error("  3. Network connectivity to ClickHouse server")
            raise
        
        # Prepare headers
        headers = custom_headers.copy() if custom_headers else {}
        if auth_type == "bearer" and auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"
        elif auth_type == "apikey" and auth_token and apikey_header:
            headers[apikey_header] = auth_token
        
        # Prepare basic auth
        auth = None
        if auth_type == "basic" and basic_username and basic_password:
            auth = (basic_username, basic_password)
        
        table_created = False
        poll_count = 0
        
        while True:
            try:
                poll_count += 1
                logger.info(f"Poll #{poll_count}: Fetching data...")
                
                # Get OAuth token if needed with error handling
                if auth_type == "oauth" and token_manager:
                    try:
                        current_token = token_manager.get_token()
                        if current_token:
                            headers["Authorization"] = f"Bearer {current_token}"
                        else:
                            logger.error("ERROR: Failed to get OAuth token - Authentication failed. Check OAuth credentials.")
                            logger.info(f"Retrying in {poll_interval}s...")
                            time.sleep(poll_interval)
                            continue
                    except Exception as oauth_error:
                        logger.error(f"ERROR: OAuth token retrieval failed: {str(oauth_error)}")
                        logger.error("Please verify OAuth Token URL, Username, and Password in source configuration.")
                        logger.info(f"Retrying in {poll_interval}s...")
                        time.sleep(poll_interval)
                        continue
                
                # Fetch API data with comprehensive error handling
                try:
                    if request_method == "GET":
                        response = requests.get(api_url, headers=headers, auth=auth, timeout=30)
                    else:
                        response = requests.post(api_url, headers=headers, auth=auth, timeout=30)
                except requests.exceptions.ConnectionError as conn_err:
                    logger.error(f"ERROR: Cannot connect to API at {api_url}")
                    logger.error(f"Details: {str(conn_err)}")
                    logger.error("Possible causes: API server is down, wrong URL, or network issue.")
                    logger.info(f"Retrying in {poll_interval}s...")
                    time.sleep(poll_interval)
                    continue
                except requests.exceptions.Timeout as timeout_err:
                    logger.error(f"ERROR: API request timed out after 30 seconds")
                    logger.error(f"API URL: {api_url}")
                    logger.error("The API is taking too long to respond. It may be overloaded or slow.")
                    logger.info(f"Retrying in {poll_interval}s...")
                    time.sleep(poll_interval)
                    continue
                except requests.exceptions.RequestException as req_err:
                    logger.error(f"ERROR: API request failed: {str(req_err)}")
                    logger.error(f"API URL: {api_url}")
                    logger.info(f"Retrying in {poll_interval}s...")
                    time.sleep(poll_interval)
                    continue
                
                # Handle HTTP status codes with specific error messages
                if response.status_code == 401:
                    logger.error(f"ERROR: Authentication failed (401 Unauthorized)")
                    if auth_type == "oauth":
                        logger.error("OAuth credentials are invalid or expired. Please check:")
                        logger.error("  - OAuth Token URL")
                        logger.error("  - OAuth Username")
                        logger.error("  - OAuth Password")
                    elif auth_type == "bearer":
                        logger.error("Bearer token is invalid or expired. Please check the token in source configuration.")
                    elif auth_type == "apikey":
                        logger.error("API Key is invalid. Please check the API key in source configuration.")
                    elif auth_type == "basic":
                        logger.error("Basic authentication failed. Please check username and password.")
                    else:
                        logger.error("Authentication required but not configured. Please set up authentication.")
                    logger.info(f"Retrying in {poll_interval}s...")
                    time.sleep(poll_interval)
                    continue
                elif response.status_code == 403:
                    logger.error(f"ERROR: Access forbidden (403). API returned: {response.text[:200]}")
                    logger.error("You don't have permission to access this resource.")
                    logger.error("Check if your API credentials have the required permissions.")
                    logger.info(f"Retrying in {poll_interval}s...")
                    time.sleep(poll_interval)
                    continue
                elif response.status_code == 404:
                    logger.error(f"ERROR: API endpoint not found (404)")
                    logger.error(f"API URL: {api_url}")
                    logger.error("The endpoint does not exist. Please verify the API URL is correct.")
                    logger.info(f"Retrying in {poll_interval}s...")
                    time.sleep(poll_interval)
                    continue
                elif response.status_code >= 500:
                    logger.error(f"ERROR: API server error ({response.status_code})")
                    logger.error(f"API Response: {response.text[:200]}")
                    logger.error("The API server is experiencing an error. This is likely a temporary issue.")
                    logger.info(f"Retrying in {poll_interval}s...")
                    time.sleep(poll_interval)
                    continue
                elif response.status_code != 200:
                    logger.error(f"ERROR: API returned unexpected status code: {response.status_code}")
                    logger.error(f"API Response: {response.text[:200]}")
                    logger.info(f"Retrying in {poll_interval}s...")
                    time.sleep(poll_interval)
                    continue
                
                # Parse JSON response with error handling
                try:
                    json_data = response.json()
                except ValueError as json_err:
                    logger.error(f"ERROR: Failed to parse JSON response from API")
                    logger.error(f"Response text (first 500 chars): {response.text[:500]}")
                    logger.error(f"JSON Error: {str(json_err)}")
                    logger.error("The API may have returned invalid JSON or an error page.")
                    logger.info(f"Retrying in {poll_interval}s...")
                    time.sleep(poll_interval)
                    continue
                
                # Auto-detect and extract records with error handling
                try:
                    detected_path, records = auto_detect_and_extract(json_data, data_path)
                except Exception as extract_err:
                    logger.error(f"ERROR: Failed to extract data from API response: {str(extract_err)}")
                    logger.error(f"JSON structure may be unexpected. Response sample: {str(json_data)[:300]}")
                    logger.info(f"Retrying in {poll_interval}s...")
                    time.sleep(poll_interval)
                    continue
                
                if poll_count == 1:
                    logger.info(f"Detected data path: '{detected_path}' with {len(records)} records")
                
                # Check if we got any records
                if not records or len(records) == 0:
                    logger.warning(f"WARNING: No records found in API response")
                    logger.warning(f"API returned valid response but no data to sync.")
                    logger.info(f"Waiting {poll_interval}s before next poll...")
                    time.sleep(poll_interval)
                    continue
                
                # Create table on first poll (using ReplacingMergeTree for UPSERT)
                if not table_created and auto_create_table:
                    logger.info(f"Creating table {target_database}.{target_table} with UPSERT support...")
                    
                    # Get complete schema
                    all_columns = {}
                    for record in records[:100]:
                        flat = flatten_record(record)
                        for key, value in flat.items():
                            safe_key = key.replace('.', '_').replace(' ', '_').replace('-', '_')
                            if safe_key not in all_columns:
                                all_columns[safe_key] = infer_clickhouse_type(value)
                    
                    # Add metadata
                    all_columns['_sync_timestamp'] = 'DateTime64(3)'
                    all_columns['_source_api'] = 'String'
                    
                    # Make ID column non-nullable for sorting key
                    safe_id_column = id_column.replace('.', '_').replace(' ', '_').replace('-', '_')
                    if safe_id_column in all_columns:
                        # Remove Nullable wrapper if present
                        all_columns[safe_id_column] = all_columns[safe_id_column].replace('Nullable(', '').replace(')', '')
                        if all_columns[safe_id_column] == 'String':
                            pass  # String is fine
                        elif not all_columns[safe_id_column]:
                            all_columns[safe_id_column] = 'String'  # Default to String if empty
                    
                    # Create database
                    client.execute(f"CREATE DATABASE IF NOT EXISTS {target_database}")
                    
                    # Drop old table if exists
                    client.execute(f"DROP TABLE IF EXISTS {target_database}.{target_table}")
                    
                    # Create table with ReplacingMergeTree for UPSERT
                    columns_def = [f"`{col}` {dtype}" for col, dtype in all_columns.items()]
                    
                    create_query = f"""
                    CREATE TABLE {target_database}.{target_table} (
                        {', '.join(columns_def)}
                    ) ENGINE = ReplacingMergeTree(_sync_timestamp)
                    ORDER BY `{safe_id_column}`
                    SETTINGS allow_nullable_key = 1
                    """
                    
                    client.execute(create_query)
                    logger.info(f"Created UPSERT table with {len(all_columns)} columns")
                    table_created = True
                
                # Flatten and insert ALL records (ReplacingMergeTree will handle duplicates)
                flattened_records = []
                for record in records:
                    flat = flatten_record(record)
                    flat['_sync_timestamp'] = datetime.now()
                    flat['_source_api'] = api_url
                    
                    # Safe column names
                    safe_flat = {}
                    for key, value in flat.items():
                        safe_key = key.replace('.', '_').replace(' ', '_').replace('-', '_')
                        safe_flat[safe_key] = value
                    
                    flattened_records.append(safe_flat)
                
                # Insert all records
                if flattened_records:
                    columns = list(flattened_records[0].keys())
                    
                    # Ensure all columns exist in the table (schema evolution)
                    # This handles cases where the API schema changes and adds new columns
                    if table_created:  # Only check if table already exists (was created in previous cycles)
                        # Build dict of column_name -> sample_value for type inference
                        sample_record = flattened_records[0]
                        ensure_columns_exist(client, target_database, target_table, sample_record)
                    
                    insert_query = f"INSERT INTO {target_database}.{target_table} ({', '.join([f'`{c}`' for c in columns])}) VALUES"
                    try:
                        client.execute(insert_query, flattened_records)
                    except Exception as insert_error:
                        error_str = str(insert_error)
                        # Handle specific ClickHouse errors
                        if "No such column" in error_str:
                            logger.warning(f"WARNING: Insert failed due to missing columns, attempting schema evolution...")
                            try:
                                sample_record = flattened_records[0] if flattened_records else {}
                                ensure_columns_exist(client, target_database, target_table, sample_record)
                                # Retry the insert
                                client.execute(insert_query, flattened_records)
                                logger.info(f"Successfully inserted after adding missing columns")
                            except Exception as retry_err:
                                logger.error(f"ERROR: Failed to fix schema and retry insert: {str(retry_err)}")
                                raise
                        elif "Connection refused" in error_str or "Unable to connect" in error_str:
                            logger.error(f"ERROR: Cannot connect to ClickHouse database")
                            logger.error("Please check ClickHouse server is running and connection settings are correct.")
                            raise
                        elif "Database" in error_str and "doesn't exist" in error_str:
                            logger.error(f"ERROR: ClickHouse database '{target_database}' does not exist")
                            logger.error("Attempting to create database...")
                            try:
                                client.execute(f"CREATE DATABASE IF NOT EXISTS {target_database}")
                                logger.info(f"Database '{target_database}' created successfully")
                                # Retry insert
                                client.execute(insert_query, flattened_records)
                                logger.info(f"Successfully inserted after creating database")
                            except Exception as db_err:
                                logger.error(f"ERROR: Failed to create database: {str(db_err)}")
                                raise
                        else:
                            logger.error(f"ERROR: ClickHouse insert failed: {error_str}")
                            raise
                    
                    # OPTIMIZE to merge duplicates immediately
                    client.execute(f"OPTIMIZE TABLE {target_database}.{target_table} FINAL")
                    
                    logger.info(f"Upserted {len(flattened_records)} records (new + updated)")
                
                # Show stats
                result = client.execute(f"SELECT count() FROM {target_database}.{target_table}")
                logger.info(f"Total rows: {result[0][0]}")
                
                # Sleep before next poll
                time.sleep(poll_interval)
                
            except KeyboardInterrupt:
                logger.info(f"\nUPSERT polling stopped by user")
                break
            except requests.exceptions.RequestException as req_ex:
                logger.error(f"ERROR: Network/HTTP error in poll cycle: {str(req_ex)}")
                logger.error(f"API URL: {api_url}")
                logger.info(f"Retrying in {poll_interval}s...")
                time.sleep(poll_interval)
            except Exception as e:
                logger.error(f"ERROR: Unexpected error in poll cycle: {str(e)}")
                logger.exception("Full error traceback:")
                logger.info(f"Retrying in {poll_interval}s...")
                time.sleep(poll_interval)
                
    except Exception as e:
        logger.error(f"Fatal error in upsert polling: {e}")
        raise

