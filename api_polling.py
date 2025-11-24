"""
API Polling Module - Continuously checks REST API for new data and syncs to ClickHouse
"""
import requests
import json
import time
import logging
from datetime import datetime
from clickhouse_driver import Client
from db_utils import load_clickhouse_config
from api_data_detector import auto_detect_and_extract
from api_sync import (
    flatten_record, infer_clickhouse_type, create_clickhouse_table_from_sample,
    send_api_sync_email
)

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


def poll_api_to_clickhouse(api_url, target_database, target_table,
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
    Poll REST API continuously and sync new data to ClickHouse
    
    Args:
        api_url: API endpoint URL
        target_database: ClickHouse database name
        target_table: ClickHouse table name
        auth_type: Authentication type
        poll_interval: Seconds between API calls (default: 5)
        id_column: Column name to use for deduplication (default: "id")
        data_path: JSON path to array (e.g., "data" for data['data'])
    """
    
    sync_start_time = datetime.now()
    total_records_synced = 0
    poll_count = 0
    
    # Import sync logger
    try:
        from sync_logger import SyncLogger
        use_sync_logger = True
        source_name = api_url  # Use API URL as source name
        SyncLogger.log_sync_start('REST_API_POLLING', source_name, 
                                 details={'target_db': target_database, 'target_table': target_table, 
                                         'poll_interval': poll_interval, 'mode': 'polling'})
    except ImportError:
        use_sync_logger = False
    
    logger.info(f"Starting continuous API polling from: {api_url}")
    logger.info(f"Target: {target_database}.{target_table}")
    logger.info(f"Polling interval: {poll_interval} seconds")
    logger.info(f"ID column for deduplication: {id_column}")
    
    # Initialize OAuth token manager if OAuth is enabled
    token_manager = None
    if auth_type == "oauth" and oauth_token_url and oauth_username and oauth_password:
        from oauth_token_manager import get_token_manager
        logger.info(f"OAuth authentication enabled")
        logger.info(f"Token URL: {oauth_token_url}")
        logger.info(f"Username: {oauth_username}")
        logger.info(f"Token refresh interval: {oauth_refresh_interval}s")
        token_manager = get_token_manager(
            oauth_token_url, oauth_username, oauth_password, oauth_refresh_interval
        )
    
    logger.info(f"Will run forever until manually stopped!")
    
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
        elif auth_type == "oauth" and token_manager:
            # OAuth token will be added dynamically in the loop
            pass
        
        # Prepare auth
        auth = None
        if auth_type == "basic" and basic_username and basic_password:
            auth = (basic_username, basic_password)
        
        table_created = False
        seen_ids = set()  # Track IDs we've already synced
        
        while True:  # Poll forever
            try:
                poll_count += 1
                poll_start = datetime.now()
                
                logger.info(f"Poll #{poll_count}: Fetching data from API...")
                
                # Get fresh OAuth token if using OAuth
                if auth_type == "oauth" and token_manager:
                    try:
                        current_token = token_manager.get_token()
                        if current_token:
                            headers["Authorization"] = f"Bearer {current_token}"
                            logger.debug(f"Using OAuth token (expires soon: check manager)")
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
                
                # Make API request with comprehensive error handling
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
                
                # Auto-detect and extract data using smart detection
                try:
                    detected_path, records = auto_detect_and_extract(json_data, data_path)
                except Exception as extract_err:
                    logger.error(f"ERROR: Failed to extract data from API response: {str(extract_err)}")
                    logger.error(f"JSON structure may be unexpected. Response sample: {str(json_data)[:300]}")
                    logger.info(f"Retrying in {poll_interval}s...")
                    time.sleep(poll_interval)
                    continue
                
                # Check if we got any records
                if not records or len(records) == 0:
                    logger.warning(f"WARNING: No records found in API response")
                    logger.warning(f"API returned valid response but no data to sync.")
                    logger.info(f"Waiting {poll_interval}s before next poll...")
                    time.sleep(poll_interval)
                    continue
                
                # Log which path was used (only on first poll)
                if poll_count == 1:
                    logger.info(f"Using data path: '{detected_path}' for future polls")
                
                logger.info(f"Received {len(records)} total records from API")
                
                # Filter for NEW records only (not in seen_ids)
                new_records = []
                for record in records:
                    flat_record = flatten_record(record)
                    record_id = flat_record.get(id_column)
                    
                    if record_id and str(record_id) not in seen_ids:
                        new_records.append(record)
                        seen_ids.add(str(record_id))
                
                logger.info(f"Found {len(new_records)} NEW records to sync")
                
                if new_records:
                    # Flatten all new records
                    all_columns = set()
                    flattened_records = []
                    for record in new_records:
                        flat_record = flatten_record(record)
                        flat_record['_source_api'] = api_url
                        flat_record['_sync_timestamp'] = datetime.now()
                        all_columns.update(flat_record.keys())
                        flattened_records.append(flat_record)
                    
                    column_list = sorted(list(all_columns))
                    
                    # Create table if needed (only on first poll)
                    if not table_created and auto_create_table:
                        complete_sample = {}
                        for col in column_list:
                            for flat_record in flattened_records:
                                if col in flat_record and flat_record[col] is not None:
                                    complete_sample[col] = flat_record[col]
                                    break
                            if col not in complete_sample:
                                complete_sample[col] = None
                        
                        if create_clickhouse_table_from_sample(client, target_database, target_table, 
                                                               complete_sample, already_flattened=True):
                            table_created = True
                            logger.info(f"Created table {target_database}.{target_table} with {len(column_list)} columns")
                    
                    # Insert new records as batch with comprehensive error handling
                    try:
                        # Ensure all columns exist in the table (schema evolution)
                        # Always check for missing columns if table exists (handles API schema changes)
                        if table_created and flattened_records:
                            try:
                                # Build dict of column_name -> sample_value for type inference
                                sample_record = flattened_records[0]
                                ensure_columns_exist(client, target_database, target_table, sample_record)
                            except Exception as schema_err:
                                logger.error(f"ERROR: Failed to check/add missing columns: {str(schema_err)}")
                                logger.error("Continuing with insert anyway...")
                        
                        batch_values = []
                        for flat_record in flattened_records:
                            values = [flat_record.get(col, None) for col in column_list]
                            batch_values.append(values)
                        
                        insert_sql = f"INSERT INTO {target_database}.{target_table} ({', '.join([f'`{c}`' for c in column_list])}) VALUES"
                        try:
                            client.execute(insert_sql, batch_values)
                        except Exception as insert_error:
                            error_str = str(insert_error)
                            # Handle specific ClickHouse errors
                            if "No such column" in error_str:
                                logger.warning(f"WARNING: Insert failed due to missing columns, attempting schema evolution...")
                                try:
                                    sample_record = flattened_records[0] if flattened_records else {}
                                    ensure_columns_exist(client, target_database, target_table, sample_record)
                                    # Retry the insert
                                    client.execute(insert_sql, batch_values)
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
                                    client.execute(insert_sql, batch_values)
                                    logger.info(f"Successfully inserted after creating database")
                                except Exception as db_err:
                                    logger.error(f"ERROR: Failed to create database: {str(db_err)}")
                                    raise
                            else:
                                logger.error(f"ERROR: ClickHouse insert failed: {error_str}")
                                raise
                        
                        total_records_synced += len(batch_values)
                        logger.info(f"Inserted {len(batch_values)} new records | Total synced: {total_records_synced}")
                        
                    except Exception as insert_exception:
                        logger.error(f"ERROR: Failed to insert data batch into ClickHouse")
                        logger.error(f"Error details: {str(insert_exception)}")
                        logger.error(f"Target: {target_database}.{target_table}")
                        logger.error(f"Records affected: {len(flattened_records) if 'flattened_records' in locals() else 0}")
                        # Don't break the loop, just log and continue
                        logger.info(f"Continuing polling, will retry on next cycle...")
                else:
                    logger.info(f"No new records found (all records already synced)")
                
                # Wait before next poll
                poll_duration = (datetime.now() - poll_start).total_seconds()
                sleep_time = max(0, poll_interval - poll_duration)
                
                if sleep_time > 0:
                    logger.info(f"Waiting {sleep_time:.1f}s until next poll...")
                    time.sleep(sleep_time)
                
            except KeyboardInterrupt:
                logger.info(f"\nPolling stopped by user")
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
        
        # Summary
        duration = (datetime.now() - sync_start_time).total_seconds()
        logger.info(f"\nPolling Summary:")
        logger.info(f"  Total polls: {poll_count}")
        logger.info(f"  Total records synced: {total_records_synced}")
        logger.info(f"  Duration: {duration:.1f}s")
        
        return {"success": True, "records_synced": total_records_synced, "polls": poll_count, "error": None}
        
    except Exception as e:
        logger.exception(f"Fatal error in API polling: {e}")
        return {"success": False, "records_synced": total_records_synced, "polls": poll_count, "error": str(e)}

