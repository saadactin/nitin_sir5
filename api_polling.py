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


def poll_api_to_clickhouse(api_url, target_database, target_table,
                           auth_type="none", auth_token="",
                           basic_username="", basic_password="",
                           apikey_header="X-API-Key",
                           custom_headers=None,
                           request_method="GET",
                           data_path="data",
                           poll_interval=5,
                           id_column="id",
                           auto_create_table=True):
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
    
    logger.info(f"🔄 Starting continuous API polling from: {api_url}")
    logger.info(f"📊 Target: {target_database}.{target_table}")
    logger.info(f"⏱️  Polling interval: {poll_interval} seconds")
    logger.info(f"🔑 ID column for deduplication: {id_column}")
    logger.info(f"♾️  Will run forever until manually stopped!")
    
    try:
        # Connect to ClickHouse
        ch_conf = load_clickhouse_config()
        client = Client(
            host=ch_conf['host'],
            port=ch_conf['port'],
            user=ch_conf['user'],
            password=ch_conf['password']
        )
        
        # Prepare headers
        headers = custom_headers.copy() if custom_headers else {}
        
        if auth_type == "bearer" and auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"
        elif auth_type == "apikey" and auth_token and apikey_header:
            headers[apikey_header] = auth_token
        
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
                
                logger.info(f"📡 Poll #{poll_count}: Fetching data from API...")
                
                # Make API request
                if request_method == "GET":
                    response = requests.get(api_url, headers=headers, auth=auth, timeout=30)
                else:
                    response = requests.post(api_url, headers=headers, auth=auth, timeout=30)
                
                if response.status_code != 200:
                    logger.error(f"API returned status {response.status_code}, retrying in {poll_interval}s...")
                    time.sleep(poll_interval)
                    continue
                
                # Parse JSON and auto-detect data path
                json_data = response.json()
                
                # Auto-detect and extract data using smart detection
                detected_path, records = auto_detect_and_extract(json_data, data_path)
                
                # Log which path was used (only on first poll)
                if poll_count == 1:
                    logger.info(f"📊 Using data path: '{detected_path}' for future polls")
                
                logger.info(f"📊 Received {len(records)} total records from API")
                
                # Filter for NEW records only (not in seen_ids)
                new_records = []
                for record in records:
                    flat_record = flatten_record(record)
                    record_id = flat_record.get(id_column)
                    
                    if record_id and str(record_id) not in seen_ids:
                        new_records.append(record)
                        seen_ids.add(str(record_id))
                
                logger.info(f"✨ Found {len(new_records)} NEW records to sync")
                
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
                            logger.info(f"✅ Created table {target_database}.{target_table} with {len(column_list)} columns")
                    
                    # Insert new records as batch
                    try:
                        batch_values = []
                        for flat_record in flattened_records:
                            values = [flat_record.get(col, None) for col in column_list]
                            batch_values.append(values)
                        
                        insert_sql = f"INSERT INTO {target_database}.{target_table} ({', '.join([f'`{c}`' for c in column_list])}) VALUES"
                        client.execute(insert_sql, batch_values)
                        
                        total_records_synced += len(batch_values)
                        logger.info(f"✅ Inserted {len(batch_values)} new records | Total synced: {total_records_synced}")
                        
                    except Exception as e:
                        logger.error(f"Error inserting batch: {e}")
                else:
                    logger.info(f"ℹ️  No new records found (all records already synced)")
                
                # Wait before next poll
                poll_duration = (datetime.now() - poll_start).total_seconds()
                sleep_time = max(0, poll_interval - poll_duration)
                
                if sleep_time > 0:
                    logger.info(f"⏳ Waiting {sleep_time:.1f}s until next poll...")
                    time.sleep(sleep_time)
                
            except KeyboardInterrupt:
                logger.info(f"\n⏹️  Polling stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in poll cycle: {e}")
                logger.info(f"Retrying in {poll_interval}s...")
                time.sleep(poll_interval)
        
        # Summary
        duration = (datetime.now() - sync_start_time).total_seconds()
        logger.info(f"\n📊 Polling Summary:")
        logger.info(f"  Total polls: {poll_count}")
        logger.info(f"  Total records synced: {total_records_synced}")
        logger.info(f"  Duration: {duration:.1f}s")
        
        return {"success": True, "records_synced": total_records_synced, "polls": poll_count, "error": None}
        
    except Exception as e:
        logger.exception(f"Fatal error in API polling: {e}")
        return {"success": False, "records_synced": total_records_synced, "polls": poll_count, "error": str(e)}

