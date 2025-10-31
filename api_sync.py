"""
API Sync Module - Handles syncing data from REST/SSE APIs to ClickHouse
"""
import requests
import json
import time
import logging
from datetime import datetime
from dateutil import parser as dateutil_parser
from clickhouse_driver import Client
from db_utils import load_clickhouse_config
from api_data_detector import auto_detect_and_extract
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def send_api_sync_email(api_url, target_database, target_table, records_synced, status, error_msg=None, sync_start_time=None, sync_end_time=None):
    """
    Send detailed email notification about API sync
    
    Args:
        api_url: API endpoint that was synced
        target_database: ClickHouse database
        target_table: ClickHouse table
        records_synced: Number of records synced
        status: 'success', 'failed', or 'running'
        error_msg: Error message if failed
        sync_start_time: When sync started
        sync_end_time: When sync ended
    """
    try:
        # Email configuration (from environment or config)
        import os
        smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
        smtp_port = int(os.getenv("SMTP_PORT", "587"))
        smtp_user = os.getenv("SMTP_USER", "saadpractice4@gmail.com")
        smtp_password = os.getenv("SMTP_PASSWORD", "lqcd zyjx ayjh hyef")
        admin_emails = os.getenv("ADMIN_EMAILS", "saad.sayyed@actin.co.in,saadpractice4@gmail.com").split(',')
        
        # Calculate duration
        duration_str = "N/A"
        if sync_start_time and sync_end_time:
            duration = sync_end_time - sync_start_time
            duration_str = str(duration).split('.')[0]  # Remove microseconds
        
        # Create email
        msg = MIMEMultipart('alternative')
        
        # Subject based on status
        if status == 'success':
            msg['Subject'] = f"✅ API Sync Completed - {target_table}"
            status_icon = "✅"
            status_color = "#28a745"
            status_text = "COMPLETED SUCCESSFULLY"
        elif status == 'failed':
            msg['Subject'] = f"❌ API Sync Failed - {target_table}"
            status_icon = "❌"
            status_color = "#dc3545"
            status_text = "FAILED"
        else:
            msg['Subject'] = f"🔄 API Sync Started - {target_table}"
            status_icon = "🔄"
            status_color = "#007bff"
            status_text = "IN PROGRESS"
        
        msg['From'] = smtp_user
        msg['To'] = ', '.join(admin_emails)
        
        # HTML email body
        html_body = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }}
                .container {{ max-width: 600px; margin: 0 auto; background-color: white; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); overflow: hidden; }}
                .header {{ background: linear-gradient(135deg, {status_color} 0%, {status_color}dd 100%); color: white; padding: 30px; text-align: center; }}
                .header h1 {{ margin: 0; font-size: 24px; font-weight: 600; }}
                .status-badge {{ display: inline-block; margin-top: 10px; padding: 8px 20px; background-color: rgba(255,255,255,0.2); border-radius: 20px; font-size: 14px; font-weight: 500; }}
                .content {{ padding: 30px; }}
                .info-row {{ margin: 15px 0; padding: 12px; background-color: #f8f9fa; border-left: 4px solid {status_color}; border-radius: 4px; }}
                .info-label {{ font-weight: 600; color: #555; font-size: 13px; text-transform: uppercase; letter-spacing: 0.5px; }}
                .info-value {{ margin-top: 5px; font-size: 15px; color: #222; word-break: break-all; }}
                .stats-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin: 20px 0; }}
                .stat-box {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 8px; text-align: center; }}
                .stat-number {{ font-size: 32px; font-weight: bold; margin-bottom: 5px; }}
                .stat-label {{ font-size: 12px; opacity: 0.9; text-transform: uppercase; letter-spacing: 1px; }}
                .error-box {{ background-color: #fff3cd; border: 1px solid #ffc107; border-radius: 5px; padding: 15px; margin: 15px 0; }}
                .error-text {{ color: #856404; font-family: 'Courier New', monospace; font-size: 13px; white-space: pre-wrap; }}
                .footer {{ background-color: #f8f9fa; padding: 20px; text-align: center; font-size: 12px; color: #666; border-top: 1px solid #dee2e6; }}
                .timestamp {{ color: #888; font-size: 11px; margin-top: 5px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>{status_icon} API to ClickHouse Sync</h1>
                    <div class="status-badge">{status_text}</div>
                </div>
                
                <div class="content">
                    <div class="info-row">
                        <div class="info-label">📡 API Endpoint</div>
                        <div class="info-value">{api_url}</div>
                    </div>
                    
                    <div class="info-row">
                        <div class="info-label">🎯 Destination</div>
                        <div class="info-value">ClickHouse → {target_database}.{target_table}</div>
                    </div>
                    
                    <div class="stats-grid">
                        <div class="stat-box">
                            <div class="stat-number">{records_synced}</div>
                            <div class="stat-label">Records Synced</div>
                        </div>
                        <div class="stat-box">
                            <div class="stat-number">{duration_str}</div>
                            <div class="stat-label">Duration</div>
                        </div>
                    </div>
                    
                    <div class="info-row">
                        <div class="info-label">⏰ Started</div>
                        <div class="info-value">{sync_start_time.strftime('%Y-%m-%d %H:%M:%S') if sync_start_time else 'N/A'}</div>
                    </div>
                    
                    <div class="info-row">
                        <div class="info-label">✅ Completed</div>
                        <div class="info-value">{sync_end_time.strftime('%Y-%m-%d %H:%M:%S') if sync_end_time else 'In Progress...'}</div>
                    </div>
                    
                    {f'''<div class="error-box">
                        <div class="info-label">❌ Error Details</div>
                        <div class="error-text">{error_msg}</div>
                    </div>''' if error_msg else ''}
                </div>
                
                <div class="footer">
                    <p>This is an automated notification from your Data Sync System</p>
                    <div class="timestamp">Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
                </div>
            </div>
        </body>
        </html>
        """
        
        msg.attach(MIMEText(html_body, 'html'))
        
        # Send email
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
        
        logger.info(f"✓ Email notification sent to {len(admin_emails)} recipients")
        return True
        
    except Exception as e:
        logger.error(f"Failed to send email notification: {e}")
        return False


def infer_clickhouse_type(value):
    """Infer ClickHouse data type from a Python value
    
    Note: We use Float64 for all numeric types to handle both integers and decimals
    This avoids type mismatch errors when a column might contain both.
    """
    if value is None:
        return "Nullable(String)"
    elif isinstance(value, bool):
        return "UInt8"
    elif isinstance(value, (int, float)):
        # Use Float64 for ALL numeric types to handle mixed int/float/null values
        return "Nullable(Float64)"
    elif isinstance(value, str):
        # Check if it looks like a datetime
        if 'T' in value and 'Z' in value:
            try:
                datetime.fromisoformat(value.replace('Z', '+00:00'))
                return "Nullable(String)"  # Store as string for simplicity
            except:
                pass
        return "Nullable(String)"  # Use Nullable for all strings
    elif isinstance(value, dict):
        return "Nullable(String)"  # Store as JSON string
    elif isinstance(value, list):
        return "Nullable(String)"  # Store as JSON string
    else:
        return "Nullable(String)"


def create_clickhouse_table_from_sample(client, database, table_name, sample_data, already_flattened=False):
    """
    Auto-create ClickHouse table based on sample data structure
    
    Args:
        client: ClickHouse client
        database: Target database name
        table_name: Target table name
        sample_data: Sample record to infer schema from
        already_flattened: If True, sample_data is already flattened and should not be flattened again
    """
    try:
        # Flatten nested data if not already flattened
        if already_flattened:
            flat_data = sample_data
        else:
            flat_data = {}
            
            if isinstance(sample_data, dict):
                for key, value in sample_data.items():
                    if isinstance(value, dict):
                        # Flatten nested objects
                        for nested_key, nested_value in value.items():
                            flat_key = f"{key}_{nested_key}"
                            flat_data[flat_key] = nested_value
                    else:
                        flat_data[key] = value
            else:
                logger.error(f"Sample data is not a dictionary: {type(sample_data)}")
                return False
        
        # Build column definitions (excluding metadata columns)
        columns = []
        for col_name, col_value in flat_data.items():
            # Skip metadata columns if they're in the sample
            if col_name in ['_sync_timestamp', '_source_api']:
                continue
                
            # Clean column name
            clean_name = col_name.replace('-', '_').replace(' ', '_').replace('.', '_')
            col_type = infer_clickhouse_type(col_value)
            columns.append(f"`{clean_name}` {col_type}")
        
        # Add metadata columns at the end
        columns.append("`_sync_timestamp` DateTime64(3) DEFAULT now64(3)")
        columns.append("`_source_api` String")
        
        # Create table with MergeTree engine
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {database}.{table_name} (
            {', '.join(columns)}
        ) ENGINE = MergeTree()
        ORDER BY tuple()
        """
        
        logger.info(f"Creating table: {database}.{table_name}")
        logger.debug(f"SQL: {create_table_sql}")
        
        client.execute(create_table_sql)
        logger.info(f"✓ Table {database}.{table_name} created successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        return False


def convert_datetime_values(value):
    """Convert ISO datetime strings to Python datetime objects, then to strings for ClickHouse"""
    if isinstance(value, str):
        # Check if it looks like a datetime string
        if 'T' in value and ('Z' in value or '+' in value or value.endswith(':00')):
            try:
                # Parse ISO format datetime string and convert back to string for ClickHouse
                dt = dateutil_parser.isoparse(value.replace('Z', '+00:00'))
                # Return as string in ClickHouse-compatible format
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                pass
    return value


def flatten_record(record):
    """Flatten nested dictionaries in a record"""
    flat = {}
    
    for key, value in record.items():
        if isinstance(value, dict):
            # Flatten nested objects
            for nested_key, nested_value in value.items():
                flat_key = f"{key}_{nested_key}"
                if isinstance(nested_value, (dict, list)):
                    flat[flat_key] = json.dumps(nested_value)
                else:
                    # Convert datetime strings
                    flat[flat_key] = convert_datetime_values(nested_value)
        elif isinstance(value, list):
            flat[key] = json.dumps(value)
        else:
            # Convert datetime strings
            flat[key] = convert_datetime_values(value)
    
    # Clean column names
    cleaned = {}
    for key, value in flat.items():
        clean_key = key.replace('-', '_').replace(' ', '_').replace('.', '_')
        cleaned[clean_key] = value
    
    return cleaned


def sync_api_to_clickhouse_once(api_url, target_database, target_table, 
                                auth_type="none", auth_token="", 
                                basic_username="", basic_password="",
                                apikey_header="X-API-Key",
                                custom_headers=None,
                                request_method="GET",
                                data_path="",
                                auto_create_table=True):
    """
    Sync data from REST API to ClickHouse ONE TIME (not continuous/polling)
    This is used when user clicks "Add & Start Sync" or "Sync Server" for REST APIs
    
    Args:
        api_url: API endpoint URL
        target_database: ClickHouse database name
        target_table: ClickHouse table name
        auth_type: Authentication type (none, bearer, basic, apikey)
        auth_token: Token for bearer/apikey auth
        basic_username: Username for basic auth
        basic_password: Password for basic auth
        apikey_header: Header name for API key
        custom_headers: Additional headers
        request_method: GET or POST
        data_path: JSON path to data array (e.g., "data.items")
        auto_create_table: Auto-create table from first record
        
    Returns:
        dict: {"success": bool, "records_synced": int, "error": str or None}
    """
    
    # Track sync metrics
    sync_start_time = datetime.now()
    records_synced = 0
    error_msg = None
    
    logger.info(f"🔄 Starting ONE-TIME REST API sync from: {api_url}")
    logger.info(f"📊 Target: {target_database}.{target_table}")
    
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
        
        # Make API request
        logger.info(f"Making {request_method} request to {api_url}")
        if request_method == "GET":
            response = requests.get(api_url, headers=headers, auth=auth, timeout=30)
        else:
            response = requests.post(api_url, headers=headers, auth=auth, timeout=30)
        
        # Check response
        if response.status_code != 200:
            error_msg = f"API returned status {response.status_code}: {response.reason}"
            logger.error(error_msg)
            return {"success": False, "records_synced": 0, "error": error_msg}
        
        # Parse JSON response
        try:
            json_data = response.json()
        except Exception as e:
            error_msg = f"Failed to parse JSON response: {str(e)}"
            logger.error(error_msg)
            return {"success": False, "records_synced": 0, "error": error_msg}
        
        # Auto-detect and extract data using smart detection
        detected_path, records = auto_detect_and_extract(json_data, data_path)
        logger.info(f"📊 Using data path: '{detected_path}' (found {len(records)} records)")
        
        if not records:
            logger.info("No records found in API response")
            return {"success": True, "records_synced": 0, "error": None}
        
        logger.info(f"Found {len(records)} records to sync")
        
        # First, flatten ALL records to get complete schema
        all_columns = set()
        flattened_records = []
        for record in records:
            flat_record = flatten_record(record)
            flat_record['_source_api'] = api_url
            flat_record['_sync_timestamp'] = datetime.now()
            all_columns.update(flat_record.keys())
            flattened_records.append(flat_record)
        
        # Sort columns for consistent order
        column_list = sorted(list(all_columns))
        logger.info(f"Detected {len(column_list)} unique columns across all records")
        
        # Auto-create table with complete schema if needed
        table_created = False
        if auto_create_table:
            # Use the first flattened record for table creation (contains all columns now)
            # But we need to create a sample with ALL possible columns
            complete_sample = {}
            for col in column_list:
                # Get first non-None value for each column
                for flat_record in flattened_records:
                    if col in flat_record and flat_record[col] is not None:
                        complete_sample[col] = flat_record[col]
                        break
                # If all values are None, use None
                if col not in complete_sample:
                    complete_sample[col] = None
            
            if create_clickhouse_table_from_sample(client, target_database, target_table, complete_sample, already_flattened=True):
                table_created = True
                logger.info(f"✅ Created table {target_database}.{target_table} with {len(column_list)} columns")
            else:
                error_msg = "Failed to create table"
                logger.error(error_msg)
                return {"success": False, "records_synced": 0, "error": error_msg}
        
        # Insert all records as a batch with consistent column structure
        try:
            # Prepare all rows for batch insert
            batch_values = []
            for flat_record in flattened_records:
                # Ensure all columns are present (fill missing with None)
                values = [flat_record.get(col, None) for col in column_list]
                batch_values.append(values)
            
            # Execute batch insert
            insert_sql = f"INSERT INTO {target_database}.{target_table} ({', '.join([f'`{c}`' for c in column_list])}) VALUES"
            client.execute(insert_sql, batch_values)
            records_synced = len(batch_values)
            logger.info(f"✅ Successfully inserted {records_synced} records in batch")
            
        except Exception as e:
            logger.error(f"Batch insert failed: {e}")
            # Fallback to row-by-row insert with error handling
            logger.info("Falling back to row-by-row insert...")
            for i, flat_record in enumerate(flattened_records):
                try:
                    # Ensure all columns are present (fill missing with None)
                    values = [flat_record.get(col, None) for col in column_list]
                    
                    insert_sql = f"INSERT INTO {target_database}.{target_table} ({', '.join([f'`{c}`' for c in column_list])}) VALUES"
                    client.execute(insert_sql, [values])
                    
                    records_synced += 1
                except Exception as row_error:
                    logger.error(f"Error inserting record {i+1}: {row_error}")
                    # Continue with other records
        
        sync_end_time = datetime.now()
        duration = (sync_end_time - sync_start_time).total_seconds()
        
        logger.info(f"✅ Sync completed successfully: {records_synced} records synced in {duration:.2f}s")
        
        # Send success email
        send_api_sync_email(api_url, target_database, target_table, records_synced, 'success', 
                          sync_start_time=sync_start_time, sync_end_time=sync_end_time)
        
        return {"success": True, "records_synced": records_synced, "error": None}
        
    except Exception as e:
        logger.exception(f"Error syncing API to ClickHouse: {e}")
        error_msg = str(e)
        sync_end_time = datetime.now()
        send_api_sync_email(api_url, target_database, target_table, records_synced, 'failed', 
                          error_msg=error_msg, sync_start_time=sync_start_time, sync_end_time=sync_end_time)
        return {"success": False, "records_synced": records_synced, "error": error_msg}


def sync_api_to_clickhouse(api_url, target_database, target_table, 
                           auth_type="none", auth_token="", 
                           basic_username="", basic_password="",
                           apikey_header="X-API-Key",
                           custom_headers=None,
                           request_method="GET",
                           data_path="",
                           is_sse=False,
                           auto_create_table=True):
    """
    Sync data from API to ClickHouse (CONTINUOUS for SSE, or infinite polling for REST)
    This is the OLD function kept for backward compatibility with SSE streams.
    For one-time REST API sync, use sync_api_to_clickhouse_once() instead.
    
    Args:
        api_url: API endpoint URL
        target_database: ClickHouse database name
        target_table: ClickHouse table name
        auth_type: Authentication type (none, bearer, basic, apikey)
        auth_token: Token for bearer/apikey auth
        basic_username: Username for basic auth
        basic_password: Password for basic auth
        apikey_header: Header name for API key
        custom_headers: Additional headers
        request_method: GET or POST
        data_path: JSON path to data array (e.g., "data.items")
        is_sse: Whether this is a Server-Sent Events endpoint
        auto_create_table: Auto-create table from first record
    """
    
    # Track sync metrics
    sync_start_time = datetime.now()
    records_synced = 0
    error_msg = None
    
    # Send "sync started" email
    send_api_sync_email(api_url, target_database, target_table, 0, 'running', sync_start_time=sync_start_time)
    
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
        last_email_time = sync_start_time
        email_interval = 300  # Send status email every 5 minutes for SSE
        
        if is_sse:
            # Handle Server-Sent Events stream - runs continuously FOREVER
            logger.info(f"🔄 Starting continuous SSE sync from: {api_url}")
            logger.info(f"📊 New data will be automatically synced to {target_database}.{target_table}")
            logger.info(f"♾️  Will process UNLIMITED records - sync runs forever until manually stopped!")
            
            retry_count = 0
            backoff_time = 5  # Start with 5 seconds, will increase on rate limits
            
            while True:  # Run forever with automatic reconnection
                try:
                    logger.info(f"Connecting to SSE endpoint (attempt #{retry_count + 1})...")
                    response = requests.get(api_url, headers=headers, auth=auth, stream=True, timeout=None)
                    
                    # Handle rate limiting (429)
                    if response.status_code == 429:
                        backoff_time = min(backoff_time * 2, 300)  # Max 5 minutes
                        logger.warning(f"[RATE LIMIT] Waiting {backoff_time} seconds before retry...")
                        logger.warning(f"Response: {response.text[:200]}")
                        time.sleep(backoff_time)
                        retry_count += 1
                        continue
                    
                    # Handle other errors (404, 500, etc.)
                    if response.status_code < 200 or response.status_code >= 300:
                        logger.error(f"[SSE FAILED] Connection failed with status {response.status_code}")
                        logger.error(f"Response: {response.text[:500]}")
                        logger.error(f"URL: {api_url}")
                        logger.error(f"Check: 1) URL is correct, 2) Endpoint exists, 3) Using correct HTTP method (GET for SSE)")
                        retry_count += 1
                        time.sleep(backoff_time)
                        continue
                    
                    logger.info("[SUCCESS] Connected to SSE stream, listening for real-time events...")
                    logger.info("[UNLIMITED] Processing unlimited records - will run forever!")
                    retry_count = 0  # Reset retry count on successful connection
                    backoff_time = 5  # Reset backoff time
                    
                    for line in response.iter_lines():
                        if line:
                            decoded_line = line.decode('utf-8')
                            
                            # SSE format: "data: {json}"
                            if decoded_line.startswith('data:'):
                                try:
                                    json_str = decoded_line[5:].strip()
                                    event_data = json.loads(json_str)
                                    
                                    event_type = event_data.get('type', 'unknown')
                                    logger.info(f"📥 Received SSE event: {event_type}")
                                    
                                    # Determine which records to process based on event type
                                    records_to_process = []
                                    
                                    if event_type == 'connected':
                                        # Connected event: just log, don't insert (it's a status message)
                                        logger.info("✅ Connected to SSE stream - waiting for data...")
                                        continue  # Skip to next event
                                        
                                    elif event_type == 'initial_data':
                                        # Initial data: contains array of records in 'data' field
                                        data_array = event_data.get('data', [])
                                        if isinstance(data_array, list):
                                            records_to_process = data_array
                                            logger.info(f"📊 Initial data batch: {len(data_array)} records")
                                        else:
                                            records_to_process = [data_array]
                                            
                                    elif event_type == 'new_data':
                                        # New data: single record in 'data' field
                                        single_record = event_data.get('data')
                                        if single_record:
                                            records_to_process = [single_record]
                                            logger.info(f"🆕 New data record: ID {single_record.get('id', 'N/A')}")
                                        else:
                                            records_to_process = [event_data]
                                    else:
                                        # Unknown event type: try to extract data or use whole event
                                        logger.warning(f"⚠️ Unknown event type: {event_type}")
                                        if 'data' in event_data:
                                            data = event_data['data']
                                            records_to_process = data if isinstance(data, list) else [data]
                                        else:
                                            records_to_process = [event_data]
                                    
                                    # Process all records from this event
                                    for record in records_to_process:
                                        # Auto-create table on first record
                                        if not table_created and auto_create_table:
                                            if create_clickhouse_table_from_sample(client, target_database, target_table, record):
                                                table_created = True
                                                logger.info(f"✅ Created table {target_database}.{target_table}")
                                            else:
                                                logger.error("Failed to create table, aborting sync")
                                                raise Exception("Table creation failed")
                                        
                                        # Flatten and insert record
                                        flat_record = flatten_record(record)
                                        flat_record['_source_api'] = api_url
                                        flat_record['_sync_timestamp'] = datetime.now()
                                        
                                        # Insert into ClickHouse
                                        columns = list(flat_record.keys())
                                        values = [flat_record[col] for col in columns]
                                        
                                        insert_sql = f"INSERT INTO {target_database}.{target_table} ({', '.join([f'`{c}`' for c in columns])}) VALUES"
                                        client.execute(insert_sql, [values])
                                        
                                        records_synced += 1
                                    
                                    current_time = datetime.now()
                                    logger.info(f"✅ Synced {len(records_to_process)} record(s) | Event: {event_type} | Total: {records_synced}")
                                    
                                    # Send periodic status email
                                    if (current_time - last_email_time).seconds >= email_interval:
                                        send_api_sync_email(api_url, target_database, target_table, records_synced, 'running', 
                                                          sync_start_time=sync_start_time, sync_end_time=current_time)
                                        last_email_time = current_time
                                        logger.info(f"📧 Status email sent: {records_synced} records synced so far")
                                    
                                except json.JSONDecodeError as e:
                                    logger.warning(f"Failed to parse SSE data: {e}")
                                except Exception as e:
                                    logger.error(f"Error processing SSE event: {e}")
                                    import traceback
                                    traceback.print_exc()
                    
                    # If loop exits normally, connection was closed by server
                    logger.warning("SSE connection closed by server, reconnecting in 5 seconds...")
                    retry_count += 1
                    time.sleep(5)
                    # Will automatically retry (infinite loop)
                    
                except requests.exceptions.RequestException as e:
                    logger.error(f"Connection error: {e}, reconnecting in 5 seconds...")
                    retry_count += 1
                    time.sleep(5)
                    # Will automatically retry (infinite loop)
                except Exception as e:
                    logger.error(f"Unexpected error in SSE stream: {e}, reconnecting in 5 seconds...")
                    import traceback
                    traceback.print_exc()
                    retry_count += 1
                    time.sleep(5)
                    # Will automatically retry (infinite loop)
                            
        else:
            # Handle regular REST API - poll every 10 seconds FOREVER
            logger.info(f"🔄 Starting continuous REST API polling from: {api_url}")
            logger.info(f"📊 Checking for new data every 10 seconds...")
            logger.info(f"♾️  Will process UNLIMITED records - sync runs forever until manually stopped!")
            
            poll_interval = 10  # seconds
            seen_ids = set()  # Track which records we've already synced
            backoff_time = 10  # Start with 10 seconds
            
            while True:  # Run forever - no limit on number of records
                try:
                    if request_method == "GET":
                        response = requests.get(api_url, headers=headers, auth=auth, timeout=30)
                    else:
                        response = requests.post(api_url, headers=headers, auth=auth, timeout=30)
                    
                    # Handle rate limiting (429)
                    if response.status_code == 429:
                        backoff_time = min(backoff_time * 2, 300)  # Max 5 minutes
                        logger.warning(f"[RATE LIMIT] Waiting {backoff_time} seconds before retry...")
                        logger.warning(f"Response: {response.text[:200]}")
                        time.sleep(backoff_time)
                        continue
                    
                    # Handle 404 errors with helpful message
                    if response.status_code == 404:
                        logger.error(f"[NOT FOUND] API endpoint not found (404)")
                        logger.error(f"URL: {api_url}")
                        logger.error(f"Method: {request_method}")
                        logger.error(f"Response: {response.text[:500]}")
                        logger.error(f"[CHECK] 1) URL is correct, 2) Using correct HTTP method")
                        time.sleep(poll_interval)
                        continue
                    
                    # Handle other errors
                    if response.status_code < 200 or response.status_code >= 300:
                        logger.error(f"[API FAILED] Request failed with status {response.status_code}")
                        logger.error(f"Response: {response.text[:500]}")
                        time.sleep(poll_interval)
                        continue
                    
                    # Success! Reset backoff
                    backoff_time = 10
                    logger.debug(f"[SUCCESS] API responded with status {response.status_code}")
                    
                    # Parse JSON response
                    json_data = response.json()
                    
                    # Auto-detect and extract data using smart detection
                    detected_path, records = auto_detect_and_extract(json_data, data_path)
                    logger.info(f"📊 Using data path: '{detected_path}' (found {len(records)} records)")
                    
                    if not records:
                        logger.info(f"No records found, waiting {poll_interval}s...")
                        time.sleep(poll_interval)
                        continue
                    
                    # Auto-create table from first record
                    if not table_created and auto_create_table:
                        if create_clickhouse_table_from_sample(client, target_database, target_table, records[0]):
                            table_created = True
                            logger.info(f"✅ Created table {target_database}.{target_table}")
                        else:
                            logger.error("Failed to create table, aborting sync")
                            raise Exception("Table creation failed")
                    
                    # Process only new records
                    new_records = 0
                    for record in records:
                        # Generate a unique ID for this record (you may need to adjust this based on your data)
                        record_id = json.dumps(record, sort_keys=True)
                        
                        if record_id not in seen_ids:
                            flat_record = flatten_record(record)
                            flat_record['_source_api'] = api_url
                            flat_record['_sync_timestamp'] = datetime.now()
                            
                            columns = list(flat_record.keys())
                            values = [flat_record[col] for col in columns]
                            
                            insert_sql = f"INSERT INTO {target_database}.{target_table} ({', '.join([f'`{c}`' for c in columns])}) VALUES"
                            client.execute(insert_sql, [values])
                            
                            seen_ids.add(record_id)
                            records_synced += 1
                            new_records += 1
                    
                    if new_records > 0:
                        logger.info(f"✅ Synced {new_records} new records | Total: {records_synced}")
                    else:
                        logger.info(f"📊 No new records found | Total synced: {records_synced}")
                    
                    # Send periodic status email
                    current_time = datetime.now()
                    if (current_time - last_email_time).seconds >= email_interval:
                        send_api_sync_email(api_url, target_database, target_table, records_synced, 'running', 
                                          sync_start_time=sync_start_time, sync_end_time=current_time)
                        last_email_time = current_time
                        logger.info(f"📧 Status email sent: {records_synced} records synced so far")
                    
                    # Wait before next poll
                    time.sleep(poll_interval)
                    
                except Exception as e:
                    logger.error(f"Error in REST API polling: {e}")
                    import traceback
                    traceback.print_exc()
                    time.sleep(poll_interval)
        
        # Sync completed successfully
        sync_end_time = datetime.now()
        send_api_sync_email(api_url, target_database, target_table, records_synced, 'success', 
                          sync_start_time=sync_start_time, sync_end_time=sync_end_time)
        return True
        
    except Exception as e:
        logger.exception(f"Error syncing API to ClickHouse: {e}")
        error_msg = str(e)
        sync_end_time = datetime.now()
        send_api_sync_email(api_url, target_database, target_table, records_synced, 'failed', 
                          error_msg=error_msg, sync_start_time=sync_start_time, sync_end_time=sync_end_time)
        return False


if __name__ == "__main__":
    # Example: Sync SSE stream
    print("Starting API sync to ClickHouse...")
    
    success = sync_api_to_clickhouse(
        api_url="http://localhost:3000/api/crm/stream",
        target_database="test4",
        target_table="crm_deals",
        is_sse=True,
        auto_create_table=True
    )
    
    if success:
        print("✓ Sync completed successfully")
    else:
        print("✗ Sync failed")
