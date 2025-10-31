"""
Auto-start polling for all configured API sources when Flask starts
Add this to app.py after the Flask app is created
"""
import threading
import psycopg2
import os
import json
from dotenv import load_dotenv

load_dotenv()

def auto_start_polling_sources(app):
    """
    Auto-start polling threads for all API sources with polling_mode=True
    Call this function when Flask app starts
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv('PG_HOST', 'localhost'),
            port=os.getenv('PG_PORT', 5432),
            database=os.getenv('PG_DATABASE', 'test1'),
            user=os.getenv('PG_USERNAME', 'migration_user'),
            password=os.getenv('PG_PASSWORD', 'StrongPassword123')
        )
        
        cur = conn.cursor()
        cur.execute("""
            SELECT id, source_name, server_address, target_database, connection_details
            FROM data_sources
            WHERE source_type = 'rest_api'
        """)
        
        sources = cur.fetchall()
        cur.close()
        conn.close()
        
        if not sources:
            app.logger.info("No API sources found for auto-start")
            return
        
        app.logger.info(f"Found {len(sources)} API sources, checking for polling mode...")
        
        from api_polling import poll_api_to_clickhouse
        from api_sync import sync_api_to_clickhouse
        
        started_count = 0
        
        for source in sources:
            source_id, name, url, target_db, details = source
            
            if not details:
                continue
            
            polling_mode = details.get('polling_mode', False)
            is_sse = details.get('is_sse', False)
            
            if not polling_mode and not is_sse:
                continue  # Skip one-time sync sources
            
            # Parse configuration
            target_table = details.get('target_table', 'api_data')
            poll_interval = details.get('poll_interval', 5)
            id_column = details.get('id_column', 'id')
            upsert_mode = details.get('upsert_mode', False)
            auth_type = details.get('auth_type')
            auth_token = details.get('auth_token')
            basic_username = details.get('basic_username')
            basic_password = details.get('basic_password')
            apikey_header = details.get('apikey_header')
            oauth_token_url = details.get('oauth_token_url', '')
            oauth_username = details.get('oauth_username', '')
            oauth_password = details.get('oauth_password', '')
            oauth_refresh_interval = details.get('oauth_refresh_interval', 3600)
            request_method = details.get('request_method', 'GET')
            data_path = details.get('data_path', '')
            
            # Parse custom headers
            custom_headers = {}
            custom_headers_str = details.get('custom_headers', '')
            if custom_headers_str:
                try:
                    custom_headers = json.loads(custom_headers_str) if isinstance(custom_headers_str, str) else custom_headers_str
                except:
                    pass
            
            # Start background thread
            def start_sync(source_name=name, api_url=url, target_db=target_db, target_tbl=target_table, is_poll=polling_mode, is_upsert=upsert_mode):
                try:
                    if is_poll and is_upsert:
                        app.logger.info(f"🚀 AUTO-START: UPSERT mode '{source_name}' every {poll_interval}s")
                        from api_upsert import upsert_api_to_clickhouse
                        upsert_api_to_clickhouse(
                            api_url=api_url,
                            target_database=target_db,
                            target_table=target_tbl,
                            auth_type=auth_type,
                            auth_token=auth_token,
                            basic_username=basic_username,
                            basic_password=basic_password,
                            apikey_header=apikey_header,
                            custom_headers=custom_headers,
                            request_method=request_method,
                            data_path=data_path,
                            poll_interval=poll_interval,
                            id_column=id_column,
                            auto_create_table=True,
                            oauth_token_url=oauth_token_url,
                            oauth_username=oauth_username,
                            oauth_password=oauth_password,
                            oauth_refresh_interval=oauth_refresh_interval
                        )
                    elif is_poll:
                        app.logger.info(f"🚀 AUTO-START: Polling '{source_name}' every {poll_interval}s")
                        poll_api_to_clickhouse(
                            api_url=api_url,
                            target_database=target_db,
                            target_table=target_tbl,
                            auth_type=auth_type,
                            auth_token=auth_token,
                            basic_username=basic_username,
                            basic_password=basic_password,
                            apikey_header=apikey_header,
                            custom_headers=custom_headers,
                            request_method=request_method,
                            data_path=data_path,
                            poll_interval=poll_interval,
                            id_column=id_column,
                            auto_create_table=True,
                            oauth_token_url=oauth_token_url,
                            oauth_username=oauth_username,
                            oauth_password=oauth_password,
                            oauth_refresh_interval=oauth_refresh_interval
                        )
                    else:  # SSE
                        app.logger.info(f"🚀 AUTO-START: SSE stream '{source_name}'")
                        sync_api_to_clickhouse(
                            api_url=api_url,
                            target_database=target_db,
                            target_table=target_tbl,
                            auth_type=auth_type,
                            auth_token=auth_token,
                            basic_username=basic_username,
                            basic_password=basic_password,
                            apikey_header=apikey_header,
                            custom_headers=custom_headers,
                            request_method=request_method,
                            data_path=data_path,
                            is_sse=True,
                            auto_create_table=True
                        )
                except Exception as e:
                    app.logger.error(f"Error auto-starting '{source_name}': {e}")
            
            thread = threading.Thread(target=start_sync, daemon=True)
            thread.start()
            started_count += 1
        
        app.logger.info(f"✅ AUTO-STARTED {started_count} polling/SSE sources")
        
    except Exception as e:
        app.logger.error(f"Error in auto-start polling: {e}")


# To integrate into app.py, add this after creating the Flask app:
"""
if __name__ == '__main__':
    # Auto-start polling for all configured sources
    from flask_auto_start_polling import auto_start_polling_sources
    auto_start_polling_sources(app)
    
    # Then run Flask
    app.run(debug=True, host='0.0.0.0', port=5000)
"""

