"""
Main routes - Homepage and core navigation
"""
from flask import Blueprint, render_template, session, request
import json
import psycopg2
from routes.utils import load_config, test_sql_connection
from auth import require_role

# Create blueprint
main_bp = Blueprint('main', __name__)


@main_bp.route("/")
@require_role(["admin", "operator", "viewer"])
def index():
    """Homepage → show available servers and sync option"""
    from flask import current_app as app
    
    app.logger.info(f"[HOME] Homepage accessed by user: {session.get('username', 'Unknown')}")
    config = load_config()
    sqlservers = config.get("sqlservers", {})
    
    # Check connection status for each server
    server_statuses = {}
    for server_name, server_conf in sqlservers.items():
        success, error = test_sql_connection(server_conf)
        server_statuses[server_name] = {
            "online": success,
            "error": error
        }
    
    # Load data_sources from Postgres so Add Source entries appear on home page
    data_sources = []
    data_source_statuses = {}
    app.logger.debug("Starting to load data_sources...")
    try:
        from db_utils import load_pg_config
        pg_conf = load_pg_config()
        app.logger.debug(f"Pg config: db={pg_conf.get('database')}, host={pg_conf.get('host')}")
        conn = psycopg2.connect(
            dbname=pg_conf.get('database', 'metrics_sync_tables'),
            user=pg_conf.get('username'),
            password=pg_conf.get('password'),
            host=pg_conf.get('host'),
            port=int(pg_conf.get('port', 5432))
        )
        app.logger.debug("Connected to PostgreSQL")
        cur = conn.cursor()
        cur.execute("""
            SELECT id, source_name, source_type, server_address, username, target_type, target_database, connection_details,
                   oauth_refresh_token, oauth_client_id, oauth_client_secret,
                   oauth_access_token, oauth_token_expiry, oauth_api_domain
            FROM data_sources WHERE is_active = true ORDER BY created_at DESC
        """)
        rows = cur.fetchall()
        app.logger.info(f"[DEBUG] Query returned {len(rows)} data_source rows")
        app.logger.debug(f"Query returned {len(rows)} rows")
        for r in rows:
            ds = {
                'id': r[0],
                'source_name': r[1],
                'source_type': r[2],
                'server_address': r[3],
                'username': r[4],
                'target_type': r[5],
                'target_database': r[6],
                'connection_details': r[7],
                'oauth_refresh_token': r[8] if len(r) > 8 else None,
                'oauth_client_id': r[9] if len(r) > 9 else None,
                'oauth_client_secret': r[10] if len(r) > 10 else None,
                'oauth_access_token': r[11] if len(r) > 11 else None,
                'oauth_token_expiry': r[12] if len(r) > 12 else None,
                'oauth_api_domain': r[13] if len(r) > 13 else None
            }
            data_sources.append(ds)
            app.logger.info(f"[DEBUG] Loaded data_source: {ds['source_name']} (ID: {ds['id']})")
            app.logger.debug(f"Loaded source: {ds['source_name']}")
        cur.close()
        conn.close()
    except Exception as e:
        app.logger.exception(f"Could not load data_sources: {e}")
        app.logger.exception(f"ERROR loading data_sources: {e}")
        import traceback
        traceback.print_exc()

    # Determine status for each data source
    for ds in data_sources:
        try:
            if ds['source_type'] == 'sap_hana':
                # Test HANA connection
                try:
                    import hdbcli.dbapi as hana_dbapi
                    connection_details = json.loads(ds['connection_details']) if isinstance(ds['connection_details'], str) else (ds['connection_details'] or {})
                    
                    # Parse server address
                    if ':' in ds['server_address']:
                        host, port = ds['server_address'].split(':', 1)
                    else:
                        host = ds['server_address']
                        port = connection_details.get('port')
                    
                    if not port:
                        # Try to load from .env
                        try:
                            from db_utils import load_hana_config
                            hana_base = load_hana_config()
                            port = str(hana_base['port'])
                        except ValueError:
                            port = None
                    
                    # Try to connect
                    conn = hana_dbapi.connect(
                        address=host,
                        port=int(port),
                        user=ds['username'],
                        password=ds['password'],
                        encrypt=True,
                        sslValidateCertificate=False,
                        timeout=5
                    )
                    conn.close()
                    data_source_statuses[ds['id']] = {'online': True, 'error': None}
                except ImportError:
                    # hdbcli not installed - can't test connection
                    data_source_statuses[ds['id']] = {'online': None, 'error': 'hdbcli library not installed'}
                except Exception as e:
                    data_source_statuses[ds['id']] = {'online': False, 'error': str(e)}
            elif ds['source_type'] == 'sql_server':
                # Build a minimal server_conf similar to YAML config
                server_conf = {
                    'server': ds['server_address'],
                    'username': ds.get('username'),
                    'password': None,  # don't expose password here
                }
                # We can't test without password; mark as unknown unless password present in DB
                # Attempt to fetch password from DB for testing (internal only)
                try:
                    from db_utils import load_pg_config
                    pg_conf = load_pg_config()
                    conn = psycopg2.connect(
                        dbname=pg_conf.get('database', 'metrics_sync_tables'),
                        user=pg_conf.get('username'),
                        password=pg_conf.get('password'),
                        host=pg_conf.get('host'),
                        port=int(pg_conf.get('port', 5432))
                    )
                    cur = conn.cursor()
                    cur.execute("SELECT password FROM data_sources WHERE id = %s", (ds['id'],))
                    pw_row = cur.fetchone()
                    cur.close()
                    conn.close()
                    if pw_row and pw_row[0]:
                        server_conf['password'] = pw_row[0]
                except Exception:
                    pass

                if server_conf.get('password'):
                    success, error = test_sql_connection(server_conf)
                    data_source_statuses[ds['id']] = {'online': success, 'error': error}
                else:
                    data_source_statuses[ds['id']] = {'online': False, 'error': 'Password not available for connection test'}
            elif ds['source_type'] == 'api' or ds['source_type'] == 'rest_api':
                # Test API connection
                try:
                    import requests
                    api_url = ds['server_address']
                    connection_details = ds.get('connection_details') or {}
                    
                    # Check if it's an SSE stream
                    is_sse = connection_details.get('is_sse', False)
                    
                    if is_sse:
                        # For SSE, just mark as "streaming" without full test
                        data_source_statuses[ds['id']] = {'online': True, 'error': None, 'status': 'Streaming'}
                    else:
                        headers = {}
                        auth_type = connection_details.get('auth_type', 'none')
                        
                        # Handle Zoho OAuth authentication
                        if auth_type == 'zoho_oauth':
                            zoho_refresh_token = ds.get('oauth_refresh_token') or connection_details.get('zoho_refresh_token')
                            zoho_client_id = ds.get('oauth_client_id') or connection_details.get('zoho_client_id')
                            zoho_client_secret = ds.get('oauth_client_secret') or connection_details.get('zoho_client_secret')
                            stored_access_token = ds.get('oauth_access_token')
                            stored_token_expiry = ds.get('oauth_token_expiry')
                            stored_api_domain = ds.get('oauth_api_domain')
                            
                            if zoho_refresh_token and zoho_client_id and zoho_client_secret:
                                try:
                                    from zoho_oauth_manager import ZohoOAuthManager
                                    token_result = ZohoOAuthManager.get_valid_token(
                                        source_id=ds['id'],
                                        refresh_token=zoho_refresh_token,
                                        client_id=zoho_client_id,
                                        client_secret=zoho_client_secret,
                                        stored_access_token=stored_access_token,
                                        stored_expiry=stored_token_expiry,
                                        stored_api_domain=stored_api_domain
                                    )
                                    
                                    if token_result:
                                        # Use correct API domain
                                        if stored_api_domain and not api_url.startswith(stored_api_domain):
                                            for domain in ['https://www.zohoapis.com', 'https://www.zohoapis.eu', 'https://www.zohoapis.in']:
                                                if api_url.startswith(domain):
                                                    api_url = api_url.replace(domain, stored_api_domain)
                                                    break
                                        
                                        headers['Authorization'] = f"{token_result.get('token_type', 'Bearer')} {token_result['access_token']}"
                                    else:
                                        data_source_statuses[ds['id']] = {'online': False, 'error': 'Failed to obtain Zoho access token'}
                                        continue
                                except Exception as oauth_error:
                                    app.logger.exception(f"Error checking Zoho OAuth status: {oauth_error}")
                                    data_source_statuses[ds['id']] = {'online': False, 'error': f'OAuth error: {str(oauth_error)}'}
                                    continue
                            else:
                                data_source_statuses[ds['id']] = {'online': False, 'error': 'Zoho OAuth credentials missing'}
                                continue
                        # Handle OAuth authentication
                        elif auth_type == 'oauth':
                            oauth_token_url = connection_details.get('oauth_token_url', '')
                            oauth_username = connection_details.get('oauth_username', '')
                            oauth_password = connection_details.get('oauth_password', '')
                            
                            if oauth_token_url and oauth_username and oauth_password:
                                try:
                                    app.logger.info(f"Status check OAuth: Requesting token from {oauth_token_url}")
                                    token_response = requests.post(
                                        oauth_token_url,
                                        json={"username": oauth_username, "password": oauth_password},
                                        timeout=10
                                    )
                                    
                                    if token_response.status_code == 200:
                                        token_data = token_response.json()
                                        oauth_token = token_data.get('token') or token_data.get('access_token') or token_data.get('oauth_token')
                                        
                                        if not oauth_token:
                                            # Try to find any string field that looks like a token
                                            for key, value in token_data.items():
                                                if isinstance(value, str) and len(value) > 20:
                                                    oauth_token = value
                                                    break
                                        
                                        if oauth_token:
                                            headers["Authorization"] = f"Bearer {oauth_token}"
                                            app.logger.info(f"Status check: OAuth token obtained successfully")
                                        else:
                                            data_source_statuses[ds['id']] = {'online': False, 'error': 'No token found in OAuth response'}
                                            continue
                                    else:
                                        data_source_statuses[ds['id']] = {'online': False, 'error': f'OAuth token request failed: HTTP {token_response.status_code}'}
                                        continue
                                except Exception as e:
                                    app.logger.exception(f"OAuth token request failed in status check: {e}")
                                    data_source_statuses[ds['id']] = {'online': False, 'error': f'OAuth token request failed: {str(e)}'}
                                    continue
                            else:
                                data_source_statuses[ds['id']] = {'online': False, 'error': 'OAuth credentials missing'}
                                continue
                        elif auth_type == 'bearer':
                            token = connection_details.get('auth_token', '')
                            headers['Authorization'] = f'Bearer {token}'
                        elif auth_type == 'apikey':
                            key_name = connection_details.get('apikey_header', 'X-API-Key')
                            key_value = connection_details.get('auth_token', '')
                            headers[key_name] = key_value
                        
                        # Make the status check request
                        response = requests.get(api_url, headers=headers, timeout=5)
                        if response.status_code == 200:
                            data_source_statuses[ds['id']] = {'online': True, 'error': None}
                        else:
                            data_source_statuses[ds['id']] = {'online': False, 'error': f'HTTP {response.status_code}'}
                except Exception as e:
                    data_source_statuses[ds['id']] = {'online': False, 'error': str(e)}
            else:
                data_source_statuses[ds['id']] = {'online': False, 'error': 'Unknown source type'}
        except Exception as e:
            app.logger.exception(f"Error checking status for source {ds.get('source_name')}: {e}")
            data_source_statuses[ds['id']] = {'online': False, 'error': str(e)}

    role = session.get("role")
    app.logger.info(f"[INFO] Loaded {len(sqlservers)} SQL servers and {len(data_sources)} data sources for display")
    app.logger.debug(f"About to render: sqlservers={len(sqlservers)}, data_sources={len(data_sources)}")
    app.logger.debug(f"data_sources content: {[ds.get('source_name') for ds in data_sources]}")
    return render_template("sync_servers.html", sqlservers=sqlservers, server_statuses=server_statuses, data_sources=data_sources, data_source_statuses=data_source_statuses, role=role)

