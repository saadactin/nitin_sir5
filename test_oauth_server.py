"""
Test OAuth API Server
Simulates a real OAuth-protected API with automatic token refresh
"""
from flask import Flask, request, jsonify
import uuid
from datetime import datetime
import random
import string

app = Flask(__name__)

# Store valid tokens
valid_tokens = {}
data_counter = 0

def generate_data_record():
    """Generate a sample data record"""
    global data_counter
    data_counter += 1
    
    return {
        'id': data_counter,
        'name': f"Item_{data_counter}",
        'value': random.randint(100, 1000),
        'price': round(random.uniform(10, 500), 2),
        'category': random.choice(['A', 'B', 'C', 'D']),
        'active': random.choice([True, False]),
        'timestamp': datetime.now().isoformat(),
        'description': ''.join(random.choices(string.ascii_letters, k=20))
    }

# Generate initial data
api_data = [generate_data_record() for _ in range(10)]

def add_new_records():
    """Add 5 new records (called periodically)"""
    global api_data
    for _ in range(5):
        api_data.append(generate_data_record())

# Background thread to add data
import threading
import time

def background_data_adder():
    while True:
        time.sleep(5)
        add_new_records()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Added 5 records. Total: {len(api_data)}")

# Start background thread
thread = threading.Thread(target=background_data_adder, daemon=True)
thread.start()

@app.route('/pass', methods=['POST'])
def get_token():
    """
    OAuth token endpoint
    POST with JSON: {"username": "admin", "password": "secret"}
    Returns: {"token": "...", "expires_in": 3600}
    """
    try:
        data = request.json or {}
        username = data.get('username', '')
        password = data.get('password', '')
        
        print(f"[TOKEN REQUEST] Username: {username}, Password: {'*' * len(password)}")
        
        # Validate credentials
        if username == 'admin' and password == 'secret':
            # Generate new token
            token = str(uuid.uuid4())
            valid_tokens[token] = datetime.now()
            
            print(f"[TOKEN GENERATED] {token[:8]}... (expires in 3600s)")
            
            return jsonify({
                'token': token,
                'expires_in': 3600,
                'token_type': 'Bearer'
            }), 200
        else:
            print(f"[TOKEN FAILED] Invalid credentials")
            return jsonify({
                'error': 'Invalid credentials',
                'message': 'Username or password is incorrect'
            }), 401
            
    except Exception as e:
        print(f"[TOKEN ERROR] {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/data', methods=['GET'])
def get_data():
    """
    Protected data endpoint
    Requires: Authorization: Bearer <token>
    Returns: {"data": [...]}
    """
    # Check Authorization header
    auth_header = request.headers.get('Authorization', '')
    
    if not auth_header.startswith('Bearer '):
        print(f"[DATA REQUEST] DENIED - No Bearer token")
        return jsonify({
            'error': 'Unauthorized',
            'message': 'Authorization header with Bearer token required'
        }), 401
    
    # Extract token
    token = auth_header.replace('Bearer ', '')
    
    # Validate token
    if token not in valid_tokens:
        print(f"[DATA REQUEST] DENIED - Invalid token: {token[:8]}...")
        return jsonify({
            'error': 'Unauthorized',
            'message': 'Invalid or expired token'
        }), 401
    
    # Return data
    print(f"[DATA REQUEST] GRANTED - Token: {token[:8]}... - Returning {len(api_data)} records")
    
    return jsonify({
        'status': 'success',
        'count': len(api_data),
        'timestamp': datetime.now().isoformat(),
        'data': api_data
    }), 200

@app.route('/stats', methods=['GET'])
def get_stats():
    """Public stats endpoint (no auth required)"""
    return jsonify({
        'total_records': len(api_data),
        'latest_id': api_data[-1]['id'] if api_data else None,
        'active_tokens': len(valid_tokens),
        'server_time': datetime.now().isoformat()
    })

if __name__ == '__main__':
    print("=" * 80)
    print("OAUTH TEST API SERVER")
    print("=" * 80)
    print("\n📡 Endpoints:")
    print("  POST http://localhost:4010/pass")
    print("       Body: {\"username\": \"admin\", \"password\": \"secret\"}")
    print("       Returns: {\"token\": \"...\"}")
    print("\n  GET  http://localhost:4010/data")
    print("       Headers: {\"Authorization\": \"Bearer <token>\"}")
    print("       Returns: {\"data\": [...]}")
    print("\n  GET  http://localhost:4010/stats (no auth)")
    print("       Returns: {\"total_records\": ...}")
    print("\n🔑 Test Credentials:")
    print("   Username: admin")
    print("   Password: secret")
    print("\n📊 Data:")
    print(f"   Starting with {len(api_data)} records")
    print("   Adding 5 new records every 5 seconds")
    print("\n" + "=" * 80)
    print("Starting server on http://localhost:4010")
    print("=" * 80 + "\n")
    
    app.run(host='0.0.0.0', port=4010, debug=False)

