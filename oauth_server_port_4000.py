"""
OAuth API Server on Port 4000
Matches your form configuration exactly
"""
from flask import Flask, request, jsonify
import jwt
import datetime
import random
import string

app = Flask(__name__)
SECRET_KEY = "your-secret-key-change-this-in-production"

# Data store
data_store = []
record_id = 0

def generate_record():
    """Generate sample data"""
    global record_id
    record_id += 1
    return {
        "id": record_id,
        "name": f"Item_{record_id}",
        "value": random.randint(100, 1000),
        "category": random.choice(["A", "B", "C"]),
        "timestamp": datetime.datetime.now().isoformat()
    }

# Initialize with 10 records
for _ in range(10):
    data_store.append(generate_record())

@app.route('/api/pass', methods=['POST'])
def generate_token():
    """Generate OAuth token"""
    try:
        data = request.json or {}
        username = data.get('username', '')
        password = data.get('password', '')
        
        print(f"[TOKEN REQUEST] User: {username}")
        
        # Validate credentials
        if username == 'saad' and password == 'saad':
            # Generate JWT token
            token = jwt.encode(
                {
                    'username': username,
                    'iat': datetime.datetime.utcnow(),
                    'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=1)
                },
                SECRET_KEY,
                algorithm='HS256'
            )
            
            print(f"[TOKEN GENERATED] {token[:30]}...")
            
            return jsonify({
                'success': True,
                'message': 'Authentication successful',
                'token': token,
                'expiresIn': '1h',
                'tokenType': 'Bearer'
            }), 200
        else:
            print(f"[TOKEN FAILED] Invalid credentials")
            return jsonify({
                'success': False,
                'error': 'Authentication failed',
                'message': 'Invalid username or password'
            }), 401
            
    except Exception as e:
        print(f"[ERROR] {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/data', methods=['GET'])
def get_data():
    """Get data (requires OAuth token)"""
    auth_header = request.headers.get('Authorization', '')
    
    if not auth_header.startswith('Bearer '):
        print(f"[DATA] DENIED - No Bearer token")
        return jsonify({
            'error': 'Unauthorized',
            'message': 'Authorization header required'
        }), 401
    
    token = auth_header.replace('Bearer ', '')
    
    # Verify token
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        print(f"[DATA] GRANTED - User: {payload['username']} - Returning {len(data_store)} records")
        
        # Add 2 new records occasionally
        if random.random() > 0.5:
            for _ in range(2):
                data_store.append(generate_record())
            print(f"[DATA] Added 2 new records. Total: {len(data_store)}")
        
        return jsonify({
            'success': True,
            'count': len(data_store),
            'timestamp': datetime.datetime.now().isoformat(),
            'data': data_store
        }), 200
        
    except jwt.ExpiredSignatureError:
        print(f"[DATA] DENIED - Token expired")
        return jsonify({
            'error': 'Token expired',
            'message': 'Please get a new token'
        }), 401
    except jwt.InvalidTokenError:
        print(f"[DATA] DENIED - Invalid token")
        return jsonify({
            'error': 'Invalid token'
        }), 401

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Public stats (no auth)"""
    return jsonify({
        'total_records': len(data_store),
        'server_time': datetime.datetime.now().isoformat()
    })

if __name__ == '__main__':
    print("="*80)
    print("OAUTH API SERVER - PORT 4000")
    print("="*80)
    print("\n🔑 OAuth Endpoints:")
    print("   POST http://localhost:4000/api/pass")
    print("   Body: {\"username\": \"saad\", \"password\": \"saad\"}")
    print("   Returns: {\"token\": \"...\"}")
    print("\n📊 Data Endpoint:")
    print("   GET http://localhost:4000/api/data")
    print("   Headers: {\"Authorization\": \"Bearer <token>\"}")
    print("   Returns: {\"data\": [...]}")
    print("\n📈 Stats (Public):")
    print("   GET http://localhost:4000/api/stats")
    print("\n🔐 Test Credentials:")
    print("   Username: saad")
    print("   Password: saad")
    print("\n📊 Starting with {} records, adds more randomly".format(len(data_store)))
    print("\n" + "="*80)
    print("Server starting on http://localhost:4000")
    print("="*80 + "\n")
    
    app.run(host='0.0.0.0', port=4000, debug=False)

