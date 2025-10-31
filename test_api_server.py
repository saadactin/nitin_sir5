"""
Test API server that generates dynamic data
Adds 5 new records every 5 seconds with different data types and structures
"""
from flask import Flask, jsonify
import random
import string
from datetime import datetime, timedelta
import threading
import time

app = Flask(__name__)

# Global data store
data_store = []
record_counter = 0

def generate_record():
    """Generate a record with various data types and structures"""
    global record_counter
    record_counter += 1
    
    # Random values
    random_string = ''.join(random.choices(string.ascii_letters, k=10))
    random_int = random.randint(1, 10000)
    random_float = round(random.uniform(0, 1000), 2)
    random_bool = random.choice([True, False])
    random_null = None if random.random() > 0.7 else f"value_{random.randint(1, 100)}"
    
    record = {
        "id": f"rec_{record_counter}",
        "name": f"Record {random_string}",
        "value": random_int,
        "price": random_float,
        "active": random_bool,
        "optional_field": random_null,
        "timestamp": datetime.now().isoformat(),
        "category": random.choice(["A", "B", "C", "D"]),
        # Nested object
        "metadata": {
            "created_by": f"user_{random.randint(1, 100)}",
            "tags": random.choice([["tag1", "tag2"], ["tag3"], []]),
            "score": random_float
        },
        # Nested location
        "location": {
            "city": random.choice(["NYC", "LA", "Chicago", "Houston"]),
            "coordinates": {
                "lat": round(random.uniform(-90, 90), 6),
                "lng": round(random.uniform(-180, 180), 6)
            }
        },
        # Various number types
        "stats": {
            "views": random.randint(0, 1000000),
            "likes": random.randint(0, 10000),
            "shares": random.randint(0, 1000),
            "rating": round(random.uniform(0, 5), 1)
        }
    }
    
    return record

def background_data_generator():
    """Generate 5 new records every 5 seconds"""
    while True:
        time.sleep(5)
        for _ in range(5):
            data_store.append(generate_record())
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Added 5 records. Total: {len(data_store)}")

@app.route('/api/data')
def get_data():
    """Return all data"""
    return jsonify({
        "status": "success",
        "count": len(data_store),
        "timestamp": datetime.now().isoformat(),
        "data": data_store
    })

@app.route('/api/data/simple')
def get_data_simple():
    """Return data as direct array (no wrapper)"""
    return jsonify(data_store)

@app.route('/api/stats')
def get_stats():
    """Return statistics"""
    return jsonify({
        "total_records": len(data_store),
        "latest_id": data_store[-1]["id"] if data_store else None,
        "server_time": datetime.now().isoformat()
    })

if __name__ == '__main__':
    # Generate initial 10 records
    print("Generating initial data...")
    for _ in range(10):
        data_store.append(generate_record())
    
    print(f"Starting server with {len(data_store)} initial records")
    print("Will add 5 new records every 5 seconds")
    print("\nAvailable endpoints:")
    print("  http://localhost:5555/api/data (wrapped in 'data' key)")
    print("  http://localhost:5555/api/data/simple (direct array)")
    print("  http://localhost:5555/api/stats (statistics)")
    
    # Start background generator
    generator_thread = threading.Thread(target=background_data_generator, daemon=True)
    generator_thread.start()
    
    # Run Flask server
    app.run(host='0.0.0.0', port=5555, debug=False)

