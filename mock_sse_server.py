"""
Mock SSE Server for Testing API Sync
Simulates the CRM API at http://localhost:3000/api/crm/stream
"""
from flask import Flask, Response
import json
import time
import random
from datetime import datetime, timedelta

app = Flask(__name__)

# Sample deal data
ACCOUNTS = ["Account A", "Account B", "Account C", "Account D", "Account E"]
STAGES = ["Prospecting", "Needs Analysis", "Proposal", "Negotiation", "Closed Won", "Closed Lost"]
DEAL_TYPES = ["New Business", "Existing Business", "Renewal"]

def generate_deal():
    """Generate a random deal record"""
    deal_id = f"deal-{random.randint(1000, 9999)}"
    return {
        "type": "new_record",
        "record_type": "deal",
        "data": {
            "id": deal_id,
            "deal_name": f"Deal {random.randint(1, 100)} - Q4",
            "amount": random.randint(10000, 150000),
            "stage": random.choice(STAGES),
            "type": random.choice(DEAL_TYPES),
            "closing_date": (datetime.now() + timedelta(days=random.randint(1, 90))).strftime("%Y-%m-%d"),
            "account_name": random.choice(ACCOUNTS),
            "created_time": (datetime.now() - timedelta(days=random.randint(1, 30))).isoformat() + "Z",
            "modified_time": datetime.now().isoformat() + "Z"
        },
        "timestamp": datetime.now().isoformat() + "Z"
    }

@app.route('/api/crm/stream')
def sse_stream():
    """SSE endpoint that streams deal data - supports MULTIPLE concurrent connections"""
    def event_stream():
        client_id = random.randint(1000, 9999)
        print(f"🎉 Client #{client_id} connected to SSE stream!")
        try:
            # Send initial deals
            for i in range(3):
                deal = generate_deal()
                yield f"data: {json.dumps(deal)}\n\n"
                print(f"📤 [Client #{client_id}] Sent deal #{i+1}: {deal['data']['deal_name']}")
                time.sleep(1)
            
            # Continue sending deals every 3 seconds
            counter = 4
            while True:
                time.sleep(3)
                deal = generate_deal()
                yield f"data: {json.dumps(deal)}\n\n"
                print(f"📤 [Client #{client_id}] Sent deal #{counter}: {deal['data']['deal_name']}")
                counter += 1
                
        except GeneratorExit:
            print(f"👋 Client #{client_id} disconnected from SSE stream")
    
    return Response(event_stream(), mimetype='text/event-stream')

@app.route('/api/crm/deals')
def get_deals():
    """REST endpoint that returns a list of deals"""
    deals = [generate_deal()["data"] for _ in range(10)]
    return {"success": True, "data": deals, "count": len(deals)}

@app.route('/')
def index():
    return """
    <h1>Mock CRM API Server</h1>
    <p>Endpoints available:</p>
    <ul>
        <li><a href="/api/crm/stream">/api/crm/stream</a> - SSE streaming endpoint</li>
        <li><a href="/api/crm/deals">/api/crm/deals</a> - REST endpoint (JSON)</li>
    </ul>
    """

if __name__ == '__main__':
    print("="*60)
    print("🚀 Starting Mock CRM API Server")
    print("="*60)
    print("SSE Stream: http://localhost:3000/api/crm/stream")
    print("REST API:   http://localhost:3000/api/crm/deals")
    print("="*60)
    print()
    app.run(host='0.0.0.0', port=3000, debug=False, threaded=True)
