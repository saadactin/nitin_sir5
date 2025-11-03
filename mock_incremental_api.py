"""
Mock Zoho CRM Leads API Server
Simulates Zoho CRM Leads API that adds new leads every 5 seconds (5 leads per poll)
Replicates exact Zoho CRM structure and behavior with resilience features
"""
from flask import Flask, jsonify, request, make_response
import time
import threading
import uuid
from datetime import datetime, timedelta
import random
import logging
from functools import wraps

app = Flask(__name__)
app.logger.setLevel(logging.INFO)

# In-memory data store (simulates Zoho CRM database)
leads = []
leads_lock = threading.Lock()
lead_counter = 0

# Configuration
LEADS_PER_INTERVAL = 5  # Number of leads to add every interval
ADD_INTERVAL = 5  # Seconds between additions
MAX_LEADS = 10000  # Maximum leads before auto-reset (optional)

# Resilience settings
RATE_LIMIT_ENABLED = False  # Set to True to test rate limiting
RATE_LIMIT_RESET_TIME = 3600  # Rate limit resets after 1 hour
ERROR_PROBABILITY = 0.0  # 0.0 = no errors, 0.1 = 10% chance of error
MAINTENANCE_MODE = False  # Simulate maintenance downtime

# Statistics
stats = {
    "total_requests": 0,
    "successful_requests": 0,
    "failed_requests": 0,
    "rate_limited": 0,
    "errors_500": 0,
    "errors_503": 0,
    "start_time": datetime.now()
}

# Zoho-like company and name data
COMPANIES = [
    "Qpack-Quality Packaging Solution", "JSW Greentech Limited", "GARG AYURVEDA",
    "GAIL Limited", "Salvo atta company", "Suraksha World",
    "Infinity automation solutions", "Rekhashri Entertainment", "Apollo Building Products",
    "TechCorp India", "DataSys Solutions", "CloudInc Services", "DevOps Co",
    "AI Solutions Pvt Ltd", "CloudTech Innovations", "DataFlow Systems"
]

FIRST_NAMES = ["Vishal", "Anil", "Kamalesh", "Avinash", "D N", "Ramesh", "RAMAN", "ANIL",
               "Prasad", "Kamaljeet", "Avinaba", "Kannan", "Sadananda", "Manjesh",
               "John", "Jane", "Bob", "Alice", "Charlie", "Diana"]

LAST_NAMES = ["Agrawal", "Nehare", "Kumar Pandey", "Arya Singh", "Sangamesh", "babu b",
              "KUMAR TIWARI", "N", "Kumanache", "Singh Rajput", "Dhar", "Rajendran",
              "Smith", "Johnson", "Williams", "Brown", "Jones"]

LEAD_STATUSES = [
    "Sales Qualified Lead", "Contacted and Responding", "Potential Dealer",
    "Not Contacted", "Contacted", "Not Qualified", "No Response", "Junk Lead",
    "Opportunity", "Not Yet Working"
]

def generate_zoho_lead():
    """Generate a Zoho CRM-style lead record"""
    global lead_counter
    lead_counter += 1
    
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)
    company = random.choice(COMPANIES)
    status = random.choice(LEAD_STATUSES)
    
    # Generate realistic email
    email_domains = ["example.com", "test.com", "company.in", "business.com", "corp.in"]
    email = f"{first_name.lower()}.{last_name.lower().replace(' ', '')}@{random.choice(email_domains)}"
    
    # Generate phone (sometimes null)
    phone = None
    if random.random() > 0.3:  # 70% have phone
        phone = f"{random.randint(9000000000, 9999999999)}"
    
    # Zoho CRM ID format: 23863200000XXXXXX
    zoho_id = f"23863200000{lead_counter:06d}"
    
    return {
        "Company": company,
        "Email": email,
        "Full_Name": f"{first_name} {last_name}",
        "Lead_Status": status,
        "Phone": phone,
        "id": zoho_id
    }

def resilience_check(func):
    """Decorator to add resilience features (rate limiting, errors, etc.)"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        stats["total_requests"] += 1
        
        # Maintenance mode check
        if MAINTENANCE_MODE:
            stats["failed_requests"] += 1
            return make_response(jsonify({
                "code": "MAINTENANCE_MODE",
                "message": "Service is under maintenance. Please try again later.",
                "status": "error"
            }), 503)
        
        # Rate limiting check
        if RATE_LIMIT_ENABLED and stats.get("rate_limit_active", False):
            stats["rate_limited"] += 1
            return make_response(jsonify({
                "code": "RATE_LIMIT_EXCEEDED",
                "message": "API rate limit exceeded. Please try again later.",
                "status": "error"
            }), 429)
        
        # Random error injection (for testing error handling)
        if random.random() < ERROR_PROBABILITY:
            error_type = random.choice(["500", "503"])
            stats[f"errors_{error_type}"] += 1
            stats["failed_requests"] += 1
            return make_response(jsonify({
                "code": "INTERNAL_ERROR",
                "message": "An unexpected error occurred. Please try again.",
                "status": "error"
            }), int(error_type))
        
        try:
            response = func(*args, **kwargs)
            stats["successful_requests"] += 1
            return response
        except Exception as e:
            stats["failed_requests"] += 1
            app.logger.error(f"Error in {func.__name__}: {e}")
            return make_response(jsonify({
                "code": "INTERNAL_ERROR",
                "message": str(e),
                "status": "error"
            }), 500)
    
    return wrapper

def auto_add_leads():
    """Background thread that automatically adds leads (Zoho CRM style)"""
    global leads
    while True:
        try:
            time.sleep(ADD_INTERVAL)
            
            # Skip if in maintenance mode
            if MAINTENANCE_MODE:
                continue
            
            new_leads = [generate_zoho_lead() for _ in range(LEADS_PER_INTERVAL)]
            
            with leads_lock:
                leads.extend(new_leads)
                # Optional: Auto-reset if too many leads (for testing)
                if MAX_LEADS > 0 and len(leads) > MAX_LEADS:
                    app.logger.info(f"[MOCK_API] Reached max leads ({MAX_LEADS}), resetting...")
                    leads = leads[-100:]  # Keep last 100
                    global lead_counter
                    lead_counter = 100
            
            app.logger.info(f"[MOCK_API] Added {len(new_leads)} leads. Total: {len(leads)}")
        except Exception as e:
            app.logger.error(f"[MOCK_API] Error in auto_add_leads: {e}")
            time.sleep(ADD_INTERVAL)

@app.route('/crm/v8/Leads', methods=['GET'])
@resilience_check
def get_leads():
    """Get all leads - Zoho CRM style endpoint"""
    global leads
    
    # Support Zoho query parameters
    fields_param = request.args.get('fields', '')
    per_page = min(int(request.args.get('per_page', 200)), 200)  # Zoho max is 200
    page = int(request.args.get('page', 1))
    
    with leads_lock:
        total_leads = len(leads)
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        page_leads = leads[start_idx:end_idx]
        
        # Filter fields if specified
        if fields_param:
            fields_list = [f.strip() for f in fields_param.split(',')]
            filtered_leads = []
            for lead in page_leads:
                filtered_lead = {k: v for k, v in lead.items() if k in fields_list or k == 'id'}
                filtered_leads.append(filtered_lead)
            page_leads = filtered_leads
        
        # Zoho-style response
        response_data = {
            "data": page_leads,
            "info": {
                "count": len(page_leads),
                "page": page,
                "per_page": per_page,
                "more_records": end_idx < total_leads,
                "sort_by": "id",
                "sort_order": "desc"
            }
        }
        
        # Add next_page_token if more records
        if end_idx < total_leads:
            response_data["info"]["next_page_token"] = f"page_{page + 1}_{total_leads}"
        
        return jsonify(response_data)

@app.route('/api/data', methods=['GET'])
@resilience_check
def get_data():
    """Get all leads (alternative endpoint for compatibility)"""
    return get_leads()

@app.route('/api/data/latest', methods=['GET'])
@resilience_check
def get_latest():
    """Get only the latest N leads (simulates incremental fetch)"""
    global leads
    
    limit = min(int(request.args.get('limit', default=100, type=int)), 200)
    
    with leads_lock:
        latest_leads = leads[-limit:] if len(leads) > limit else leads.copy()
        
        return jsonify({
            "data": latest_leads,
            "count": len(latest_leads),
            "total": len(leads),
            "info": {
                "total_records": len(leads),
                "returned_records": len(latest_leads),
                "page": 1
            }
        })

@app.route('/api/data/stats', methods=['GET'])
@resilience_check
def get_stats():
    """Get statistics about the leads and API"""
    global leads, stats
    
    with leads_lock:
        uptime = (datetime.now() - stats["start_time"]).total_seconds()
        
        return jsonify({
            "total_records": len(leads),
            "leads_per_interval": LEADS_PER_INTERVAL,
            "add_interval_seconds": ADD_INTERVAL,
            "last_lead_id": leads[-1]["id"] if leads else None,
            "api_stats": {
                "total_requests": stats["total_requests"],
                "successful_requests": stats["successful_requests"],
                "failed_requests": stats["failed_requests"],
                "rate_limited": stats["rate_limited"],
                "errors_500": stats["errors_500"],
                "errors_503": stats["errors_503"],
                "uptime_seconds": int(uptime),
                "success_rate": round(stats["successful_requests"] / max(stats["total_requests"], 1) * 100, 2)
            },
            "timestamp": datetime.now().isoformat()
        })

@app.route('/api/data/reset', methods=['POST'])
@resilience_check
def reset_data():
    """Reset all leads (for testing)"""
    global leads, lead_counter, stats
    
    with leads_lock:
        leads.clear()
        lead_counter = 0
        stats = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "rate_limited": 0,
            "errors_500": 0,
            "errors_503": 0,
            "start_time": datetime.now()
        }
    
    return jsonify({
        "success": True,
        "message": "All leads reset successfully",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/data/add', methods=['POST'])
@resilience_check
def add_manual():
    """Manually add leads (for testing)"""
    global leads
    
    count = min(int(request.json.get('count', 1)) if request.is_json else 1, 100)  # Max 100
    
    new_leads = [generate_zoho_lead() for _ in range(count)]
    
    with leads_lock:
        leads.extend(new_leads)
    
    return jsonify({
        "success": True,
        "added": len(new_leads),
        "total_records": len(leads),
        "new_leads": new_leads
    })

@app.route('/api/health', methods=['GET'])
def health():
    """Health check endpoint"""
    global leads, MAINTENANCE_MODE
    
    return jsonify({
        "status": "maintenance" if MAINTENANCE_MODE else "healthy",
        "timestamp": datetime.now().isoformat(),
        "leads_count": len(leads),
        "maintenance_mode": MAINTENANCE_MODE
    })

@app.route('/api/maintenance', methods=['POST'])
def toggle_maintenance():
    """Toggle maintenance mode (for testing resilience)"""
    global MAINTENANCE_MODE
    
    MAINTENANCE_MODE = not MAINTENANCE_MODE
    
    return jsonify({
        "maintenance_mode": MAINTENANCE_MODE,
        "message": "Maintenance mode " + ("enabled" if MAINTENANCE_MODE else "disabled")
    })

@app.route('/api/simulate-error', methods=['POST'])
def simulate_error():
    """Simulate various error conditions (for testing)"""
    global ERROR_PROBABILITY, RATE_LIMIT_ENABLED
    
    data = request.json or {}
    error_type = data.get('type', 'none')
    value = data.get('value', 0.0)
    
    if error_type == 'rate_limit':
        RATE_LIMIT_ENABLED = bool(value)
        stats["rate_limit_active"] = bool(value)
    elif error_type == 'random_error':
        ERROR_PROBABILITY = float(value)
    elif error_type == 'maintenance':
        global MAINTENANCE_MODE
        MAINTENANCE_MODE = bool(value)
    
    return jsonify({
        "success": True,
        "error_type": error_type,
        "value": value,
        "message": f"Error simulation {error_type} set to {value}"
    })

@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return jsonify({
        "code": "NOT_FOUND",
        "message": "The requested resource was not found",
        "status": "error"
    }), 404

@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors"""
    return jsonify({
        "code": "INTERNAL_ERROR",
        "message": "An internal server error occurred",
        "status": "error"
    }), 500

if __name__ == '__main__':
    # Start background thread to auto-add leads
    auto_add_thread = threading.Thread(target=auto_add_leads, daemon=True)
    auto_add_thread.start()
    
    print("=" * 70)
    print("MOCK ZOHO CRM LEADS API SERVER")
    print("=" * 70)
    print(f"Server will add {LEADS_PER_INTERVAL} leads every {ADD_INTERVAL} seconds")
    print("\nMain Endpoints (Zoho CRM style):")
    print("  GET  /crm/v8/Leads              - Get all leads (Zoho format)")
    print("  GET  /crm/v8/Leads?fields=...    - Get leads with field filtering")
    print("  GET  /crm/v8/Leads?per_page=200 - Pagination support")
    print("\nAdditional Endpoints:")
    print("  GET  /api/data                   - Get all leads (compatible)")
    print("  GET  /api/data/stats             - Get statistics")
    print("  GET  /api/health                 - Health check")
    print("  POST /api/data/reset             - Reset all leads")
    print("  POST /api/data/add               - Manually add leads")
    print("  POST /api/maintenance            - Toggle maintenance mode")
    print("  POST /api/simulate-error         - Simulate errors (testing)")
    print("\nResilience Features:")
    print("  - Error handling and recovery")
    print("  - Maintenance mode simulation")
    print("  - Rate limiting simulation")
    print("  - Random error injection (configurable)")
    print("  - Request statistics tracking")
    print("=" * 70)
    print(f"Server starting on http://localhost:5002")
    print("=" * 70)
    
    try:
        app.run(host='0.0.0.0', port=5002, debug=False, threaded=True, use_reloader=False)
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Server stopped by user")
    except Exception as e:
        print(f"\n[ERROR] Server error: {e}")
        raise

