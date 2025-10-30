"""
Quick Fix Script - Stop syncs and start mock server
"""
import subprocess
import time
import sys

print("\n" + "="*100)
print("QUICK FIX - RESTART EVERYTHING")
print("="*100)

print("\n[1/4] Starting Mock SSE Server on port 3000...")
print("      This provides test data at: http://localhost:3000/api/crm/stream")

# Start mock server in background
try:
    mock_server = subprocess.Popen(
        ["node", "mock-sse-server.js"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        creationflags=subprocess.CREATE_NEW_CONSOLE
    )
    print("      ✓ Mock server starting...")
    time.sleep(2)  # Give it time to start
except FileNotFoundError:
    print("      × Node.js not found - skipping mock server")
    print("        (CoinGecko and JSONPlaceholder will still work)")
except Exception as e:
    print(f"      × Error starting mock server: {e}")

print("\n[2/4] Stopping Flask app...")
print("      Press Ctrl+C in the Flask terminal window to stop it")
print("      Then run: python app.py")

print("\n[3/4] Database fixes already applied:")
print("      ✓ Fixed endpoint: /api/data/stream → /api/crm/stream")
print("      ✓ Added target database: test9")
print("      ✓ CoinGecko polling: 60 seconds (rate limit safe)")

print("\n[4/4] After restarting Flask:")
print("      1. Wait 2-3 minutes for CoinGecko rate limit to reset")
print("      2. Go to http://localhost:5001")
print("      3. Click 'Sync Server' on each source")

print("\n" + "="*100)
print("READY!")
print("="*100)
print("\nNext: Restart Flask with: python app.py")
