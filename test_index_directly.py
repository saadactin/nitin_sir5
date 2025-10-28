"""Direct test to manually trigger the index function and see output"""
import sys
sys.path.insert(0, '.')

# Mock Flask request context
from flask import Flask
from unittest.mock import MagicMock, patch

# Import app
import app as app_module

print("Setting up mock session...")
with app_module.app.test_client() as client:
    # Login first
    print("Attempting login...")
    response = client.post('/login', data={
        'username': 'admin',
        'password': 'admin123'
    }, follow_redirects=True)
    
    print(f"Login status: {response.status_code}")
    
    # Now get the homepage
    print("\nRequesting homepage...")
    response = client.get('/')
    
    print(f"Homepage status: {response.status_code}")
    
    if response.status_code == 200:
        html = response.data.decode('utf-8')
        
        # Check for source names
        if 'server121' in html:
            print("✓ Found server121")
        else:
            print("✗ server121 NOT found")
        
        if 'data-source-id' in html:
            print("✓ Found data-source-id")
        else:
            print("✗ data-source-id NOT found")
            
        if 'no servers or sources configured' in html.lower():
            print("✗ Empty state shown")
        else:
            print("✓ Not showing empty state")
    else:
        print(f"Error: {response.data}")
