"""Simple check using requests to get the homepage HTML and verify cards"""
import requests
import re

def check_home_page():
    print("Checking homepage...")
    
    # Create session and login
    session = requests.Session()
    login_response = session.post('http://127.0.0.1:5001/login', data={
        'username': 'admin',
        'password': 'admin123'
    }, allow_redirects=False)
    
    if login_response.status_code not in [200, 302]:
        login_response = session.post('http://127.0.0.1:5001/login', data={
            'username': 'admin',
            'password': 'admin'
        }, allow_redirects=False)
    
    print(f"Login status: {login_response.status_code}")
    
    # Get homepage
    home_response = session.get('http://127.0.0.1:5001/')
    print(f"Home page status: {home_response.status_code}")
    
    if home_response.status_code != 200:
        print(f"✗ Could not load home page: {home_response.status_code}")
        return
    
    html = home_response.text
    
    # Save to file for inspection
    with open('homepage_output.html', 'w', encoding='utf-8') as f:
        f.write(html)
    print("✓ Homepage HTML saved to homepage_output.html")
    
    # Check for data_sources
    if 'server121' in html:
        print("✓ Found 'server121' in HTML")
    else:
        print("✗ 'server121' NOT found in HTML")
    
    if 'server12' in html:
        print("✓ Found 'server12' in HTML")
    else:
        print("✗ 'server12' NOT found in HTML")
    
    if 'server1' in html:
        print("✓ Found 'server1' in HTML")
    else:
        print("✗ 'server1' NOT found in HTML")
    
    # Check for empty state
    if 'no servers or sources configured' in html.lower():
        print("✗ Empty state message detected")
    else:
        print("✓ No empty state message")
    
    # Count card instances  (looking for the card structure)
    card_count = html.count('class="bg-surface-light dark:bg-surface-dark rounded-xl')
    print(f"\nCard elements found: {card_count}")
    
    # Look for data-source-id attribute
    source_id_matches = re.findall(r'data-source-id="(\d+)"', html)
    if source_id_matches:
        print(f"✓ Found {len(source_id_matches)} elements with data-source-id")
        print(f"  Source IDs: {source_id_matches}")
    else:
        print("✗ No data-source-id attributes found")

if __name__ == '__main__':
    check_home_page()
