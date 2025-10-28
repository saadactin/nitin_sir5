"""Test template rendering directly without running the full Flask app"""
from jinja2 import Environment, FileSystemLoader
import os

# Setup Jinja environment
template_dir = os.path.join(os.path.dirname(__file__), 'templates')
env = Environment(loader=FileSystemLoader(template_dir))

# Load the template
template = env.get_template('sync_servers.html')

# Create sample data matching what index() would pass
sample_data_sources = [
    {
        'id': 1,
        'source_name': 'test_server1',
        'source_type': 'sql_server',
        'server_address': 'localhost',
        'username': 'sa',
        'target_type': 'postgresql',
        'target_database': 'test1'
    },
    {
        'id': 2,
        'source_name': 'test_server2',
        'source_type': 'sql_server',
        'server_address': '192.168.1.100',
        'username': 'admin',
        'target_type': 'clickhouse',
        'target_database': 'analytics'
    }
]

sample_data_source_statuses = {
    1: {'online': True, 'error': None},
    2: {'online': False, 'error': 'Connection timeout'}
}

sample_sqlservers = {}  # Empty YAML servers for this test
sample_server_statuses = {}

# Render the template
try:
    rendered_html = template.render(
        sqlservers=sample_sqlservers,
        server_statuses=sample_server_statuses,
        data_sources=sample_data_sources,
        data_source_statuses=sample_data_source_statuses,
        role='admin'
    )
    
    print("✓ Template rendered successfully")
    print(f"Output length: {len(rendered_html)} characters")
    
    # Check for key content
    if 'test_server1' in rendered_html:
        print("✓ Found 'test_server1' in output")
    else:
        print("✗ 'test_server1' NOT found in output")
    
    if 'test_server2' in rendered_html:
        print("✓ Found 'test_server2' in output")
    else:
        print("✗ 'test_server2' NOT found in output")
    
    # Check for data source card structure
    if 'data-source-id' in rendered_html:
        print("✓ Found data-source-id attribute (card structure present)")
    else:
        print("✗ data-source-id attribute NOT found")
    
    # Check for empty state message
    if 'no servers or sources configured' in rendered_html.lower():
        print("✗ WARNING: Empty state message found (cards may not be rendering)")
    else:
        print("✓ No empty state message (cards should be visible)")
    
    # Save rendered HTML for inspection
    output_file = 'test_render_output.html'
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(rendered_html)
    print(f"\n✓ Full output saved to: {output_file}")
    print("  Open this file in a browser to see the rendered cards")
    
except Exception as e:
    print(f"✗ Template rendering failed: {e}")
    import traceback
    traceback.print_exc()
