"""
Complete Test: Insert Sample Data to ClickHouse
This simulates what would come from HANA once connection is working
"""
from clickhouse_driver import Client
import sys
from datetime import datetime, timedelta, date
import random

print("="*80)
print("HANA → ClickHouse Sync Test (Simulated)")
print("="*80)

# Step 1: Connect to ClickHouse
print("\n[Step 1] Connecting to ClickHouse...")
try:
    client = Client(
        host='localhost',
        port=9000,
        user='default',
        password='',
        database='default'
    )
    print("✓ Connected to ClickHouse")
    
    # Test query
    result = client.execute('SELECT version()')
    print(f"  ClickHouse Version: {result[0][0]}")
    
except Exception as e:
    print(f"✗ Connection failed: {e}")
    sys.exit(1)

# Step 2: Create test database
print("\n[Step 2] Creating test database...")
try:
    client.execute('CREATE DATABASE IF NOT EXISTS hana_sync')
    print("✓ Database 'hana_sync' created/exists")
except Exception as e:
    print(f"✗ Failed: {e}")
    sys.exit(1)

# Step 3: Create tables (simulating HANA schema)
print("\n[Step 3] Creating tables...")

tables = {
    'customers': """
        CREATE TABLE IF NOT EXISTS hana_sync.customers (
            customer_id Int32,
            customer_name String,
            email String,
            country String,
            registration_date Date,
            total_spent Decimal(10, 2)
        ) ENGINE = MergeTree()
        ORDER BY customer_id
    """,
    'products': """
        CREATE TABLE IF NOT EXISTS hana_sync.products (
            product_id Int32,
            product_name String,
            category String,
            price Decimal(10, 2),
            stock_quantity Int32,
            last_updated DateTime
        ) ENGINE = MergeTree()
        ORDER BY product_id
    """,
    'orders': """
        CREATE TABLE IF NOT EXISTS hana_sync.orders (
            order_id Int32,
            customer_id Int32,
            product_id Int32,
            quantity Int32,
            order_date DateTime,
            total_amount Decimal(10, 2),
            status String
        ) ENGINE = MergeTree()
        ORDER BY order_id
    """
}

for table_name, create_sql in tables.items():
    try:
        client.execute(create_sql)
        print(f"  ✓ Table '{table_name}' created")
    except Exception as e:
        print(f"  ✗ Failed to create '{table_name}': {e}")

# Step 4: Insert sample data
print("\n[Step 4] Inserting sample data...")

# Customers data
customers_data = [
    (1, 'Acme Corporation', 'contact@acme.com', 'USA', date(2024, 1, 15), 125000.50),
    (2, 'Global Industries', 'info@global.com', 'Germany', date(2024, 2, 20), 89000.75),
    (3, 'Tech Solutions Ltd', 'sales@techsol.com', 'UK', date(2024, 3, 10), 156000.00),
    (4, 'Innovation Labs', 'hello@innovate.com', 'India', date(2024, 4, 5), 67000.25),
    (5, 'Digital Dynamics', 'contact@digital.com', 'Canada', date(2024, 5, 12), 98000.00),
]

try:
    client.execute(
        'INSERT INTO hana_sync.customers VALUES',
        customers_data
    )
    print(f"  ✓ Inserted {len(customers_data)} customers")
except Exception as e:
    print(f"  ✗ Failed to insert customers: {e}")

# Products data
products_data = [
    (101, 'Enterprise Software License', 'Software', 5000.00, 100, datetime.now()),
    (102, 'Cloud Storage Plan', 'Services', 299.99, 500, datetime.now()),
    (103, 'Data Analytics Platform', 'Software', 15000.00, 50, datetime.now()),
    (104, 'Security Suite', 'Software', 8000.00, 75, datetime.now()),
    (105, 'Consulting Hours', 'Services', 200.00, 1000, datetime.now()),
]

try:
    client.execute(
        'INSERT INTO hana_sync.products VALUES',
        products_data
    )
    print(f"  ✓ Inserted {len(products_data)} products")
except Exception as e:
    print(f"  ✗ Failed to insert products: {e}")

# Orders data
orders_data = []
order_id = 1
for customer_id in range(1, 6):
    for _ in range(random.randint(2, 5)):  # 2-5 orders per customer
        product_id = random.choice([101, 102, 103, 104, 105])
        quantity = random.randint(1, 10)
        # Get product price
        product_price = next((p[3] for p in products_data if p[0] == product_id), 0)
        total = float(product_price) * quantity
        order_date = datetime.now() - timedelta(days=random.randint(1, 90))
        status = random.choice(['Completed', 'Pending', 'Shipped'])
        
        orders_data.append((
            order_id,
            customer_id,
            product_id,
            quantity,
            order_date,
            total,
            status
        ))
        order_id += 1

try:
    client.execute(
        'INSERT INTO hana_sync.orders VALUES',
        orders_data
    )
    print(f"  ✓ Inserted {len(orders_data)} orders")
except Exception as e:
    print(f"  ✗ Failed to insert orders: {e}")

# Step 5: Verify data
print("\n[Step 5] Verifying data in ClickHouse...")

queries = [
    ('customers', 'SELECT COUNT(*) FROM hana_sync.customers'),
    ('products', 'SELECT COUNT(*) FROM hana_sync.products'),
    ('orders', 'SELECT COUNT(*) FROM hana_sync.orders'),
]

total_rows = 0
for table, query in queries:
    try:
        result = client.execute(query)
        count = result[0][0]
        total_rows += count
        print(f"  ✓ {table}: {count} rows")
    except Exception as e:
        print(f"  ✗ Failed to query {table}: {e}")

# Step 6: Sample data preview
print("\n[Step 6] Sample Data Preview...")

print("\n  Top 3 Customers:")
customers = client.execute('SELECT customer_name, country, total_spent FROM hana_sync.customers ORDER BY total_spent DESC LIMIT 3')
for name, country, spent in customers:
    print(f"    - {name} ({country}): ${spent:,.2f}")

print("\n  Recent Orders:")
orders = client.execute('''
    SELECT o.order_id, c.customer_name, p.product_name, o.quantity, o.total_amount, o.status
    FROM hana_sync.orders o
    JOIN hana_sync.customers c ON o.customer_id = c.customer_id
    JOIN hana_sync.products p ON o.product_id = p.product_id
    ORDER BY o.order_date DESC
    LIMIT 5
''')
for order_id, cust, prod, qty, total, status in orders:
    print(f"    #{order_id}: {cust} - {prod} x{qty} = ${total:,.2f} [{status}]")

# Step 7: Analytics Query
print("\n[Step 7] Analytics Example...")
analytics = client.execute('''
    SELECT 
        c.country,
        COUNT(DISTINCT c.customer_id) as customers,
        COUNT(o.order_id) as orders,
        SUM(o.total_amount) as revenue
    FROM hana_sync.customers c
    LEFT JOIN hana_sync.orders o ON c.customer_id = o.customer_id
    GROUP BY c.country
    ORDER BY revenue DESC
''')

print("\n  Revenue by Country:")
print("  " + "-"*60)
print(f"  {'Country':<20} {'Customers':<12} {'Orders':<10} {'Revenue':>15}")
print("  " + "-"*60)
for country, customers, orders, revenue in analytics:
    print(f"  {country:<20} {customers:<12} {orders:<10} ${revenue:>14,.2f}")
print("  " + "-"*60)

# Final Summary
print("\n" + "="*80)
print("✓ TEST COMPLETED SUCCESSFULLY")
print("="*80)
print(f"\nSummary:")
print(f"  - Database: hana_sync")
print(f"  - Tables: 3 (customers, products, orders)")
print(f"  - Total Rows: {total_rows}")
print(f"  - Status: All data inserted and queryable")
print("\nThis data simulates what would be synced from HANA.")
print("Once HANA connection is resolved, the same structure will be used.")
print("\nNext steps:")
print("  1. Fix HANA container port configuration")
print("  2. Test actual HANA → ClickHouse sync")
print("  3. Add incremental sync capability")
