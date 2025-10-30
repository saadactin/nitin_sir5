"""Quick script to show migrated data from ClickHouse"""
from clickhouse_driver import Client
from db_utils import load_clickhouse_config

ch_conf = load_clickhouse_config()
client = Client(
    host=ch_conf['host'],
    port=ch_conf['port'],
    user=ch_conf['user'],
    password=ch_conf['password']
)

print("\n" + "="*100)
print("✅ Sample Data from ClickHouse (test11.crm)")
print("="*100 + "\n")

result = client.execute('SELECT id, name, email, address_city, company_name FROM test11.crm LIMIT 5')

# Print header
print(f"{'ID':<5} {'Name':<25} {'Email':<35} {'City':<20} {'Company':<25}")
print("-"*110)

# Print rows
for row in result:
    print(f"{row[0]:<5} {row[1]:<25} {row[2]:<35} {row[3]:<20} {row[4]:<25}")

print("\n" + "="*100)
print(f"Total records in table: {client.execute('SELECT COUNT(*) FROM test11.crm')[0][0]}")
print("="*100 + "\n")

