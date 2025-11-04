"""
Setup HANA Sample Database
Creates database, schema, table and sample hospital data
Uses .env variables only (no hardcoded values)
"""
import os
import sys
import time
from dotenv import load_dotenv

# Load .env file
load_dotenv()

def load_hana_config():
    """Load HANA config from .env variables only"""
    host = os.environ.get('HANA_HOST')
    port = os.environ.get('HANA_PORT')
    username = os.environ.get('HANA_USERNAME')
    password = os.environ.get('HANA_PASSWORD')
    
    if not all([host, port, username, password]):
        raise ValueError(
            "Missing HANA environment variables. Please set in .env:\n"
            "HANA_HOST=localhost\n"
            "HANA_PORT=39017\n"
            "HANA_USERNAME=SYSTEM\n"
            "HANA_PASSWORD=YourPassword123"
        )
    
    try:
        port = int(port)
    except ValueError:
        raise ValueError(f"HANA_PORT must be a number, got: {port}")
    
    return {
        'host': host,
        'port': port,
        'username': username,
        'password': password
    }

def wait_for_hana(config, max_attempts=30, wait_seconds=10):
    """Wait for HANA to be ready"""
    print(f"Waiting for HANA at {config['host']}:{config['port']} to be ready...")
    
    try:
        import hdbcli.dbapi as hana_dbapi
    except ImportError:
        print("ERROR: hdbcli not installed. Install it with: pip install hdbcli")
        return False
    
    for attempt in range(max_attempts):
        try:
            conn = hana_dbapi.connect(
                address=config['host'],
                port=config['port'],
                user=config['username'],
                password=config['password']
            )
            conn.close()
            print("✅ HANA is ready!")
            return True
        except Exception as e:
            if attempt < max_attempts - 1:
                print(f"   Attempt {attempt + 1}/{max_attempts}: HANA not ready yet, waiting {wait_seconds}s...")
                time.sleep(wait_seconds)
            else:
                print(f"❌ HANA connection failed after {max_attempts} attempts: {e}")
                return False
    
    return False

def setup_hana_sample_data():
    """Create database, schema, table and sample data"""
    print("=" * 70)
    print("HANA Sample Database Setup - Hospital Data")
    print("=" * 70)
    
    # Load config from .env
    try:
        config = load_hana_config()
        print(f"\n✅ Loaded HANA config from .env:")
        print(f"   Host: {config['host']}")
        print(f"   Port: {config['port']}")
        print(f"   Username: {config['username']}")
        print(f"   Password: {'***' if config['password'] else '(empty)'}")
    except ValueError as e:
        print(f"\n❌ Configuration Error: {e}")
        return False
    
    # Wait for HANA to be ready
    if not wait_for_hana(config):
        return False
    
    try:
        import hdbcli.dbapi as hana_dbapi
    except ImportError:
        print("❌ ERROR: hdbcli not installed. Install it with: pip install hdbcli")
        return False
    
    # Connect to HANA
    try:
        print(f"\n📡 Connecting to HANA at {config['host']}:{config['port']}...")
        conn = hana_dbapi.connect(
            address=config['host'],
            port=config['port'],
            user=config['username'],
            password=config['password'],
            encrypt=True,
            sslValidateCertificate=False
        )
        print("✅ Connected to HANA!")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False
    
    cursor = conn.cursor()
    
    try:
        # Step 1: Create database
        db_name = "HOSPITAL_DB"
        print(f"\n1️⃣ Creating database '{db_name}'...")
        try:
            cursor.execute(f'CREATE DATABASE "{db_name}"')
            print(f"   ✅ Database '{db_name}' created")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"   ℹ️  Database '{db_name}' already exists")
            else:
                print(f"   ⚠️  Error creating database (might already exist): {e}")
        
        # Step 2: Create schema
        schema_name = "HOSPITAL_SCHEMA"
        print(f"\n2️⃣ Creating schema '{schema_name}'...")
        try:
            # Use the new database
            cursor.execute(f'USE DATABASE "{db_name}"')
            cursor.execute(f'CREATE SCHEMA "{schema_name}"')
            print(f"   ✅ Schema '{schema_name}' created")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"   ℹ️  Schema '{schema_name}' already exists")
            else:
                print(f"   ⚠️  Error creating schema: {e}")
                return False
        
        # Step 3: Create table
        table_name = "HOSPITALS"
        print(f"\n3️⃣ Creating table '{schema_name}.{table_name}'...")
        
        create_table_sql = f'''
        CREATE COLUMN TABLE "{schema_name}"."{table_name}"
        (
            HOSPITAL_ID INTEGER PRIMARY KEY,
            HOSPITAL_NAME NVARCHAR(200) NOT NULL,
            ADDRESS NVARCHAR(500),
            CITY NVARCHAR(100),
            STATE NVARCHAR(100),
            PHONE NVARCHAR(20),
            BEDS INTEGER,
            SPECIALITY NVARCHAR(200),
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        '''
        
        try:
            cursor.execute(create_table_sql)
            print(f"   ✅ Table '{schema_name}.{table_name}' created")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"   ℹ️  Table '{schema_name}.{table_name}' already exists")
                # Clear existing data
                cursor.execute(f'TRUNCATE TABLE "{schema_name}"."{table_name}"')
                print(f"   🗑️  Cleared existing data")
            else:
                print(f"   ❌ Error creating table: {e}")
                return False
        
        # Step 4: Insert sample data
        print(f"\n4️⃣ Inserting sample hospital data...")
        
        hospitals = [
            {
                'id': 1,
                'name': 'City General Hospital',
                'address': '123 Medical Center Drive',
                'city': 'New York',
                'state': 'NY',
                'phone': '555-0101',
                'beds': 500,
                'speciality': 'General Medicine, Cardiology, Surgery'
            },
            {
                'id': 2,
                'name': 'Sunset Medical Center',
                'address': '456 Health Boulevard',
                'city': 'Los Angeles',
                'state': 'CA',
                'phone': '555-0102',
                'beds': 350,
                'speciality': 'Emergency Care, Orthopedics, Pediatrics'
            },
            {
                'id': 3,
                'name': 'Riverside Community Hospital',
                'address': '789 Riverside Avenue',
                'city': 'Chicago',
                'state': 'IL',
                'phone': '555-0103',
                'beds': 275,
                'speciality': 'Oncology, Neurology, Maternity'
            }
        ]
        
        insert_sql = f'''
        INSERT INTO "{schema_name}"."{table_name}"
        (HOSPITAL_ID, HOSPITAL_NAME, ADDRESS, CITY, STATE, PHONE, BEDS, SPECIALITY)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        for hospital in hospitals:
            cursor.execute(insert_sql, (
                hospital['id'],
                hospital['name'],
                hospital['address'],
                hospital['city'],
                hospital['state'],
                hospital['phone'],
                hospital['beds'],
                hospital['speciality']
            ))
            print(f"   ✅ Inserted: {hospital['name']}")
        
        conn.commit()
        
        # Step 5: Verify data
        print(f"\n5️⃣ Verifying data...")
        cursor.execute(f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"')
        count = cursor.fetchone()[0]
        print(f"   ✅ Found {count} hospitals in table")
        
        cursor.execute(f'SELECT * FROM "{schema_name}"."{table_name}" ORDER BY HOSPITAL_ID')
        rows = cursor.fetchall()
        print(f"\n📋 Sample Data:")
        print("-" * 70)
        for row in rows:
            print(f"   ID: {row[0]}, Name: {row[1]}, City: {row[3]}, Beds: {row[6]}")
        print("-" * 70)
        
        print("\n" + "=" * 70)
        print("✅ HANA Sample Database Setup Complete!")
        print("=" * 70)
        print(f"\n📊 Database: {db_name}")
        print(f"📁 Schema: {schema_name}")
        print(f"📋 Table: {table_name}")
        print(f"📝 Records: {count}")
        print(f"\n💡 You can now use this in your sync tool with:")
        print(f"   HANA_HOST={config['host']}")
        print(f"   HANA_PORT={config['port']}")
        print(f"   HANA_USERNAME={config['username']}")
        print(f"   HANA_PASSWORD={'***' if config['password'] else '(empty)'}")
        print(f"\n   Select database: {db_name}")
        print(f"   Select schema: {schema_name}")
        print(f"   Select table: {table_name}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error during setup: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()
        print("\n🔌 Disconnected from HANA")

if __name__ == '__main__':
    success = setup_hana_sample_data()
    sys.exit(0 if success else 1)

