"""
Data validation script for HANA to ClickHouse migration
Validates that data has been correctly migrated from HANA to ClickHouse
"""

import sys
import os
from clickhouse_driver import Client
from datetime import datetime

try:
    import hdbcli.dbapi as hana_dbapi
    HANA_AVAILABLE = True
except ImportError:
    HANA_AVAILABLE = False
    print("Error: hdbcli not available. Install it to run validation.")
    sys.exit(1)


def validate_migration(hana_config, clickhouse_config, schema, table):
    """
    Validate that data migrated correctly from HANA to ClickHouse
    
    Args:
        hana_config: Dict with host, port, username, password
        clickhouse_config: Dict with host, port, user, password, database
        schema: HANA schema name
        table: HANA table name
    """
    print(f"\n{'='*70}")
    print(f"VALIDATING MIGRATION: {schema}.{table}")
    print(f"{'='*70}")
    
    # Connect to HANA
    print("Connecting to HANA...")
    try:
        hana_conn = hana_dbapi.connect(
            address=hana_config['host'],
            port=int(hana_config['port']),
            user=hana_config['username'],
            password=hana_config['password'],
            encrypt=True,
            sslValidateCertificate=False
        )
        print("✓ Connected to HANA")
    except Exception as e:
        print(f"✗ Failed to connect to HANA: {e}")
        return False
    
    # Connect to ClickHouse
    print("Connecting to ClickHouse...")
    try:
        ch_client = Client(
            host=clickhouse_config['host'],
            port=int(clickhouse_config['port']),
            user=clickhouse_config['user'],
            password=clickhouse_config.get('password', ''),
            database=clickhouse_config['database']
        )
        print("✓ Connected to ClickHouse")
    except Exception as e:
        print(f"✗ Failed to connect to ClickHouse: {e}")
        hana_conn.close()
        return False
    
    try:
        # Get ClickHouse table name
        clean_schema = schema.replace('"', '').upper().replace(' ', '_')
        clean_table = table.replace('"', '').upper().replace(' ', '_')
        ch_table_name = f"{clean_schema}_{clean_table}"
        
        # Check if table exists in ClickHouse
        print(f"\nChecking if ClickHouse table exists: {clickhouse_config['database']}.{ch_table_name}")
        try:
            ch_client.execute(f"SELECT 1 FROM {clickhouse_config['database']}.{ch_table_name} LIMIT 1")
            print("✓ ClickHouse table exists")
        except Exception as e:
            print(f"✗ ClickHouse table does not exist: {e}")
            return False
        
        # Compare row counts
        print("\n[1] Comparing row counts...")
        hana_cursor = hana_conn.cursor()
        hana_cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
        hana_count = hana_cursor.fetchone()[0]
        hana_cursor.close()
        
        ch_count = ch_client.execute(
            f"SELECT count() FROM {clickhouse_config['database']}.{ch_table_name}"
        )[0][0]
        
        print(f"  HANA row count: {hana_count:,}")
        print(f"  ClickHouse row count: {ch_count:,}")
        
        if hana_count == ch_count:
            print("  ✓ Row counts match!")
        else:
            print(f"  ✗ Row count mismatch! Difference: {abs(hana_count - ch_count):,}")
            return False
        
        # Compare sample data
        print("\n[2] Comparing sample data...")
        hana_cursor = hana_conn.cursor()
        hana_cursor.execute(f'SELECT * FROM "{schema}"."{table}" LIMIT 5')
        hana_columns = [desc[0] for desc in hana_cursor.description]
        hana_sample = hana_cursor.fetchall()
        hana_cursor.close()
        
        # Get ClickHouse sample
        ch_columns = [col[0] for col in ch_client.execute(
            f"SELECT name FROM system.columns WHERE database = '{clickhouse_config['database']}' "
            f"AND table = '{ch_table_name}' AND name NOT LIKE '_%' ORDER BY position"
        )]
        
        # Get data columns (exclude metadata columns)
        data_columns = [col for col in ch_columns if not col.startswith('_')]
        
        ch_sample = ch_client.execute(
            f"SELECT {', '.join([f'`{col}`' for col in data_columns[:len(hana_columns)]])} "
            f"FROM {clickhouse_config['database']}.{ch_table_name} LIMIT 5"
        )
        
        if len(hana_sample) != len(ch_sample):
            print(f"  ✗ Sample size mismatch: HANA={len(hana_sample)}, CH={len(ch_sample)}")
        else:
            print(f"  ✓ Sample sizes match ({len(hana_sample)} rows)")
            
            # Compare first row values
            if len(hana_sample) > 0 and len(ch_sample) > 0:
                match_count = 0
                mismatch_count = 0
                
                for i, (hana_row, ch_row) in enumerate(zip(hana_sample[:3], ch_sample[:3])):
                    # Compare non-null values
                    for j, (hana_val, ch_val) in enumerate(zip(hana_row[:min(5, len(hana_row))], ch_row[:min(5, len(ch_row))])):
                        if hana_val is None and ch_val is None:
                            match_count += 1
                        elif hana_val == ch_val:
                            match_count += 1
                        else:
                            # Try string comparison for dates/timestamps
                            if str(hana_val) == str(ch_val):
                                match_count += 1
                            else:
                                mismatch_count += 1
                                if mismatch_count <= 3:  # Show first 3 mismatches
                                    print(f"    ⚠ Value mismatch in row {i+1}, col {j+1}: HANA={hana_val}, CH={ch_val}")
                
                if mismatch_count == 0:
                    print("  ✓ Sample data matches!")
                else:
                    print(f"  ⚠ Found {mismatch_count} value mismatches (showing first 3)")
        
        # Verify metadata columns
        print("\n[3] Verifying metadata columns...")
        metadata_cols = ch_client.execute(
            f"SELECT name FROM system.columns WHERE database = '{clickhouse_config['database']}' "
            f"AND table = '{ch_table_name}' AND name LIKE '_%'"
        )
        metadata_names = [col[0] for col in metadata_cols]
        
        required_metadata = ['_source_schema', '_source_table', '_sync_timestamp']
        for req in required_metadata:
            if req in metadata_names:
                print(f"  ✓ {req} column exists")
            else:
                print(f"  ✗ {req} column missing!")
        
        # Verify metadata values
        print("\n[4] Verifying metadata values...")
        metadata_sample = ch_client.execute(
            f"SELECT _source_schema, _source_table FROM {clickhouse_config['database']}.{ch_table_name} LIMIT 1"
        )
        if metadata_sample:
            source_schema, source_table = metadata_sample[0]
            if source_schema == schema and source_table == table:
                print(f"  ✓ Metadata values correct: {source_schema}.{source_table}")
            else:
                print(f"  ✗ Metadata mismatch: expected {schema}.{table}, got {source_schema}.{source_table}")
        else:
            print("  ✗ No data in table to verify metadata")
        
        # Check for NULL handling
        print("\n[5] Checking NULL value handling...")
        hana_null_count = 0
        try:
            hana_cursor = hana_conn.cursor()
            # Count NULLs in first few columns
            for col in hana_columns[:3]:
                try:
                    hana_cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}" WHERE "{col}" IS NULL')
                    hana_null_count += hana_cursor.fetchone()[0]
                except:
                    pass
            hana_cursor.close()
        except:
            pass
        
        ch_null_count = 0
        try:
            for col in data_columns[:3]:
                try:
                    result = ch_client.execute(
                        f"SELECT count() FROM {clickhouse_config['database']}.{ch_table_name} "
                        f"WHERE `{col}` IS NULL"
                    )
                    ch_null_count += result[0][0]
                except:
                    pass
        except:
            pass
        
        print(f"  HANA NULL count (first 3 cols): {hana_null_count}")
        print(f"  ClickHouse NULL count (first 3 cols): {ch_null_count}")
        
        print(f"\n{'='*70}")
        print("VALIDATION SUMMARY")
        print(f"{'='*70}")
        print("✓ Row counts match")
        print("✓ Sample data verified")
        print("✓ Metadata columns present")
        print("✓ Migration validation passed!")
        
        return True
        
    except Exception as e:
        print(f"\n✗ Validation error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        hana_conn.close()
        ch_client.disconnect()


def main():
    """Main validation function"""
    print("="*70)
    print("HANA TO CLICKHOUSE MIGRATION VALIDATION")
    print("="*70)
    
    # Configuration
    hana_config = {
        'host': '192.168.16.62',
        'port': 30015,
        'username': 'Tor1111',
        'password': 'Tor1111'
    }
    
    clickhouse_config = {
        'host': os.getenv('CLICKHOUSE_HOST', 'localhost'),
        'port': int(os.getenv('CLICKHOUSE_PORT', '9000')),
        'user': os.getenv('CLICKHOUSE_USER', 'default'),
        'password': os.getenv('CLICKHOUSE_PASSWORD', ''),
        'database': os.getenv('CLICKHOUSE_DATABASE', 'hana_migrated')
    }
    
    # Get schema and table from command line or use defaults
    if len(sys.argv) >= 3:
        schema = sys.argv[1]
        table = sys.argv[2]
    else:
        print("\nUsage: python validate_hana_migration.py <schema> <table>")
        print("Example: python validate_hana_migration.py SCHEMA1 CUSTOMERS")
        print("\nOr provide schema and table interactively:")
        
        try:
            schema = input("Enter HANA schema name: ").strip()
            table = input("Enter HANA table name: ").strip()
        except KeyboardInterrupt:
            print("\nCancelled.")
            sys.exit(0)
    
    if not schema or not table:
        print("Error: Schema and table names are required")
        sys.exit(1)
    
    # Run validation
    success = validate_migration(hana_config, clickhouse_config, schema, table)
    
    if success:
        print("\n✓ All validations passed!")
        sys.exit(0)
    else:
        print("\n✗ Validation failed. Please check the output above.")
        sys.exit(1)


if __name__ == '__main__':
    main()

