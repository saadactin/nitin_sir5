"""
Test Script for HANA Incremental Sync
Tests and verifies that incremental sync is working correctly for HANA sources
"""

import os
import sys
from typing import Dict, List, Optional
import logging
from datetime import datetime, timedelta

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('hana_incremental_sync_test.log')
    ]
)
logger = logging.getLogger(__name__)

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def test_hana_incremental_sync(source_id: Optional[int] = None, source_name: Optional[str] = None) -> Dict:
    """
    Test HANA incremental sync for a specific source.
    
    Args:
        source_id: Source ID from database (optional)
        source_name: Source name (optional, used if source_id not provided)
    
    Returns:
        dict with test results
    """
    results = {
        'success': False,
        'source_id': None,
        'source_name': None,
        'connection_test': False,
        'incremental_sync_test': False,
        'tables_synced': 0,
        'records_synced': 0,
        'errors': []
    }
    
    try:
        # Import required modules
        from hana_sync import HanaToClickHouseSync
        from db_utils import load_hana_config, load_clickhouse_config, load_pg_config
        import psycopg2
        import json
        
        # Get source from database
        pg_conf = load_pg_config()
        conn = psycopg2.connect(
            dbname=pg_conf.get('database', 'metrics_sync_tables'),
            user=pg_conf.get('username'),
            password=pg_conf.get('password'),
            host=pg_conf.get('host'),
            port=int(pg_conf.get('port', 5432))
        )
        cursor = conn.cursor()
        
        if source_id:
            cursor.execute("""
                SELECT id, source_name, server_address, username, password, 
                       connection_details, target_database, source_type
                FROM data_sources 
                WHERE id = %s AND is_active = true
            """, (source_id,))
        elif source_name:
            cursor.execute("""
                SELECT id, source_name, server_address, username, password, 
                       connection_details, target_database, source_type
                FROM data_sources 
                WHERE source_name = %s AND is_active = true
            """, (source_name,))
        else:
            # Get first HANA source
            cursor.execute("""
                SELECT id, source_name, server_address, username, password, 
                       connection_details, target_database, source_type
                FROM data_sources 
                WHERE source_type = 'sap_hana' AND is_active = true
                ORDER BY id
                LIMIT 1
            """)
        
        row = cursor.fetchone()
        if not row:
            results['errors'].append("No HANA source found")
            return results
        
        source_id, source_name, server_address, username, password, \
        connection_details_json, target_database, source_type = row
        
        results['source_id'] = source_id
        results['source_name'] = source_name
        
        if source_type != 'sap_hana':
            results['errors'].append(f"Source '{source_name}' is not a HANA source (type: {source_type})")
            return results
        
        logger.info(f"Testing incremental sync for HANA source: {source_name} (ID: {source_id})")
        
        # Parse connection details
        connection_details = {}
        if connection_details_json:
            if isinstance(connection_details_json, str):
                connection_details = json.loads(connection_details_json)
            else:
                connection_details = connection_details_json
        
        # Build HANA config
        try:
            hana_base_config = load_hana_config()
            source_host = connection_details.get('host') or (server_address.split(':')[0] if ':' in server_address else server_address)
            source_port = connection_details.get('port') or (server_address.split(':')[1] if ':' in server_address else None)
            
            hana_config = {
                'host': source_host if source_host else hana_base_config['host'],
                'port': int(source_port) if source_port else hana_base_config['port'],
                'username': username if username else hana_base_config['username'],
                'password': password if password else hana_base_config['password']
            }
        except ValueError:
            source_host = connection_details.get('host') or (server_address.split(':')[0] if ':' in server_address else server_address)
            source_port = connection_details.get('port') or (server_address.split(':')[1] if ':' in server_address else None)
            if not source_port:
                results['errors'].append("HANA_PORT is required")
                return results
            
            hana_config = {
                'host': source_host,
                'port': int(source_port),
                'username': username,
                'password': password or connection_details.get('password', '')
            }
        
        # Build ClickHouse config
        ch_base_config = load_clickhouse_config()
        clickhouse_config = {
            'host': ch_base_config['host'],
            'port': ch_base_config['port'],
            'user': ch_base_config['user'],
            'password': ch_base_config['password'],
            'database': target_database or 'JARVIS_DB'
        }
        
        cursor.close()
        conn.close()
        
        # Test connections
        logger.info("Testing HANA connection...")
        sync_engine = HanaToClickHouseSync(hana_config, clickhouse_config)
        
        if not sync_engine.connect_hana():
            results['errors'].append(f"Failed to connect to HANA: {hana_config['host']}:{hana_config['port']}")
            return results
        
        results['connection_test'] = True
        logger.info("✅ HANA connection successful")
        
        if not sync_engine.connect_clickhouse():
            results['errors'].append(f"Failed to connect to ClickHouse: {clickhouse_config['host']}:{clickhouse_config['port']}")
            sync_engine.close_connections()
            return results
        
        logger.info("✅ ClickHouse connection successful")
        
        # Perform incremental sync
        logger.info("Performing incremental sync...")
        try:
            sync_results = sync_engine.sync_incremental(clickhouse_config['database'])
            
            if sync_results:
                results['incremental_sync_test'] = True
                results['tables_synced'] = len([r for r in sync_results if r and isinstance(r, dict) and r.get('status') == 'success'])
                results['records_synced'] = sum(r.get('records_synced', 0) for r in sync_results if r and isinstance(r, dict))
                
                logger.info(f"✅ Incremental sync completed: {results['tables_synced']} tables, {results['records_synced']} records")
                
                # Log details for each table
                for result in sync_results:
                    if result and isinstance(result, dict):
                        if result.get('status') == 'success':
                            logger.info(f"  ✅ {result.get('source_table', 'Unknown')}: {result.get('records_synced', 0)} records")
                        else:
                            logger.warning(f"  ⚠️ {result.get('source_table', 'Unknown')}: {result.get('message', 'Failed')}")
                            results['errors'].append(f"{result.get('source_table', 'Unknown')}: {result.get('message', 'Failed')}")
            else:
                results['errors'].append("Incremental sync returned no results")
        
        except Exception as e:
            logger.error(f"Incremental sync error: {e}")
            results['errors'].append(f"Incremental sync failed: {str(e)}")
        
        sync_engine.close_connections()
        results['success'] = results['connection_test'] and results['incremental_sync_test']
        
    except Exception as e:
        logger.exception(f"Test error: {e}")
        results['errors'].append(str(e))
    
    return results


def test_scheduling_for_hana() -> Dict:
    """
    Test if HANA sources are properly scheduled.
    
    Returns:
        dict with scheduling test results
    """
    results = {
        'success': False,
        'sources_found': 0,
        'sources_scheduled': 0,
        'schedules': [],
        'errors': []
    }
    
    try:
        from scheduler_utils import scheduled_jobs
        from db_utils import load_pg_config
        import psycopg2
        
        # Get all HANA sources
        pg_conf = load_pg_config()
        conn = psycopg2.connect(
            dbname=pg_conf.get('database', 'metrics_sync_tables'),
            user=pg_conf.get('username'),
            password=pg_conf.get('password'),
            host=pg_conf.get('host'),
            port=int(pg_conf.get('port', 5432))
        )
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, source_name, source_type
            FROM data_sources
            WHERE source_type = 'sap_hana' AND is_active = true
        """)
        
        hana_sources = cursor.fetchall()
        results['sources_found'] = len(hana_sources)
        
        logger.info(f"Found {len(hana_sources)} HANA source(s)")
        
        # Check schedules in database
        for source_id, source_name, source_type in hana_sources:
            cursor.execute("""
                SELECT job_type, minutes, hour, minute, last_run, status
                FROM metrics_sync_tables.schedules
                WHERE source_id = %s AND (status != 'deleted' OR status IS NULL)
            """, (source_id,))
            
            schedules = cursor.fetchall()
            if schedules:
                results['sources_scheduled'] += 1
                for job_type, minutes, hour, minute, last_run, status in schedules:
                    schedule_info = {
                        'source_id': source_id,
                        'source_name': source_name,
                        'job_type': job_type,
                        'minutes': minutes,
                        'hour': hour,
                        'minute': minute,
                        'last_run': last_run.isoformat() if last_run else None,
                        'status': status
                    }
                    results['schedules'].append(schedule_info)
                    logger.info(f"  📅 {source_name}: {job_type} (last run: {last_run or 'Never'})")
            else:
                logger.info(f"  ⚠️ {source_name}: No schedule found")
        
        # Check in-memory scheduled jobs
        for job in scheduled_jobs:
            if job.get('source_id'):
                for source_id, source_name, _ in hana_sources:
                    if job.get('source_id') == source_id:
                        logger.info(f"  ✅ {source_name}: Scheduled in memory ({job.get('type')})")
        
        cursor.close()
        conn.close()
        
        results['success'] = results['sources_scheduled'] > 0
        
    except Exception as e:
        logger.exception(f"Scheduling test error: {e}")
        results['errors'].append(str(e))
    
    return results


def main():
    """Main test function"""
    print("\n" + "="*60)
    print("HANA Incremental Sync Test")
    print("="*60 + "\n")
    
    # Test 1: Check scheduling
    print("📅 Test 1: Checking HANA Source Scheduling")
    print("-" * 60)
    schedule_results = test_scheduling_for_hana()
    
    print(f"\nSources found: {schedule_results['sources_found']}")
    print(f"Sources scheduled: {schedule_results['sources_scheduled']}")
    
    if schedule_results['schedules']:
        print("\nActive Schedules:")
        for schedule in schedule_results['schedules']:
            print(f"  - {schedule['source_name']}: {schedule['job_type']}")
            if schedule['last_run']:
                print(f"    Last run: {schedule['last_run']}")
            print(f"    Status: {schedule['status']}")
    else:
        print("\n⚠️ No schedules found for HANA sources")
        print("   Use the 'Create Schedule' page to set up scheduling")
    
    # Test 2: Test incremental sync
    print("\n\n🔄 Test 2: Testing Incremental Sync")
    print("-" * 60)
    
    # Ask which source to test
    source_input = input("\nEnter source ID or name (press Enter to test first HANA source): ").strip()
    
    source_id = None
    source_name = None
    
    if source_input:
        try:
            source_id = int(source_input)
        except ValueError:
            source_name = source_input
    
    sync_results = test_hana_incremental_sync(source_id=source_id, source_name=source_name)
    
    print(f"\n{'='*60}")
    print("Test Results")
    print(f"{'='*60}")
    print(f"Source: {sync_results['source_name']} (ID: {sync_results['source_id']})")
    print(f"Connection Test: {'✅ Passed' if sync_results['connection_test'] else '❌ Failed'}")
    print(f"Incremental Sync Test: {'✅ Passed' if sync_results['incremental_sync_test'] else '❌ Failed'}")
    print(f"Tables Synced: {sync_results['tables_synced']}")
    print(f"Records Synced: {sync_results['records_synced']:,}")
    
    if sync_results['errors']:
        print(f"\nErrors ({len(sync_results['errors'])}):")
        for error in sync_results['errors']:
            print(f"  - {error}")
    
    print(f"\nOverall: {'✅ SUCCESS' if sync_results['success'] else '❌ FAILED'}")
    print("="*60 + "\n")
    
    # Recommendations
    if not schedule_results['success']:
        print("💡 Recommendation: Set up scheduling for HANA sources using the 'Create Schedule' page")
    
    if not sync_results['success']:
        print("💡 Recommendation: Check HANA and ClickHouse connections and ensure tables have timestamp columns")


if __name__ == "__main__":
    main()

