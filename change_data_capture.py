"""
Change Data Capture (CDC) Module - Phase 2
Tracks inserts, updates, and deletes for APIs and HANA sources
"""

import logging
import hashlib
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, asdict
from db_utils import get_pg_connection, return_pg_connection
from data_integrity import compute_row_checksum

logger = logging.getLogger(__name__)


@dataclass
class ChangeRecord:
    """Represents a single change (insert, update, delete)"""
    change_type: str  # 'insert', 'update', 'delete'
    source_name: str
    table_name: str
    record_id: Optional[str]
    record_hash: str
    old_data: Optional[Dict] = None
    new_data: Optional[Dict] = None
    change_timestamp: datetime = None
    sync_id: Optional[str] = None
    
    def __post_init__(self):
        if self.change_timestamp is None:
            self.change_timestamp = datetime.now()


class ChangeDataCapture:
    """Manages Change Data Capture for sync operations"""
    
    def __init__(self):
        self.pending_changes: List[ChangeRecord] = []
    
    def detect_changes(
        self,
        source_name: str,
        table_name: str,
        old_data: List[Dict],
        new_data: List[Dict],
        key_columns: List[str] = None
    ) -> List[ChangeRecord]:
        """
        Detect changes between old and new data.
        
        Args:
            source_name: Name of the data source
            table_name: Name of the table
            old_data: Previous state of data
            new_data: Current state of data
            key_columns: Columns to use as primary key
        
        Returns:
            List of ChangeRecord objects
        """
        changes = []
        
        try:
            # Convert to dictionaries keyed by record identifier
            if key_columns:
                old_dict = {}
                for row in old_data:
                    key = self._get_record_key(row, key_columns)
                    old_dict[key] = row
                
                new_dict = {}
                for row in new_data:
                    key = self._get_record_key(row, key_columns)
                    new_dict[key] = row
                
                # Find inserts (in new but not in old)
                for key, row in new_dict.items():
                    if key not in old_dict:
                        changes.append(ChangeRecord(
                            change_type='insert',
                            source_name=source_name,
                            table_name=table_name,
                            record_id=str(key),
                            record_hash=compute_row_checksum(row),
                            new_data=row
                        ))
                
                # Find updates (in both but different)
                for key in set(old_dict.keys()) & set(new_dict.keys()):
                    old_row = old_dict[key]
                    new_row = new_dict[key]
                    
                    old_hash = compute_row_checksum(old_row)
                    new_hash = compute_row_checksum(new_row)
                    
                    if old_hash != new_hash:
                        changes.append(ChangeRecord(
                            change_type='update',
                            source_name=source_name,
                            table_name=table_name,
                            record_id=str(key),
                            record_hash=new_hash,
                            old_data=old_row,
                            new_data=new_row
                        ))
                
                # Find deletes (in old but not in new)
                for key, row in old_dict.items():
                    if key not in new_dict:
                        changes.append(ChangeRecord(
                            change_type='delete',
                            source_name=source_name,
                            table_name=table_name,
                            record_id=str(key),
                            record_hash=compute_row_checksum(row),
                            old_data=row
                        ))
            else:
                # No key columns - use row hashes
                old_hashes = {compute_row_checksum(row): row for row in old_data}
                new_hashes = {compute_row_checksum(row): row for row in new_data}
                
                # Inserts
                for hash_val, row in new_hashes.items():
                    if hash_val not in old_hashes:
                        changes.append(ChangeRecord(
                            change_type='insert',
                            source_name=source_name,
                            table_name=table_name,
                            record_id=None,
                            record_hash=hash_val,
                            new_data=row
                        ))
                
                # Deletes
                for hash_val, row in old_hashes.items():
                    if hash_val not in new_hashes:
                        changes.append(ChangeRecord(
                            change_type='delete',
                            source_name=source_name,
                            table_name=table_name,
                            record_id=None,
                            record_hash=hash_val,
                            old_data=row
                        ))
            
            logger.info(f"Detected {len(changes)} changes for {source_name}.{table_name}: "
                       f"{sum(1 for c in changes if c.change_type == 'insert')} inserts, "
                       f"{sum(1 for c in changes if c.change_type == 'update')} updates, "
                       f"{sum(1 for c in changes if c.change_type == 'delete')} deletes")
            
        except Exception as e:
            logger.error(f"Error detecting changes: {e}", exc_info=True)
        
        return changes
    
    def _get_record_key(self, row: Dict, key_columns: List[str]) -> Tuple:
        """Get record key from key columns"""
        return tuple(row.get(col) for col in key_columns)
    
    def save_changes(self, changes: List[ChangeRecord], sync_id: str = None):
        """Save change records to database"""
        if not changes:
            return
        
        try:
            conn = get_pg_connection()
            try:
                cur = conn.cursor()
                
                for change in changes:
                    cur.execute("""
                        INSERT INTO metrics_sync_tables.change_log
                        (change_type, source_name, table_name, record_id, record_hash,
                         old_data, new_data, change_timestamp, sync_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        change.change_type,
                        change.source_name,
                        change.table_name,
                        change.record_id,
                        change.record_hash,
                        json.dumps(change.old_data) if change.old_data else None,
                        json.dumps(change.new_data) if change.new_data else None,
                        change.change_timestamp,
                        sync_id
                    ))
                
                conn.commit()
                cur.close()
                logger.info(f"Saved {len(changes)} change records to database")
            finally:
                return_pg_connection(conn)
        except Exception as e:
            logger.error(f"Error saving changes: {e}", exc_info=True)
    
    def get_changes(
        self,
        source_name: str = None,
        table_name: str = None,
        change_type: str = None,
        since: datetime = None,
        limit: int = 100
    ) -> List[Dict]:
        """Get change records from database"""
        try:
            conn = get_pg_connection()
            try:
                cur = conn.cursor()
                
                query = """
                    SELECT id, change_type, source_name, table_name, record_id,
                           record_hash, old_data, new_data, change_timestamp, sync_id
                    FROM metrics_sync_tables.change_log
                    WHERE 1=1
                """
                params = []
                
                if source_name:
                    query += " AND source_name = %s"
                    params.append(source_name)
                
                if table_name:
                    query += " AND table_name = %s"
                    params.append(table_name)
                
                if change_type:
                    query += " AND change_type = %s"
                    params.append(change_type)
                
                if since:
                    query += " AND change_timestamp >= %s"
                    params.append(since)
                
                query += " ORDER BY change_timestamp DESC LIMIT %s"
                params.append(limit)
                
                cur.execute(query, params)
                rows = cur.fetchall()
                cur.close()
                
                changes = []
                for row in rows:
                    changes.append({
                        'id': row[0],
                        'change_type': row[1],
                        'source_name': row[2],
                        'table_name': row[3],
                        'record_id': row[4],
                        'record_hash': row[5],
                        'old_data': json.loads(row[6]) if row[6] else None,
                        'new_data': json.loads(row[7]) if row[7] else None,
                        'change_timestamp': row[8].isoformat() if row[8] else None,
                        'sync_id': row[9]
                    })
                
                return changes
            finally:
                return_pg_connection(conn)
        except Exception as e:
            logger.error(f"Error getting changes: {e}")
            return []
    
    def track_api_changes(
        self,
        api_url: str,
        table_name: str,
        previous_data: List[Dict],
        current_data: List[Dict],
        sync_id: str = None
    ) -> List[ChangeRecord]:
        """Track changes for API sync"""
        # Try to detect ID column
        key_columns = None
        if current_data:
            # Common ID column names
            id_candidates = ['id', 'ID', '_id', 'Id', 'record_id', 'key']
            for candidate in id_candidates:
                if candidate in current_data[0]:
                    key_columns = [candidate]
                    break
        
        changes = self.detect_changes(
            source_name=api_url,
            table_name=table_name,
            old_data=previous_data,
            new_data=current_data,
            key_columns=key_columns
        )
        
        if changes:
            self.save_changes(changes, sync_id)
        
        return changes
    
    def track_hana_changes(
        self,
        hana_source: str,
        schema: str,
        table: str,
        previous_data: List[Dict],
        current_data: List[Dict],
        key_columns: List[str] = None,
        sync_id: str = None
    ) -> List[ChangeRecord]:
        """Track changes for HANA sync"""
        changes = self.detect_changes(
            source_name=hana_source,
            table_name=f"{schema}_{table}",
            old_data=previous_data,
            new_data=current_data,
            key_columns=key_columns
        )
        
        if changes:
            self.save_changes(changes, sync_id)
        
        return changes


# Global CDC instance
cdc = ChangeDataCapture()

