"""
ClickHouse-specific validation utilities
Handles validation for data synced to ClickHouse from various sources
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from clickhouse_driver import Client
from data_integrity import (
    ValidationResult,
    validate_row_counts,
    compare_tables_full,
    compute_table_checksum,
    compute_row_checksum
)
from db_utils import load_clickhouse_config

logger = logging.getLogger(__name__)


class ClickHouseValidator:
    """Validator for ClickHouse data"""
    
    def __init__(self, database: str = None):
        """Initialize ClickHouse validator"""
        ch_config = load_clickhouse_config()
        self.database = database or ch_config.get('database', 'default')
        self.ch_config = ch_config
        self.client = None
    
    def connect(self) -> bool:
        """Connect to ClickHouse"""
        try:
            self.client = Client(
                host=self.ch_config['host'],
                port=self.ch_config['port'],
                user=self.ch_config['user'],
                password=self.ch_config['password'],
                database=self.database
            )
            logger.info(f"Connected to ClickHouse: {self.ch_config['host']}:{self.ch_config['port']}")
            return True
        except Exception as e:
            logger.error(f"ClickHouse connection failed: {e}")
            return False
    
    def get_table_row_count(self, table_name: str) -> int:
        """Get row count from ClickHouse table"""
        try:
            if not self.client:
                self.connect()
            
            query = f"SELECT count() FROM {table_name}"
            result = self.client.execute(query)
            return result[0][0] if result else 0
        except Exception as e:
            logger.error(f"Error getting row count for {table_name}: {e}")
            return 0
    
    def get_table_data(self, table_name: str, limit: int = None, where_clause: str = None) -> List[Dict]:
        """Get data from ClickHouse table"""
        try:
            if not self.client:
                self.connect()
            
            query = f"SELECT * FROM {table_name}"
            if where_clause:
                query += f" WHERE {where_clause}"
            if limit:
                query += f" LIMIT {limit}"
            
            result = self.client.execute(query)
            
            # Get column names
            columns_query = f"DESCRIBE TABLE {table_name}"
            columns_result = self.client.execute(columns_query)
            columns = [col[0] for col in columns_result]
            
            # Convert to list of dictionaries
            rows = []
            for row in result:
                row_dict = dict(zip(columns, row))
                rows.append(row_dict)
            
            return rows
        except Exception as e:
            logger.error(f"Error getting data from {table_name}: {e}")
            return []
    
    def validate_table_exists(self, table_name: str) -> bool:
        """Check if table exists in ClickHouse"""
        try:
            if not self.client:
                self.connect()
            
            query = f"EXISTS TABLE {table_name}"
            result = self.client.execute(query)
            return result[0][0] == 1 if result else False
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            return False
    
    def validate_row_count(
        self,
        table_name: str,
        expected_count: int,
        tolerance: float = 0.0
    ) -> Tuple[bool, ValidationResult]:
        """Validate row count matches expected"""
        actual_count = self.get_table_row_count(table_name)
        is_valid, message = validate_row_counts(expected_count, actual_count, tolerance)
        
        result = ValidationResult(
            success=is_valid,
            source_rows=expected_count,
            target_rows=actual_count,
            missing_rows=max(0, expected_count - actual_count),
            extra_rows=max(0, actual_count - expected_count),
            mismatched_rows=0,
            checksum_match=False
        )
        
        if not is_valid:
            result.errors.append(message)
        else:
            result.details = {'message': message}
        
        return is_valid, result
    
    def validate_table_data(
        self,
        table_name: str,
        source_data: List[Dict],
        key_columns: List[str] = None,
        sample_size: int = None
    ) -> ValidationResult:
        """Validate table data matches source data"""
        try:
            # Get target data
            if sample_size:
                target_data = self.get_table_data(table_name, limit=sample_size)
            else:
                target_data = self.get_table_data(table_name)
            
            # Perform comparison
            result = compare_tables_full(
                source_rows=source_data,
                target_rows=target_data,
                key_columns=key_columns
            )
            
            return result
        except Exception as e:
            logger.error(f"Error validating table data: {e}")
            result = ValidationResult(
                success=False,
                source_rows=len(source_data) if source_data else 0,
                target_rows=0,
                missing_rows=0,
                extra_rows=0,
                mismatched_rows=0,
                checksum_match=False
            )
            result.errors.append(f"Validation error: {str(e)}")
            return result
    
    def get_table_checksum(self, table_name: str, exclude_columns: List[str] = None) -> Optional[str]:
        """Get checksum for entire table"""
        try:
            data = self.get_table_data(table_name)
            if not data:
                return None
            
            return compute_table_checksum(data, exclude_columns)
        except Exception as e:
            logger.error(f"Error computing table checksum: {e}")
            return None
    
    def close(self):
        """Close ClickHouse connection"""
        if self.client:
            try:
                self.client.disconnect()
            except:
                pass

