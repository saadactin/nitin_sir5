"""
Performance Optimizer
Optimizes database queries, adds caching, and improves page load times
"""

import functools
import time
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import hashlib
import json

logger = logging.getLogger(__name__)

# Simple in-memory cache (can be upgraded to Redis later)
_cache = {}
_cache_timestamps = {}
CACHE_TTL = 300  # 5 minutes default


def cache_result(ttl: int = CACHE_TTL, key_prefix: str = ""):
    """
    Decorator to cache function results
    
    Args:
        ttl: Time to live in seconds
        key_prefix: Prefix for cache key
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Create cache key
            cache_key = f"{key_prefix}:{func.__name__}:{str(args)}:{str(kwargs)}"
            cache_key = hashlib.md5(cache_key.encode()).hexdigest()
            
            # Check cache
            if cache_key in _cache:
                timestamp = _cache_timestamps.get(cache_key, 0)
                if time.time() - timestamp < ttl:
                    # Cache hit - no debug logging
                    return _cache[cache_key]
            
            # Execute function
            result = func(*args, **kwargs)
            
            # Store in cache
            _cache[cache_key] = result
            _cache_timestamps[cache_key] = time.time()
            # Cache miss - no debug logging
            
            return result
        return wrapper
    return decorator


def clear_cache(pattern: str = None):
    """Clear cache entries matching pattern"""
    if pattern is None:
        _cache.clear()
        _cache_timestamps.clear()
        # Cache cleared - minimal logging
    else:
        keys_to_delete = [k for k in _cache.keys() if pattern in k]
        for key in keys_to_delete:
            _cache.pop(key, None)
            _cache_timestamps.pop(key, None)
        # Cache entries cleared - minimal logging


def optimize_query(query_func):
    """Decorator to optimize database queries"""
    @functools.wraps(query_func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = query_func(*args, **kwargs)
            duration = time.time() - start_time
            if duration > 1.0:  # Log slow queries
                logger.warning(f"Slow query in {query_func.__name__}: {duration:.2f}s")
            return result
        except Exception as e:
            logger.error(f"Query error in {query_func.__name__}: {e}")
            raise
    return wrapper


def paginate_query(query, page: int = 1, per_page: int = 50):
    """
    Add pagination to a query
    
    Args:
        query: SQLAlchemy query or raw query string
        page: Page number (1-indexed)
        per_page: Items per page
    
    Returns:
        dict with items, total, pages, current_page
    """
    offset = (page - 1) * per_page
    # This is a helper - actual implementation depends on query type
    return {
        'page': page,
        'per_page': per_page,
        'offset': offset
    }


def batch_process(items, batch_size: int = 100, processor=None):
    """
    Process items in batches to avoid memory issues
    
    Args:
        items: List of items to process
        batch_size: Size of each batch
        processor: Function to process each batch
    """
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        if processor:
            processor(batch)
        yield batch

