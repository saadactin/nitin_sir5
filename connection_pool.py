"""
Connection Pool Manager
Provides connection pooling for PostgreSQL, SQL Server, HANA, and ClickHouse databases.
This improves performance by reusing connections and reduces connection overhead.
"""

import os
import logging
import threading
from contextlib import contextmanager
from typing import Optional, Dict, Any
from sqlalchemy import create_engine, Engine, event
from sqlalchemy.pool import QueuePool, NullPool
from sqlalchemy.engine import Connection
import psycopg2
from psycopg2 import pool
import pyodbc
from hdbcli import dbapi

logger = logging.getLogger(__name__)

# Global connection pools
_pg_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None
_pg_engine: Optional[Engine] = None
_sql_server_engines: Dict[str, Engine] = {}
_hana_connections: Dict[str, Any] = {}
_clickhouse_clients: Dict[str, Any] = {}
_pool_lock = threading.Lock()

# Pool configuration - Increased defaults to prevent exhaustion
PG_POOL_MIN_CONN = int(os.environ.get('PG_POOL_MIN_CONN', '5'))
PG_POOL_MAX_CONN = int(os.environ.get('PG_POOL_MAX_CONN', '20'))
SQL_POOL_SIZE = int(os.environ.get('SQL_POOL_SIZE', '5'))
SQL_MAX_OVERFLOW = int(os.environ.get('SQL_MAX_OVERFLOW', '10'))
HANA_POOL_SIZE = int(os.environ.get('HANA_POOL_SIZE', '3'))
CLICKHOUSE_POOL_SIZE = int(os.environ.get('CLICKHOUSE_POOL_SIZE', '5'))


class ConnectionPoolManager:
    """Manages connection pools for all database types"""
    
    @staticmethod
    def init_postgresql_pool(config: Dict[str, Any]) -> bool:
        """Initialize PostgreSQL connection pool"""
        global _pg_pool, _pg_engine
        
        try:
            with _pool_lock:
                if _pg_pool is not None:
                    logger.info("PostgreSQL pool already initialized")
                    return True
                
                # Create psycopg2 connection pool
                _pg_pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=PG_POOL_MIN_CONN,
                    maxconn=PG_POOL_MAX_CONN,
                    host=config['host'],
                    port=config['port'],
                    database=config['database'],
                    user=config['username'],
                    password=config['password'],
                    connect_timeout=10
                )
                
                # Create SQLAlchemy engine for ORM operations
                connection_string = (
                    f"postgresql+psycopg2://{config['username']}:{config['password']}"
                    f"@{config['host']}:{config['port']}/{config['database']}"
                )
                
                _pg_engine = create_engine(
                    connection_string,
                    poolclass=QueuePool,
                    pool_size=PG_POOL_MIN_CONN,
                    max_overflow=PG_POOL_MAX_CONN - PG_POOL_MIN_CONN,
                    pool_pre_ping=True,  # Verify connections before using
                    pool_recycle=3600,  # Recycle connections after 1 hour
                    echo=False
                )
                
                logger.info(f"PostgreSQL connection pool initialized (min={PG_POOL_MIN_CONN}, max={PG_POOL_MAX_CONN})")
                return True
                
        except Exception as e:
            logger.error(f"Failed to initialize PostgreSQL pool: {e}")
            return False
    
    @staticmethod
    def get_postgresql_connection():
        """Get a connection from PostgreSQL pool with timeout and fallback"""
        global _pg_pool
        
        if _pg_pool is None:
            from db_utils import load_pg_config
            config = load_pg_config()
            if not ConnectionPoolManager.init_postgresql_pool(config):
                raise RuntimeError("Failed to initialize PostgreSQL connection pool")
        
        try:
            import time
            timeout = 5  # seconds
            start_time = time.time()
            
            while True:
                try:
                    return _pg_pool.getconn()
                except Exception as pool_error:
                    # Check if it's a pool exhaustion error
                    if "pool exhausted" in str(pool_error).lower() or "PoolError" in str(type(pool_error).__name__):
                        elapsed = time.time() - start_time
                        if elapsed > timeout:
                            # Timeout reached, fallback to direct connection
                            logger.warning(f"Connection pool exhausted after {elapsed:.1f}s, using direct connection")
                            from db_utils import load_pg_config
                            import psycopg2
                            config = load_pg_config()
                            return psycopg2.connect(
                                dbname=config["database"],
                                user=config["username"],
                                password=config["password"],
                                host=config["host"],
                                port=config["port"],
                            )
                        time.sleep(0.1)  # Wait 100ms before retry
                    else:
                        # Other error, raise it
                        raise
        except Exception as e:
            logger.error(f"Failed to get connection from PostgreSQL pool: {e}")
            # Fallback to direct connection
            try:
                from db_utils import load_pg_config
                import psycopg2
                config = load_pg_config()
                logger.warning("Falling back to direct PostgreSQL connection")
                return psycopg2.connect(
                    dbname=config["database"],
                    user=config["username"],
                    password=config["password"],
                    host=config["host"],
                    port=config["port"],
                )
            except Exception as fallback_error:
                logger.error(f"Fallback connection also failed: {fallback_error}")
                raise
    
    @staticmethod
    def return_postgresql_connection(conn):
        """Return a connection to PostgreSQL pool (or close if direct connection)"""
        global _pg_pool
        
        if not conn:
            return
        
        # Check if this is a pooled connection or direct connection
        # Pooled connections have a _pool attribute
        if _pg_pool and hasattr(conn, '_pool'):
            try:
                _pg_pool.putconn(conn)
            except Exception as e:
                logger.error(f"Error returning connection to pool: {e}")
                try:
                    conn.close()
                except:
                    pass
        else:
            # Direct connection (fallback), just close it
            try:
                conn.close()
            except:
                pass
    
    @staticmethod
    def get_postgresql_engine() -> Engine:
        """Get SQLAlchemy engine for PostgreSQL"""
        global _pg_engine
        
        if _pg_engine is None:
            from db_utils import load_pg_config
            config = load_pg_config()
            if not ConnectionPoolManager.init_postgresql_pool(config):
                raise RuntimeError("Failed to initialize PostgreSQL connection pool")
        
        return _pg_engine
    
    @staticmethod
    def init_sql_server_pool(server_name: str, config: Dict[str, Any]) -> bool:
        """Initialize SQL Server connection pool for a specific server"""
        global _sql_server_engines
        
        try:
            with _pool_lock:
                if server_name in _sql_server_engines:
                    logger.info(f"SQL Server pool for {server_name} already initialized")
                    return True
                
                from urllib.parse import quote_plus
                from hybrid_sync import get_sqlalchemy_engine
                
                # Use existing get_sqlalchemy_engine but enhance with pooling
                engine = get_sqlalchemy_engine(config)
                
                # Configure pool settings
                engine.pool._pool_size = SQL_POOL_SIZE
                engine.pool._max_overflow = SQL_MAX_OVERFLOW
                engine.pool._pool_pre_ping = True
                engine.pool._pool_recycle = 3600
                
                _sql_server_engines[server_name] = engine
                logger.info(f"SQL Server connection pool initialized for {server_name} (size={SQL_POOL_SIZE})")
                return True
                
        except Exception as e:
            logger.error(f"Failed to initialize SQL Server pool for {server_name}: {e}")
            return False
    
    @staticmethod
    def get_sql_server_engine(server_name: str, config: Dict[str, Any]) -> Engine:
        """Get SQL Server engine (creates pool if needed)"""
        global _sql_server_engines
        
        if server_name not in _sql_server_engines:
            ConnectionPoolManager.init_sql_server_pool(server_name, config)
        
        return _sql_server_engines.get(server_name)
    
    @staticmethod
    def init_hana_pool(config: Dict[str, Any], pool_key: str = 'default') -> bool:
        """Initialize HANA connection pool"""
        global _hana_connections
        
        try:
            with _pool_lock:
                if pool_key in _hana_connections:
                    logger.info(f"HANA pool {pool_key} already initialized")
                    return True
                
                # HANA doesn't have built-in pooling, so we'll create a connection manager
                # Store config for creating connections on demand
                _hana_connections[pool_key] = {
                    'config': config,
                    'connections': [],
                    'max_size': HANA_POOL_SIZE
                }
                
                logger.info(f"HANA connection manager initialized for {pool_key} (max={HANA_POOL_SIZE})")
                return True
                
        except Exception as e:
            logger.error(f"Failed to initialize HANA pool {pool_key}: {e}")
            return False
    
    @staticmethod
    @contextmanager
    def get_hana_connection(pool_key: str = 'default'):
        """Get HANA connection from pool (context manager)"""
        global _hana_connections
        
        if pool_key not in _hana_connections:
            from db_utils import load_hana_config
            config = load_hana_config()
            ConnectionPoolManager.init_hana_pool(config, pool_key)
        
        pool_info = _hana_connections[pool_key]
        config = pool_info['config']
        conn = None
        
        try:
            # Try to reuse existing connection
            if pool_info['connections']:
                conn = pool_info['connections'].pop()
                # Test if connection is still alive
                try:
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1 FROM DUMMY")
                    cursor.close()
                except:
                    # Connection is dead, create new one
                    try:
                        conn.close()
                    except:
                        pass
                    conn = None
            
            # Create new connection if needed
            if conn is None:
                conn = dbapi.connect(
                    address=config['host'],
                    port=config['port'],
                    user=config['username'],
                    password=config['password']
                )
            
            yield conn
            
            # Return connection to pool if pool not full
            if len(pool_info['connections']) < pool_info['max_size']:
                pool_info['connections'].append(conn)
            else:
                conn.close()
                
        except Exception as e:
            if conn:
                try:
                    conn.close()
                except:
                    pass
            raise
    
    @staticmethod
    def init_clickhouse_pool(config: Dict[str, Any], pool_key: str = 'default') -> bool:
        """Initialize ClickHouse connection pool"""
        global _clickhouse_clients
        
        try:
            with _pool_lock:
                if pool_key in _clickhouse_clients:
                    logger.info(f"ClickHouse pool {pool_key} already initialized")
                    return True
                
                from clickhouse_driver import Client
                
                # ClickHouse driver doesn't have built-in pooling
                # Store config for creating clients on demand
                _clickhouse_clients[pool_key] = {
                    'config': config,
                    'clients': [],
                    'max_size': CLICKHOUSE_POOL_SIZE
                }
                
                logger.info(f"ClickHouse connection manager initialized for {pool_key} (max={CLICKHOUSE_POOL_SIZE})")
                return True
                
        except Exception as e:
            logger.error(f"Failed to initialize ClickHouse pool {pool_key}: {e}")
            return False
    
    @staticmethod
    @contextmanager
    def get_clickhouse_client(pool_key: str = 'default'):
        """Get ClickHouse client from pool (context manager)"""
        global _clickhouse_clients
        
        if pool_key not in _clickhouse_clients:
            from db_utils import load_clickhouse_config
            config = load_clickhouse_config()
            ConnectionPoolManager.init_clickhouse_pool(config, pool_key)
        
        pool_info = _clickhouse_clients[pool_key]
        config = pool_info['config']
        client = None
        
        try:
            # Try to reuse existing client
            if pool_info['clients']:
                client = pool_info['clients'].pop()
                # Test if client is still alive
                try:
                    client.execute("SELECT 1")
                except:
                    # Client is dead, create new one
                    try:
                        client.disconnect()
                    except:
                        pass
                    client = None
            
            # Create new client if needed
            if client is None:
                from clickhouse_driver import Client
                client = Client(
                    host=config['host'],
                    port=config['port'],
                    user=config['user'],
                    password=config['password']
                )
            
            yield client
            
            # Return client to pool if pool not full
            if len(pool_info['clients']) < pool_info['max_size']:
                pool_info['clients'].append(client)
            else:
                try:
                    client.disconnect()
                except:
                    pass
                
        except Exception as e:
            if client:
                try:
                    client.disconnect()
                except:
                    pass
            raise
    
    @staticmethod
    def close_all_pools():
        """Close all connection pools (call on application shutdown)"""
        global _pg_pool, _pg_engine, _sql_server_engines, _hana_connections, _clickhouse_clients
        
        logger.info("Closing all connection pools...")
        
        with _pool_lock:
            # Close PostgreSQL pool
            if _pg_pool:
                try:
                    _pg_pool.closeall()
                    logger.info("PostgreSQL pool closed")
                except Exception as e:
                    logger.error(f"Error closing PostgreSQL pool: {e}")
                _pg_pool = None
            
            # Close PostgreSQL engine
            if _pg_engine:
                try:
                    _pg_engine.dispose()
                    logger.info("PostgreSQL engine closed")
                except Exception as e:
                    logger.error(f"Error closing PostgreSQL engine: {e}")
                _pg_engine = None
            
            # Close SQL Server engines
            for server_name, engine in _sql_server_engines.items():
                try:
                    engine.dispose()
                    logger.info(f"SQL Server engine closed for {server_name}")
                except Exception as e:
                    logger.error(f"Error closing SQL Server engine for {server_name}: {e}")
            _sql_server_engines.clear()
            
            # Close HANA connections
            for pool_key, pool_info in _hana_connections.items():
                try:
                    for conn in pool_info['connections']:
                        try:
                            conn.close()
                        except:
                            pass
                    logger.info(f"HANA pool {pool_key} closed")
                except Exception as e:
                    logger.error(f"Error closing HANA pool {pool_key}: {e}")
            _hana_connections.clear()
            
            # Close ClickHouse clients
            for pool_key, pool_info in _clickhouse_clients.items():
                try:
                    for client in pool_info['clients']:
                        try:
                            client.disconnect()
                        except:
                            pass
                    logger.info(f"ClickHouse pool {pool_key} closed")
                except Exception as e:
                    logger.error(f"Error closing ClickHouse pool {pool_key}: {e}")
            _clickhouse_clients.clear()
        
        logger.info("All connection pools closed")


# Convenience functions for backward compatibility
def get_pg_connection_pooled():
    """Get PostgreSQL connection from pool (replaces get_pg_connection)"""
    return ConnectionPoolManager.get_postgresql_connection()

def return_pg_connection_pooled(conn):
    """Return PostgreSQL connection to pool"""
    ConnectionPoolManager.return_postgresql_connection(conn)

@contextmanager
def pg_connection_context():
    """Context manager for PostgreSQL connections"""
    conn = None
    try:
        conn = ConnectionPoolManager.get_postgresql_connection()
        yield conn
    finally:
        if conn:
            ConnectionPoolManager.return_postgresql_connection(conn)

