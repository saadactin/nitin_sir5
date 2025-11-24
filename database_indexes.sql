-- Performance Optimization: Database Indexes
-- Run this script in your PostgreSQL database to improve query performance

-- Indexes for sync_history table (most frequently queried)
CREATE INDEX IF NOT EXISTS idx_sync_history_sync_time 
    ON metrics_sync_tables.sync_history(sync_time DESC);

CREATE INDEX IF NOT EXISTS idx_sync_history_server_status 
    ON metrics_sync_tables.sync_history(server_name, status);

CREATE INDEX IF NOT EXISTS idx_sync_history_server_time 
    ON metrics_sync_tables.sync_history(server_name, sync_time DESC);

-- Indexes for data_sources table
CREATE INDEX IF NOT EXISTS idx_data_sources_active_type 
    ON data_sources(is_active, source_type) WHERE is_active = true;

CREATE INDEX IF NOT EXISTS idx_data_sources_created_at 
    ON data_sources(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_data_sources_source_type 
    ON data_sources(source_type) WHERE is_active = true;

-- Analyze tables to update statistics
ANALYZE metrics_sync_tables.sync_history;
ANALYZE data_sources;

-- Check index usage (run after some time to verify indexes are being used)
-- SELECT schemaname, tablename, indexname, idx_scan, idx_tup_read, idx_tup_fetch
-- FROM pg_stat_user_indexes
-- WHERE schemaname = 'metrics_sync_tables' OR tablename = 'data_sources'
-- ORDER BY idx_scan DESC;

