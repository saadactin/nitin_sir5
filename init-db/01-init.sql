-- PostgreSQL Initialization Script for SQL Server to PostgreSQL Sync Application
-- This script is executed when the PostgreSQL container is first started

-- Create extensions that might be needed
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Set default timezone
SET timezone = 'UTC';

-- Create application-specific schema for metadata
CREATE SCHEMA IF NOT EXISTS metrics_sync_tables;

-- Grant permissions to the migration user
GRANT ALL PRIVILEGES ON SCHEMA metrics_sync_tables TO migration_user;
GRANT ALL PRIVILEGES ON DATABASE sync_db TO migration_user;

-- Create sync tracking tables (these will also be created by the application)
CREATE TABLE IF NOT EXISTS sync_database_status (
    server_name VARCHAR(100),
    database_name VARCHAR(100),
    last_full_sync TIMESTAMP,
    last_incremental_sync TIMESTAMP,
    sync_status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (server_name, database_name)
);

CREATE TABLE IF NOT EXISTS sync_table_status (
    server_name VARCHAR(100),
    database_name VARCHAR(100),
    schema_name VARCHAR(100),
    table_name VARCHAR(100),
    last_pk_value VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (server_name, database_name, schema_name, table_name)
);

-- Create application metadata tables
CREATE TABLE IF NOT EXISTS metrics_sync_tables.schedules (
    id SERIAL PRIMARY KEY,
    server_name TEXT NOT NULL,
    job_type TEXT NOT NULL,
    last_run TIMESTAMP,
    status TEXT,
    error TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS metrics_sync_tables.sync_history (
    id SERIAL PRIMARY KEY,
    server_name TEXT NOT NULL,
    sync_time TIMESTAMP DEFAULT NOW(),
    status TEXT NOT NULL,
    details TEXT
);

CREATE TABLE IF NOT EXISTS metrics_sync_tables.users (
    id SERIAL PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    password TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('admin', 'operator', 'viewer')),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Grant permissions on all tables to migration user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO migration_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA metrics_sync_tables TO migration_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO migration_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA metrics_sync_tables TO migration_user;

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_sync_db_status_server ON sync_database_status(server_name);
CREATE INDEX IF NOT EXISTS idx_sync_db_status_updated ON sync_database_status(updated_at);
CREATE INDEX IF NOT EXISTS idx_sync_table_status_server ON sync_table_status(server_name, database_name);
CREATE INDEX IF NOT EXISTS idx_sync_table_status_updated ON sync_table_status(updated_at);

CREATE INDEX IF NOT EXISTS idx_schedules_server ON metrics_sync_tables.schedules(server_name);
CREATE INDEX IF NOT EXISTS idx_sync_history_time ON metrics_sync_tables.sync_history(sync_time);
CREATE INDEX IF NOT EXISTS idx_users_username ON metrics_sync_tables.users(username);

-- Optimize PostgreSQL settings for this use case
ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';
ALTER SYSTEM SET log_statement = 'mod';
ALTER SYSTEM SET log_min_duration_statement = 1000;

-- Create a function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create triggers for automatic updated_at timestamps
CREATE TRIGGER update_sync_database_status_updated_at
    BEFORE UPDATE ON sync_database_status
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();

CREATE TRIGGER update_sync_table_status_updated_at
    BEFORE UPDATE ON sync_table_status
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();

-- Log completion
DO $$
BEGIN
    RAISE NOTICE 'SQL Server to PostgreSQL Sync database initialization completed successfully';
END $$;