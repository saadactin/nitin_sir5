-- SQL to fix API source configurations
-- Run this in PostgreSQL test1 database

-- Fix ID 26 (crm1): Wrong endpoint + missing database
UPDATE data_sources 
SET connection_details = jsonb_set(
    jsonb_set(connection_details, '{api_url}', '"http://localhost:3000/api/crm/stream"'),
    '{clickhouse_database}', '"test9"'
) 
WHERE id = 26;

-- Fix ID 27 (crm2): Missing database (jsonplaceholder)
UPDATE data_sources 
SET connection_details = jsonb_set(connection_details, '{clickhouse_database}', '"test9"')
WHERE id = 27;

-- Fix ID 28 (crm3 - CoinGecko): Missing database + add slow polling
UPDATE data_sources 
SET connection_details = jsonb_set(
    jsonb_set(
        jsonb_set(connection_details, '{clickhouse_database}', '"test9"'),
        '{polling_interval}', '60'
    ),
    '{is_sse}', 'false'  -- CoinGecko is REST API, not SSE
)
WHERE id = 28;

-- Verify changes
SELECT id, source_name, source_type, 
       connection_details->>'api_url' as api_url,
       connection_details->>'clickhouse_database' as target_db,
       connection_details->>'target_table' as target_table,
       connection_details->>'is_sse' as is_sse,
       connection_details->>'polling_interval' as polling_interval
FROM data_sources 
WHERE id IN (26, 27, 28)
ORDER BY id;
