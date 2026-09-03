-- ==============================================================================
-- Migration: 000001_init_schema.down.sql
-- Description: Drop all tables, indexes, and extensions created in 000001_init_schema.up.sql
-- ==============================================================================

-- Drop tables in reverse order of dependencies
DROP TABLE IF EXISTS system_logs CASCADE;
DROP TABLE IF EXISTS access_logs CASCADE;
DROP TABLE IF EXISTS replicas CASCADE;
DROP TABLE IF EXISTS objects CASCADE;
DROP TABLE IF EXISTS storage_nodes CASCADE;
DROP TABLE IF EXISTS users CASCADE;

-- Optional: Drop pgcrypto extension if no other database dependencies exist
DROP EXTENSION IF EXISTS "pgcrypto";
