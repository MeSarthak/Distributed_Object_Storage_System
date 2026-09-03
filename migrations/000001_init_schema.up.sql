-- ==============================================================================
-- Migration: 000001_init_schema.up.sql
-- Description: Create core schema for Distributed Object Storage System
-- Entities: users, storage_nodes, objects, replicas, access_logs, system_logs
-- ==============================================================================

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- 1. Users Table
CREATE TABLE IF NOT EXISTS users (
    user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role VARCHAR(50) NOT NULL DEFAULT 'USER' CHECK (role IN ('USER', 'ADMIN')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);

-- 2. Storage Nodes Table
CREATE TABLE IF NOT EXISTS storage_nodes (
    node_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    hostname VARCHAR(255) NOT NULL UNIQUE,
    ip_address VARCHAR(255) NOT NULL,
    total_storage BIGINT NOT NULL DEFAULT 0 CHECK (total_storage >= 0),
    used_storage BIGINT NOT NULL DEFAULT 0 CHECK (used_storage >= 0),
    cpu_usage DOUBLE PRECISION NOT NULL DEFAULT 0.0 CHECK (cpu_usage >= 0.0 AND cpu_usage <= 100.0),
    memory_usage DOUBLE PRECISION NOT NULL DEFAULT 0.0 CHECK (memory_usage >= 0.0 AND memory_usage <= 100.0),
    latency DOUBLE PRECISION NOT NULL DEFAULT 0.0 CHECK (latency >= 0.0),
    status VARCHAR(50) NOT NULL DEFAULT 'ONLINE' CHECK (status IN ('ONLINE', 'OFFLINE', 'DEGRADED')),
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_storage_nodes_status ON storage_nodes(status);
CREATE INDEX IF NOT EXISTS idx_storage_nodes_heartbeat ON storage_nodes(last_heartbeat);

-- 3. Objects Table
CREATE TABLE IF NOT EXISTS objects (
    object_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    object_name VARCHAR(512) NOT NULL,
    file_size BIGINT NOT NULL CHECK (file_size >= 0),
    mime_type VARCHAR(128) NOT NULL DEFAULT 'application/octet-stream',
    checksum CHAR(64) NOT NULL,
    upload_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_accessed TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    replication_factor INT NOT NULL DEFAULT 3 CHECK (replication_factor >= 1)
);

CREATE INDEX IF NOT EXISTS idx_objects_owner ON objects(owner_id);
CREATE INDEX IF NOT EXISTS idx_objects_name ON objects(object_name);
CREATE INDEX IF NOT EXISTS idx_objects_last_accessed ON objects(last_accessed);

-- 4. Replicas Table
CREATE TABLE IF NOT EXISTS replicas (
    replica_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    object_id UUID NOT NULL REFERENCES objects(object_id) ON DELETE CASCADE,
    node_id UUID NOT NULL REFERENCES storage_nodes(node_id) ON DELETE CASCADE,
    status VARCHAR(50) NOT NULL DEFAULT 'HEALTHY' CHECK (status IN ('HEALTHY', 'RECOVERING', 'LOST')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (object_id, node_id)
);

CREATE INDEX IF NOT EXISTS idx_replicas_object ON replicas(object_id);
CREATE INDEX IF NOT EXISTS idx_replicas_node ON replicas(node_id);
CREATE INDEX IF NOT EXISTS idx_replicas_status ON replicas(status);

-- 5. Access Logs Table
CREATE TABLE IF NOT EXISTS access_logs (
    access_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    object_id UUID REFERENCES objects(object_id) ON DELETE SET NULL,
    user_id UUID REFERENCES users(user_id) ON DELETE SET NULL,
    access_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    response_time INT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_access_logs_object ON access_logs(object_id);
CREATE INDEX IF NOT EXISTS idx_access_logs_user ON access_logs(user_id);
CREATE INDEX IF NOT EXISTS idx_access_logs_time ON access_logs(access_time);

-- 6. System Logs Table
CREATE TABLE IF NOT EXISTS system_logs (
    log_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type VARCHAR(100) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    description TEXT NOT NULL,
    severity VARCHAR(20) NOT NULL DEFAULT 'INFO' CHECK (severity IN ('DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL')),
    metadata JSONB DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_system_logs_event_type ON system_logs(event_type);
CREATE INDEX IF NOT EXISTS idx_system_logs_severity ON system_logs(severity);
CREATE INDEX IF NOT EXISTS idx_system_logs_timestamp ON system_logs(timestamp DESC);
