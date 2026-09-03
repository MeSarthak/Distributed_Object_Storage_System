package database

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"distributed-storage/pkg/config"

	_ "github.com/lib/pq"
)

// DB wraps sql.DB with helper methods
type DB struct {
	*sql.DB
}

// Connect establishes a connection pool to PostgreSQL with retries
func Connect(cfg config.DatabaseConfig) (*DB, error) {
	dsn := cfg.DSN()
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres connection: %w", err)
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	// Retry ping up to 10 times with backoff (helps when containers boot concurrently)
	var pingErr error
	for attempts := 1; attempts <= 10; attempts++ {
		pingErr = db.Ping()
		if pingErr == nil {
			log.Printf("[DB] Connected to PostgreSQL at %s:%d/%s", cfg.Host, cfg.Port, cfg.Name)
			return &DB{DB: db}, nil
		}
		log.Printf("[DB] Waiting for database at %s:%d (attempt %d/10): %v", cfg.Host, cfg.Port, attempts, pingErr)
		time.Sleep(2 * time.Second)
	}

	return nil, fmt.Errorf("could not connect to postgres after 10 attempts: %w", pingErr)
}

// RunMigrations applies schema up migrations (convenience wrapper)
func (db *DB) RunMigrations(migrationPath string) error {
	return db.RunMigrationsUp(migrationPath)
}

// RunMigrationsUp executes schema up migrations to create tables and indexes
func (db *DB) RunMigrationsUp(migrationPath string) error {
	log.Printf("[DB] Running UP migrations...")

	var content []byte
	var err error

	pathsToTry := []string{
		migrationPath,
		"migrations/000001_init_schema.up.sql",
		"migrations/001_init_schema.sql",
		"/app/migrations/000001_init_schema.up.sql",
		"../../migrations/000001_init_schema.up.sql",
	}

	for _, p := range pathsToTry {
		if p == "" {
			continue
		}
		content, err = os.ReadFile(p)
		if err == nil {
			log.Printf("[DB] Loaded UP migration from file: %s", p)
			break
		}
	}

	if err != nil || len(content) == 0 {
		log.Printf("[DB] Using embedded fallback schema script")
		content = []byte(fallbackSchemaSQL)
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to start migration transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec(string(content)); err != nil {
		return fmt.Errorf("failed to execute UP migration: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit UP migration: %w", err)
	}

	log.Printf("[DB] UP migrations applied successfully.")
	return nil
}

// RunMigrationsDown rolls back the schema migrations
func (db *DB) RunMigrationsDown(migrationPath string) error {
	log.Printf("[DB] Running DOWN migrations...")

	var content []byte
	var err error

	pathsToTry := []string{
		migrationPath,
		"migrations/000001_init_schema.down.sql",
		"/app/migrations/000001_init_schema.down.sql",
		"../../migrations/000001_init_schema.down.sql",
	}

	for _, p := range pathsToTry {
		if p == "" {
			continue
		}
		content, err = os.ReadFile(p)
		if err == nil {
			log.Printf("[DB] Loaded DOWN migration from file: %s", p)
			break
		}
	}

	if err != nil || len(content) == 0 {
		log.Printf("[DB] Using embedded fallback rollback script")
		content = []byte(fallbackDownSQL)
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to start rollback transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec(string(content)); err != nil {
		return fmt.Errorf("failed to execute DOWN migration: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit DOWN migration: %w", err)
	}

	log.Printf("[DB] DOWN migrations applied successfully.")
	return nil
}

// CheckSchema verifies all 6 core tables exist
func (db *DB) CheckSchema() error {
	requiredTables := []string{
		"users",
		"objects",
		"storage_nodes",
		"replicas",
		"access_logs",
		"system_logs",
	}

	for _, table := range requiredTables {
		var exists bool
		query := `SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_schema = 'public' AND table_name = $1
		);`
		err := db.QueryRow(query, table).Scan(&exists)
		if err != nil {
			return fmt.Errorf("error verifying table %s: %w", table, err)
		}
		if !exists {
			return fmt.Errorf("required table %s does not exist", table)
		}
	}

	log.Printf("[DB] Verified all 6 core tables exist and are healthy.")
	return nil
}

const fallbackSchemaSQL = `
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS users (
    user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role VARCHAR(50) NOT NULL DEFAULT 'USER' CHECK (role IN ('USER', 'ADMIN')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);

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
`

const fallbackDownSQL = `
DROP TABLE IF EXISTS system_logs CASCADE;
DROP TABLE IF EXISTS access_logs CASCADE;
DROP TABLE IF EXISTS replicas CASCADE;
DROP TABLE IF EXISTS objects CASCADE;
DROP TABLE IF EXISTS storage_nodes CASCADE;
DROP TABLE IF EXISTS users CASCADE;
DROP EXTENSION IF EXISTS "pgcrypto";
`

