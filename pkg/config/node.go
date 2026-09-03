package config

import (
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

// StorageNodeConfig holds configuration for an independent storage node service
type StorageNodeConfig struct {
	NodeID            uuid.UUID
	Hostname          string
	IPAddress         string
	Port              int
	StoragePath       string
	CapacityBytes     int64
	CoordinatorURL    string
	HeartbeatInterval time.Duration
}

// LoadStorageNodeConfig reads storage node configuration from environment variables
func LoadStorageNodeConfig() (*StorageNodeConfig, error) {
	_ = godotenv.Load()

	nodeIDStr := os.Getenv("NODE_ID")
	var nodeID uuid.UUID
	var err error
	if nodeIDStr != "" {
		nodeID, err = uuid.Parse(nodeIDStr)
		if err != nil {
			return nil, fmt.Errorf("invalid NODE_ID UUID: %w", err)
		}
	} else {
		// Generate deterministically or dynamically
		nodeID = uuid.New()
	}

	hostname := getEnvStr("NODE_HOSTNAME", "storage-node-1")

	cfg := &StorageNodeConfig{
		NodeID:            nodeID,
		Hostname:          hostname,
		IPAddress:         getEnvStr("NODE_IP", "0.0.0.0"),
		Port:              getEnvInt("NODE_PORT", 9001),
		StoragePath:       getEnvStr("NODE_STORAGE_PATH", "./data/chunks"),
		CapacityBytes:     getEnvInt64("NODE_CAPACITY_BYTES", 10*1024*1024*1024), // 10 GB
		CoordinatorURL:    getEnvStr("NODE_COORDINATOR_URL", "http://coordinator:8080"),
		HeartbeatInterval: time.Duration(getEnvInt("HEARTBEAT_INTERVAL_SECONDS", 5)) * time.Second,
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// Validate ensures storage node config rules are met
func (n *StorageNodeConfig) Validate() error {
	if n.Port <= 0 || n.Port > 65535 {
		return fmt.Errorf("invalid storage node port: %d", n.Port)
	}
	if n.Hostname == "" {
		return fmt.Errorf("storage node hostname cannot be empty")
	}
	if n.StoragePath == "" {
		return fmt.Errorf("storage path cannot be empty")
	}
	if n.CapacityBytes <= 0 {
		return fmt.Errorf("capacity bytes must be positive")
	}
	if n.CoordinatorURL == "" {
		return fmt.Errorf("coordinator URL cannot be empty")
	}
	return nil
}
