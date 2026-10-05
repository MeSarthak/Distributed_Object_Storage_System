package config

import (
	"os"
	"testing"
)

func TestCoordinatorConfigDefaultsAndValidation(t *testing.T) {
	// Clear relevant env vars to test defaults
	os.Unsetenv("COORDINATOR_PORT")
	os.Unsetenv("JWT_SECRET")
	os.Unsetenv("WEIGHT_STORAGE")

	cfg, err := LoadCoordinatorConfig()
	if err != nil {
		t.Fatalf("Failed to load default coordinator config: %v", err)
	}

	if cfg.Server.Port != 8080 {
		t.Errorf("expected default port 8080, got %d", cfg.Server.Port)
	}

	if cfg.Replication.DefaultReplicationFactor != 3 {
		t.Errorf("expected default replication factor 3, got %d", cfg.Replication.DefaultReplicationFactor)
	}

	// Test invalid weights
	t.Setenv("WEIGHT_STORAGE", "0.99")
	_, err = LoadCoordinatorConfig()
	if err == nil {
		t.Errorf("expected error when weights do not sum to 1.0, got nil")
	}

	// Test invalid JWT
	t.Setenv("WEIGHT_STORAGE", "0.35")
	t.Setenv("JWT_SECRET", "short")
	_, err = LoadCoordinatorConfig()
	if err == nil {
		t.Errorf("expected error when JWT secret is shorter than 16 chars, got nil")
	}

	// Test invalid placement strategy
	t.Setenv("JWT_SECRET", "super-secret-jwt-key-change-in-production-min-32-chars")
	t.Setenv("PLACEMENT_STRATEGY", "invalid_strategy")
	_, err = LoadCoordinatorConfig()
	if err == nil {
		t.Errorf("expected error for invalid placement strategy, got nil")
	}

	// Test invalid replication check interval
	t.Setenv("PLACEMENT_STRATEGY", "weighted")
	t.Setenv("REPLICATION_CHECK_INTERVAL_SECONDS", "0")
	_, err = LoadCoordinatorConfig()
	if err == nil {
		t.Errorf("expected error for non-positive check interval, got nil")
	}
	os.Unsetenv("REPLICATION_CHECK_INTERVAL_SECONDS")
}

func TestStorageNodeConfigDefaultsAndValidation(t *testing.T) {
	os.Unsetenv("NODE_PORT")
	os.Unsetenv("NODE_HOSTNAME")

	cfg, err := LoadStorageNodeConfig()
	if err != nil {
		t.Fatalf("Failed to load default storage node config: %v", err)
	}

	if cfg.Port != 9001 {
		t.Errorf("expected default port 9001, got %d", cfg.Port)
	}

	t.Setenv("NODE_PORT", "999999")
	_, err = LoadStorageNodeConfig()
	if err == nil {
		t.Errorf("expected error for invalid port, got nil")
	}
}
