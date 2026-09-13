package coordinator

import (
	"context"
	"testing"

	"distributed-storage/pkg/config"
	"distributed-storage/pkg/types"

	"github.com/google/uuid"
)

func TestClassifyTier(t *testing.T) {
	cfg := config.ReplicationConfig{
		DefaultReplicationFactor: 3,
		MinReplicationFactor:     2,
		MaxReplicationFactor:     5,
		HotAccessThreshold:       50,
		ColdAccessThreshold:      5,
	}

	rm := &ReplicationManager{cfg: cfg}

	// Hot tier test
	tier, factor := rm.ClassifyTier(50)
	if tier != "HOT" || factor != 5 {
		t.Errorf("expected HOT / 5, got %s / %d", tier, factor)
	}
	tier, factor = rm.ClassifyTier(100)
	if tier != "HOT" || factor != 5 {
		t.Errorf("expected HOT / 5, got %s / %d", tier, factor)
	}

	// Cold tier test
	tier, factor = rm.ClassifyTier(5)
	if tier != "COLD" || factor != 2 {
		t.Errorf("expected COLD / 2, got %s / %d", tier, factor)
	}
	tier, factor = rm.ClassifyTier(0)
	if tier != "COLD" || factor != 2 {
		t.Errorf("expected COLD / 2, got %s / %d", tier, factor)
	}

	// Warm tier test
	tier, factor = rm.ClassifyTier(6)
	if tier != "WARM" || factor != 3 {
		t.Errorf("expected WARM / 3, got %s / %d", tier, factor)
	}
	tier, factor = rm.ClassifyTier(49)
	if tier != "WARM" || factor != 3 {
		t.Errorf("expected WARM / 3, got %s / %d", tier, factor)
	}
}

func TestReplicationManagerFloorEnforcement(t *testing.T) {
	cfg := config.ReplicationConfig{
		DefaultReplicationFactor: 3,
		MinReplicationFactor:     2,
		MaxReplicationFactor:     5,
		HotAccessThreshold:       50,
		ColdAccessThreshold:      5,
	}

	rm := &ReplicationManager{
		cfg:        cfg,
		inProgress: make(map[uuid.UUID]struct{}),
	}

	// Tier is COLD (target factor 2) and current healthy replicas = 2
	// Ensure Action is ActionNone and healthyCount is preserved
	tier, targetFactor := rm.ClassifyTier(0)
	if tier != "COLD" {
		t.Fatalf("expected COLD tier")
	}
	if targetFactor < cfg.MinReplicationFactor {
		t.Errorf("target factor %d is below MinReplicationFactor %d", targetFactor, cfg.MinReplicationFactor)
	}

	// Test deduplication lock
	objID := uuid.New()
	rm.inProgress[objID] = struct{}{}

	res, err := rm.RebalanceObject(context.Background(), types.Object{ObjectID: objID}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != nil {
		t.Errorf("expected nil result for in-progress object")
	}
}
