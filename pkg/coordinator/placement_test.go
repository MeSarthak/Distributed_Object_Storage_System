package coordinator

import (
	"context"
	"testing"

	"distributed-storage/pkg/config"
	"distributed-storage/pkg/types"

	"github.com/google/uuid"
)

// ---------------------------------------------------------------------------
// Test doubles (in-memory fakes) for the placement engine tests
// ---------------------------------------------------------------------------

// fakeNodeReader implements NodeReader for unit tests without PostgreSQL.
type fakeNodeReader struct {
	nodes []types.StorageNode
}

func (f *fakeNodeReader) GetOnlineNodes() ([]types.StorageNode, error) {
	var online []types.StorageNode
	for _, n := range f.nodes {
		if n.Status == types.NodeStatusOnline {
			online = append(online, n)
		}
	}
	return online, nil
}

// ---------------------------------------------------------------------------
// Helper: standard 3-node cluster
// ---------------------------------------------------------------------------

func threeNodeCluster() []types.StorageNode {
	return []types.StorageNode{
		{
			NodeID:       uuid.MustParse("00000000-0000-0000-0000-000000000001"),
			Hostname:     "node-1",
			TotalStorage: 10 * 1024 * 1024 * 1024,
			UsedStorage:  1 * 1024 * 1024 * 1024,
			CPUUsage:     20.0,
			MemoryUsage:  30.0,
			Latency:      5.0,
			Status:       types.NodeStatusOnline,
		},
		{
			NodeID:       uuid.MustParse("00000000-0000-0000-0000-000000000002"),
			Hostname:     "node-2",
			TotalStorage: 10 * 1024 * 1024 * 1024,
			UsedStorage:  5 * 1024 * 1024 * 1024,
			CPUUsage:     50.0,
			MemoryUsage:  60.0,
			Latency:      20.0,
			Status:       types.NodeStatusOnline,
		},
		{
			NodeID:       uuid.MustParse("00000000-0000-0000-0000-000000000003"),
			Hostname:     "node-3",
			TotalStorage: 10 * 1024 * 1024 * 1024,
			UsedStorage:  2 * 1024 * 1024 * 1024,
			CPUUsage:     15.0,
			MemoryUsage:  20.0,
			Latency:      3.0,
			Status:       types.NodeStatusOnline,
		},
	}
}

func defaultPlacementCfg() config.PlacementConfig {
	return config.PlacementConfig{
		Strategy:            string(types.PlacementStrategyWeighted),
		WeightStorage:       0.35,
		WeightCPU:           0.20,
		WeightRAM:           0.15,
		WeightLatency:       0.15,
		WeightHealth:        0.15,
		MaxCPUThreshold:     90.0,
		MaxRAMThreshold:     90.0,
		MinFreeStorageBytes: 104857600, // 100 MB
	}
}

// ---------------------------------------------------------------------------
// Tests running directly against production PlacementEngine
// ---------------------------------------------------------------------------

// TestPlacementSelectsHealthyNodes verifies that SelectNodes returns online
// nodes and rejects offline ones.
func TestPlacementSelectsHealthyNodes(t *testing.T) {
	nodes := threeNodeCluster()
	// Make node-2 offline.
	nodes[1].Status = types.NodeStatusOffline

	engine := NewPlacementEngine(&fakeNodeReader{nodes: nodes}, defaultPlacementCfg())
	selected, err := engine.SelectNodes(context.Background(), 2, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(selected) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(selected))
	}
	for _, n := range selected {
		if n.Status != types.NodeStatusOnline {
			t.Errorf("selected an offline node: %s", n.Hostname)
		}
	}
}

// TestPlacementExcludesOfflineNode ensures an explicitly failed/offline node is
// never chosen even when it appears online in the list.
func TestPlacementExcludesOfflineNode(t *testing.T) {
	nodes := threeNodeCluster()
	excludeID := nodes[0].NodeID // node-1

	engine := NewPlacementEngine(&fakeNodeReader{nodes: nodes}, defaultPlacementCfg())
	selected, err := engine.SelectNodes(context.Background(), 1, []uuid.UUID{excludeID})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, n := range selected {
		if n.NodeID == excludeID {
			t.Errorf("excluded node %s was still selected", n.Hostname)
		}
	}
}

// TestPlacementExcludesCPUOverloadedNode ensures nodes with CPU > threshold
// are rejected.
func TestPlacementExcludesCPUOverloadedNode(t *testing.T) {
	nodes := threeNodeCluster()
	// Overload node-2 CPU.
	nodes[1].CPUUsage = 95.0

	cfg := defaultPlacementCfg()
	engine := NewPlacementEngine(&fakeNodeReader{nodes: nodes}, cfg)

	selected, err := engine.SelectNodes(context.Background(), 2, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, n := range selected {
		if n.NodeID == nodes[1].NodeID {
			t.Errorf("overloaded CPU node %s should have been excluded", n.Hostname)
		}
	}
}

// TestPlacementExcludesLowStorageNode ensures nodes with less than
// MinFreeStorageBytes available are excluded.
func TestPlacementExcludesLowStorageNode(t *testing.T) {
	nodes := threeNodeCluster()
	// Fill node-1 nearly completely.
	nodes[0].UsedStorage = nodes[0].TotalStorage - 1024 // only 1 KB free

	engine := NewPlacementEngine(&fakeNodeReader{nodes: nodes}, defaultPlacementCfg())
	selected, err := engine.SelectNodes(context.Background(), 2, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, n := range selected {
		if n.NodeID == nodes[0].NodeID {
			t.Errorf("low-storage node %s should have been excluded", n.Hostname)
		}
	}
}

// TestPlacementInsufficientNodes ensures an error is returned when there are
// not enough eligible nodes.
func TestPlacementInsufficientNodes(t *testing.T) {
	nodes := threeNodeCluster()
	// Exclude all nodes.
	excludeIDs := []uuid.UUID{nodes[0].NodeID, nodes[1].NodeID, nodes[2].NodeID}

	engine := NewPlacementEngine(&fakeNodeReader{nodes: nodes}, defaultPlacementCfg())
	_, err := engine.SelectNodes(context.Background(), 1, excludeIDs)
	if err == nil {
		t.Error("expected error when no eligible nodes remain")
	}
}

// TestPlacementScoring verifies that the scoring formula uses the configured
// weights. A node with lower CPU, lower memory and more free space should
// outscore a node with higher resource utilisation.
func TestPlacementScoring(t *testing.T) {
	eng := &PlacementEngine{cfg: defaultPlacementCfg()}

	betterNode := types.StorageNode{
		TotalStorage: 10 * 1024 * 1024 * 1024,
		UsedStorage:  1 * 1024 * 1024 * 1024,
		CPUUsage:     5.0,
		MemoryUsage:  10.0,
		Latency:      2.0,
		Status:       types.NodeStatusOnline,
	}
	worseNode := types.StorageNode{
		TotalStorage: 10 * 1024 * 1024 * 1024,
		UsedStorage:  8 * 1024 * 1024 * 1024,
		CPUUsage:     80.0,
		MemoryUsage:  75.0,
		Latency:      200.0,
		Status:       types.NodeStatusOnline,
	}

	better := eng.scoreNode(betterNode)
	worse := eng.scoreNode(worseNode)

	if better <= worse {
		t.Errorf("expected betterNode (score %.4f) to outscore worseNode (score %.4f)", better, worse)
	}
}

// TestPlacementExcludesMultipleNodes verifies that providing multiple node IDs
// to exclude all causes them to be skipped.
func TestPlacementExcludesMultipleNodes(t *testing.T) {
	nodes := threeNodeCluster()
	// Exclude node-1 and node-3; only node-2 should be left.
	excludeIDs := []uuid.UUID{nodes[0].NodeID, nodes[2].NodeID}

	engine := NewPlacementEngine(&fakeNodeReader{nodes: nodes}, defaultPlacementCfg())
	selected, err := engine.SelectNodes(context.Background(), 1, excludeIDs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(selected) != 1 {
		t.Fatalf("expected 1 node, got %d", len(selected))
	}
	if selected[0].NodeID != nodes[1].NodeID {
		t.Errorf("expected node-2, got %s", selected[0].Hostname)
	}
}

// TestPlacementStrategyLeastLoaded verifies that least_loaded strategy selects
// the node with minimal combined CPU and memory load.
func TestPlacementStrategyLeastLoaded(t *testing.T) {
	nodes := threeNodeCluster()
	// node-1: CPU 20 + Mem 30 = 50
	// node-2: CPU 50 + Mem 60 = 110
	// node-3: CPU 15 + Mem 20 = 35 -> least loaded!

	cfg := defaultPlacementCfg()
	cfg.Strategy = string(types.PlacementStrategyLeastLoaded)

	engine := NewPlacementEngine(&fakeNodeReader{nodes: nodes}, cfg)
	selected, err := engine.SelectNodes(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(selected) != 1 {
		t.Fatalf("expected 1 node, got %d", len(selected))
	}
	if selected[0].NodeID != nodes[2].NodeID {
		t.Errorf("expected node-3 (least loaded), got %s", selected[0].Hostname)
	}
}

// TestPlacementStrategyRoundRobin verifies that round_robin strategy distributes
// node selection sequentially across calls.
func TestPlacementStrategyRoundRobin(t *testing.T) {
	nodes := threeNodeCluster()
	// Node hostnames: node-1, node-2, node-3

	cfg := defaultPlacementCfg()
	cfg.Strategy = string(types.PlacementStrategyRoundRobin)

	engine := NewPlacementEngine(&fakeNodeReader{nodes: nodes}, cfg)

	first, err := engine.SelectNodes(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("first select failed: %v", err)
	}
	second, err := engine.SelectNodes(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("second select failed: %v", err)
	}
	third, err := engine.SelectNodes(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("third select failed: %v", err)
	}

	if first[0].Hostname != "node-1" {
		t.Errorf("expected first round-robin to be node-1, got %s", first[0].Hostname)
	}
	if second[0].Hostname != "node-2" {
		t.Errorf("expected second round-robin to be node-2, got %s", second[0].Hostname)
	}
	if third[0].Hostname != "node-3" {
		t.Errorf("expected third round-robin to be node-3, got %s", third[0].Hostname)
	}
}
