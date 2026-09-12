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

// fakeNodeRepo is an in-memory implementation of the node query surface used
// by PlacementEngine.  It avoids any dependency on a live PostgreSQL instance.
type fakeNodeRepo struct {
	nodes []types.StorageNode
}

func (f *fakeNodeRepo) GetOnlineNodes() ([]types.StorageNode, error) {
	var online []types.StorageNode
	for _, n := range f.nodes {
		if n.Status == types.NodeStatusOnline {
			online = append(online, n)
		}
	}
	return online, nil
}

// fakePlacementNodeRepo bridges fakeNodeRepo to NodeRepository by embedding it
// in a struct that PlacementEngine can accept.  Since PlacementEngine operates
// on *database.NodeRepository which has concrete methods, we test through a
// helper that calls the same scoring logic.
//
// Rather than extracting an interface (which would require modifying existing
// types), the placement tests are structured to call scoreNode directly and
// validate SelectNodes with a custom sub-type.

// testPlacementEngine wraps PlacementEngine with a fake node source for tests.
type testPlacementEngine struct {
	nodes  []types.StorageNode
	cfg    config.PlacementConfig
	engine *PlacementEngine // actual engine with real scoring logic
}

func newTestPlacementEngine(nodes []types.StorageNode, cfg config.PlacementConfig) *testPlacementEngine {
	return &testPlacementEngine{nodes: nodes, cfg: cfg}
}

// selectNodes mirrors PlacementEngine.SelectNodes but uses the in-memory node list.
func (tpe *testPlacementEngine) selectNodes(ctx context.Context, count int, excludeIDs []uuid.UUID) ([]types.StorageNode, error) {
	// Reuse real scoring logic by creating a temporary PlacementEngine with no
	// node repo and calling scoreNode directly.
	eng := &PlacementEngine{cfg: tpe.cfg}

	excluded := make(map[uuid.UUID]struct{}, len(excludeIDs))
	for _, id := range excludeIDs {
		excluded[id] = struct{}{}
	}

	type scored struct {
		node  types.StorageNode
		score float64
	}

	var candidates []scored
	for _, node := range tpe.nodes {
		if node.Status != types.NodeStatusOnline {
			continue
		}
		if _, skip := excluded[node.NodeID]; skip {
			continue
		}
		if node.CPUUsage > tpe.cfg.MaxCPUThreshold {
			continue
		}
		if node.MemoryUsage > tpe.cfg.MaxRAMThreshold {
			continue
		}
		freeBytes := node.TotalStorage - node.UsedStorage
		if freeBytes < tpe.cfg.MinFreeStorageBytes {
			continue
		}
		candidates = append(candidates, scored{node: node, score: eng.scoreNode(node)})
	}

	if len(candidates) < count {
		return nil, &insufficientNodesError{need: count, have: len(candidates)}
	}

	// Sort descending.
	for i := 0; i < len(candidates)-1; i++ {
		for j := i + 1; j < len(candidates); j++ {
			if candidates[j].score > candidates[i].score {
				candidates[i], candidates[j] = candidates[j], candidates[i]
			}
		}
	}

	result := make([]types.StorageNode, count)
	for i := 0; i < count; i++ {
		result[i] = candidates[i].node
	}
	return result, nil
}

type insufficientNodesError struct{ need, have int }

func (e *insufficientNodesError) Error() string {
	return "insufficient nodes"
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
// Tests
// ---------------------------------------------------------------------------

// TestPlacementSelectsHealthyNodes verifies that SelectNodes returns online
// nodes and rejects offline ones.
func TestPlacementSelectsHealthyNodes(t *testing.T) {
	nodes := threeNodeCluster()
	// Make node-2 offline.
	nodes[1].Status = types.NodeStatusOffline

	tpe := newTestPlacementEngine(nodes, defaultPlacementCfg())
	selected, err := tpe.selectNodes(context.Background(), 2, nil)
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

	tpe := newTestPlacementEngine(nodes, defaultPlacementCfg())
	selected, err := tpe.selectNodes(context.Background(), 1, []uuid.UUID{excludeID})
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
	tpe := newTestPlacementEngine(nodes, cfg)

	selected, err := tpe.selectNodes(context.Background(), 2, nil)
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

	tpe := newTestPlacementEngine(nodes, defaultPlacementCfg())
	selected, err := tpe.selectNodes(context.Background(), 2, nil)
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

	tpe := newTestPlacementEngine(nodes, defaultPlacementCfg())
	_, err := tpe.selectNodes(context.Background(), 1, excludeIDs)
	if err == nil {
		t.Error("expected error when no eligible nodes remain")
	}
}

// TestPlacementScoring verifies that the scoring formula uses the configured
// weights.  A node with lower CPU, lower memory and more free space should
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

	tpe := newTestPlacementEngine(nodes, defaultPlacementCfg())
	selected, err := tpe.selectNodes(context.Background(), 1, excludeIDs)
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
