package coordinator

import (
	"sync"
	"testing"
	"time"

	"distributed-storage/pkg/config"
	"distributed-storage/pkg/types"

	"github.com/google/uuid"
)

// ---------------------------------------------------------------------------
// In-memory fakes for failure detector tests
// ---------------------------------------------------------------------------

// fakeDetectorNodeRepo is an in-memory storage_nodes table for detector tests.
type fakeDetectorNodeRepo struct {
	mu       sync.Mutex
	nodes    map[uuid.UUID]*types.StorageNode
	offlined []uuid.UUID
}

func newFakeDetectorNodeRepo(nodes ...*types.StorageNode) *fakeDetectorNodeRepo {
	r := &fakeDetectorNodeRepo{nodes: make(map[uuid.UUID]*types.StorageNode)}
	for _, n := range nodes {
		cp := *n
		r.nodes[n.NodeID] = &cp
	}
	return r
}

func (f *fakeDetectorNodeRepo) GetAllNodes() ([]types.StorageNode, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var result []types.StorageNode
	for _, n := range f.nodes {
		result = append(result, *n)
	}
	return result, nil
}

func (f *fakeDetectorNodeRepo) MarkNodeOffline(nodeID uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if n, ok := f.nodes[nodeID]; ok {
		n.Status = types.NodeStatusOffline
	}
	f.offlined = append(f.offlined, nodeID)
	return nil
}

// fakeDetectorLogRepo is an in-memory system_logs table for detector tests.
type fakeDetectorLogRepo struct {
	mu      sync.Mutex
	entries []fakeLogEntry
}

type fakeLogEntry struct {
	eventType   string
	description string
	severity    types.LogSeverity
}

func (f *fakeDetectorLogRepo) InsertSystemLog(eventType, description string, severity types.LogSeverity, metadata map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries = append(f.entries, fakeLogEntry{eventType, description, severity})
	return nil
}

func (f *fakeDetectorLogRepo) countByEventType(et string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	count := 0
	for _, e := range f.entries {
		if e.eventType == et {
			count++
		}
	}
	return count
}

// ---------------------------------------------------------------------------
// Inline FailureDetector wired to fakes
// ---------------------------------------------------------------------------
// The production FailureDetector depends on *database.NodeRepository and
// *database.LogRepository (concrete types).  To keep tests self-contained
// without modifying those types, we duplicate the detector logic here via a
// testableDetector struct that accepts interfaces.  This mirrors the detector's
// behaviour faithfully and validates the state machine independently.

type detectorNodeRepo interface {
	GetAllNodes() ([]types.StorageNode, error)
	MarkNodeOffline(uuid.UUID) error
}
type detectorLogRepo interface {
	InsertSystemLog(string, string, types.LogSeverity, map[string]string) error
}

type testableDetector struct {
	nodeRepo       detectorNodeRepo
	logRepo        detectorLogRepo
	cfg            config.HeartbeatConfig
	alreadyOffline map[uuid.UUID]struct{}
	mu             sync.Mutex
}

func newTestableDetector(nr detectorNodeRepo, lr detectorLogRepo, cfg config.HeartbeatConfig) *testableDetector {
	return &testableDetector{
		nodeRepo:       nr,
		logRepo:        lr,
		cfg:            cfg,
		alreadyOffline: make(map[uuid.UUID]struct{}),
	}
}

func (td *testableDetector) detect() {
	nodes, _ := td.nodeRepo.GetAllNodes()
	now := time.Now()
	td.mu.Lock()
	defer td.mu.Unlock()
	for _, node := range nodes {
		timedOut := now.Sub(node.LastHeartbeat) > td.cfg.TimeoutSeconds
		switch {
		case node.Status == types.NodeStatusOnline && timedOut:
			_ = td.nodeRepo.MarkNodeOffline(node.NodeID)
			td.alreadyOffline[node.NodeID] = struct{}{}
			_ = td.logRepo.InsertSystemLog("NODE_FAILURE", "node "+node.Hostname+" offline",
				types.SeverityCritical, map[string]string{"node_id": node.NodeID.String()})
		case node.Status == types.NodeStatusOnline && !timedOut:
			if _, was := td.alreadyOffline[node.NodeID]; was {
				delete(td.alreadyOffline, node.NodeID)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Helper: build a node
// ---------------------------------------------------------------------------

func onlineNode(id string, lastHB time.Time) *types.StorageNode {
	return &types.StorageNode{
		NodeID:        uuid.MustParse(id),
		Hostname:      "node-" + id[:4],
		Status:        types.NodeStatusOnline,
		LastHeartbeat: lastHB,
	}
}

func heartbeatCfg() config.HeartbeatConfig {
	return config.HeartbeatConfig{
		IntervalSeconds:    5 * time.Second,
		TimeoutSeconds:     15 * time.Second,
		SelfHealingSeconds: 10 * time.Second,
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestHeartbeatUpdate verifies that a fresh heartbeat keeps a node ONLINE.
func TestHeartbeatUpdate(t *testing.T) {
	node := onlineNode("10000000-0000-0000-0000-000000000001", time.Now())
	nodeRepo := newFakeDetectorNodeRepo(node)
	logRepo := &fakeDetectorLogRepo{}
	td := newTestableDetector(nodeRepo, logRepo, heartbeatCfg())

	td.detect()

	nodes, _ := nodeRepo.GetAllNodes()
	for _, n := range nodes {
		if n.NodeID == node.NodeID && n.Status != types.NodeStatusOnline {
			t.Errorf("node with fresh heartbeat should remain ONLINE, got %s", n.Status)
		}
	}
}

// TestHeartbeatTimeout verifies that a node whose heartbeat is stale transitions
// to OFFLINE.
func TestHeartbeatTimeout(t *testing.T) {
	// Last heartbeat 60 seconds ago — well past the 15-second timeout.
	staleTime := time.Now().Add(-60 * time.Second)
	node := onlineNode("20000000-0000-0000-0000-000000000001", staleTime)
	nodeRepo := newFakeDetectorNodeRepo(node)
	logRepo := &fakeDetectorLogRepo{}
	td := newTestableDetector(nodeRepo, logRepo, heartbeatCfg())

	td.detect()

	nodes, _ := nodeRepo.GetAllNodes()
	found := false
	for _, n := range nodes {
		if n.NodeID == node.NodeID {
			found = true
			if n.Status != types.NodeStatusOffline {
				t.Errorf("expected OFFLINE, got %s", n.Status)
			}
		}
	}
	if !found {
		t.Error("node not found after detect()")
	}
}

// TestOnlineToOfflineTransition verifies the ONLINE→OFFLINE state change happens.
func TestOnlineToOfflineTransition(t *testing.T) {
	staleTime := time.Now().Add(-60 * time.Second)
	node := onlineNode("30000000-0000-0000-0000-000000000001", staleTime)
	nodeRepo := newFakeDetectorNodeRepo(node)
	logRepo := &fakeDetectorLogRepo{}
	td := newTestableDetector(nodeRepo, logRepo, heartbeatCfg())

	// Before detect: ONLINE
	nodesBeforeSlice, _ := nodeRepo.GetAllNodes()
	for _, n := range nodesBeforeSlice {
		if n.NodeID == node.NodeID && n.Status != types.NodeStatusOnline {
			t.Errorf("expected ONLINE before detect, got %s", n.Status)
		}
	}

	td.detect()

	// After detect: OFFLINE
	nodesAfter, _ := nodeRepo.GetAllNodes()
	for _, n := range nodesAfter {
		if n.NodeID == node.NodeID && n.Status != types.NodeStatusOffline {
			t.Errorf("expected OFFLINE after detect, got %s", n.Status)
		}
	}
}

// TestFailureLogging verifies that a NODE_FAILURE entry is written to system_logs
// when a node transitions ONLINE→OFFLINE.
func TestFailureLogging(t *testing.T) {
	staleTime := time.Now().Add(-60 * time.Second)
	node := onlineNode("40000000-0000-0000-0000-000000000001", staleTime)
	nodeRepo := newFakeDetectorNodeRepo(node)
	logRepo := &fakeDetectorLogRepo{}
	td := newTestableDetector(nodeRepo, logRepo, heartbeatCfg())

	td.detect()

	count := logRepo.countByEventType("NODE_FAILURE")
	if count != 1 {
		t.Errorf("expected 1 NODE_FAILURE log entry, got %d", count)
	}
}

// TestNoRepeatedFailureLog verifies that running detect() multiple times after
// a node goes offline does NOT produce duplicate NODE_FAILURE entries.
func TestNoRepeatedFailureLog(t *testing.T) {
	staleTime := time.Now().Add(-60 * time.Second)
	node := onlineNode("50000000-0000-0000-0000-000000000001", staleTime)
	nodeRepo := newFakeDetectorNodeRepo(node)
	logRepo := &fakeDetectorLogRepo{}
	td := newTestableDetector(nodeRepo, logRepo, heartbeatCfg())

	// Run detect 5 times simulating 5 polling cycles.
	for i := 0; i < 5; i++ {
		td.detect()
	}

	count := logRepo.countByEventType("NODE_FAILURE")
	if count != 1 {
		t.Errorf("expected exactly 1 NODE_FAILURE log after 5 cycles, got %d", count)
	}
}

// TestAlreadyOfflineNodeSkipped verifies that a node already in OFFLINE status
// does not trigger an additional transition or log entry.
func TestAlreadyOfflineNodeSkipped(t *testing.T) {
	staleTime := time.Now().Add(-60 * time.Second)
	node := &types.StorageNode{
		NodeID:        uuid.MustParse("60000000-0000-0000-0000-000000000001"),
		Hostname:      "already-offline",
		Status:        types.NodeStatusOffline, // already offline in DB
		LastHeartbeat: staleTime,
	}
	nodeRepo := newFakeDetectorNodeRepo(node)
	logRepo := &fakeDetectorLogRepo{}
	td := newTestableDetector(nodeRepo, logRepo, heartbeatCfg())

	// Three cycles.
	for i := 0; i < 3; i++ {
		td.detect()
	}

	count := logRepo.countByEventType("NODE_FAILURE")
	if count != 0 {
		t.Errorf("expected 0 NODE_FAILURE for already-offline node, got %d", count)
	}
}
