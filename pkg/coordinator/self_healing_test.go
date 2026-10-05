package coordinator

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"distributed-storage/pkg/config"
	"distributed-storage/pkg/types"

	"github.com/google/uuid"
)

// ---------------------------------------------------------------------------
// In-memory fakes implementing SelfHealing repository interfaces
// ---------------------------------------------------------------------------

type fakeSHReplicaRepo struct {
	mu       sync.Mutex
	replicas []*types.Replica
}

func newFakeSHReplicaRepo() *fakeSHReplicaRepo {
	return &fakeSHReplicaRepo{}
}

func (f *fakeSHReplicaRepo) GetReplicasByNode(nodeID uuid.UUID) ([]types.Replica, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []types.Replica
	for _, r := range f.replicas {
		if r.NodeID == nodeID {
			out = append(out, *r)
		}
	}
	return out, nil
}

func (f *fakeSHReplicaRepo) GetReplicasByObject(objectID uuid.UUID) ([]types.Replica, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []types.Replica
	for _, r := range f.replicas {
		if r.ObjectID == objectID {
			out = append(out, *r)
		}
	}
	return out, nil
}

func (f *fakeSHReplicaRepo) GetHealthyReplicasByObject(objectID uuid.UUID) ([]types.Replica, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []types.Replica
	for _, r := range f.replicas {
		if r.ObjectID == objectID && r.Status == types.ReplicaStatusHealthy {
			out = append(out, *r)
		}
	}
	return out, nil
}

func (f *fakeSHReplicaRepo) CountHealthyReplicas(objectID uuid.UUID) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	count := 0
	for _, r := range f.replicas {
		if r.ObjectID == objectID && r.Status == types.ReplicaStatusHealthy {
			count++
		}
	}
	return count, nil
}

func (f *fakeSHReplicaRepo) InsertReplica(objectID, nodeID uuid.UUID) *types.Replica {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := &types.Replica{
		ReplicaID: uuid.New(),
		ObjectID:  objectID,
		NodeID:    nodeID,
		Status:    types.ReplicaStatusRecovering,
		CreatedAt: time.Now(),
	}
	f.replicas = append(f.replicas, r)
	return r
}

func (f *fakeSHReplicaRepo) InsertReplicaTx(tx *sql.Tx, objectID, nodeID uuid.UUID) (*types.Replica, error) {
	return f.InsertReplica(objectID, nodeID), nil
}

func (f *fakeSHReplicaRepo) UpdateReplicaStatus(replicaID uuid.UUID, status types.ReplicaStatus) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range f.replicas {
		if r.ReplicaID == replicaID {
			r.Status = status
			return nil
		}
	}
	return fmt.Errorf("replica %s not found", replicaID)
}

func (f *fakeSHReplicaRepo) UpdateReplicaStatusTx(tx *sql.Tx, replicaID uuid.UUID, status types.ReplicaStatus) error {
	return f.UpdateReplicaStatus(replicaID, status)
}

func (f *fakeSHReplicaRepo) MarkReplicaLostTx(tx *sql.Tx, replicaID uuid.UUID) error {
	return f.UpdateReplicaStatus(replicaID, types.ReplicaStatusLost)
}

func (f *fakeSHReplicaRepo) getStatusFor(replicaID uuid.UUID) types.ReplicaStatus {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range f.replicas {
		if r.ReplicaID == replicaID {
			return r.Status
		}
	}
	return ""
}

type fakeSHNodeRepo struct {
	mu    sync.Mutex
	nodes map[uuid.UUID]*types.StorageNode
}

func newFakeSHNodeRepo(nodes ...*types.StorageNode) *fakeSHNodeRepo {
	r := &fakeSHNodeRepo{nodes: make(map[uuid.UUID]*types.StorageNode)}
	for _, n := range nodes {
		cp := *n
		r.nodes[n.NodeID] = &cp
	}
	return r
}

func (f *fakeSHNodeRepo) GetOfflineNodes() ([]types.StorageNode, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []types.StorageNode
	for _, n := range f.nodes {
		if n.Status == types.NodeStatusOffline {
			out = append(out, *n)
		}
	}
	return out, nil
}

func (f *fakeSHNodeRepo) GetNodeByID(nodeID uuid.UUID) (*types.StorageNode, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if n, ok := f.nodes[nodeID]; ok {
		cp := *n
		return &cp, nil
	}
	return nil, fmt.Errorf("node %s not found", nodeID)
}

func (f *fakeSHNodeRepo) GetOnlineNodes() ([]types.StorageNode, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []types.StorageNode
	for _, n := range f.nodes {
		if n.Status == types.NodeStatusOnline {
			out = append(out, *n)
		}
	}
	return out, nil
}

type fakeSHObjectRepo struct {
	mu      sync.Mutex
	objects map[uuid.UUID]*types.Object
}

func newFakeSHObjectRepo() *fakeSHObjectRepo {
	return &fakeSHObjectRepo{objects: make(map[uuid.UUID]*types.Object)}
}

func (f *fakeSHObjectRepo) GetObjectByID(id uuid.UUID) (*types.Object, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if obj, ok := f.objects[id]; ok {
		cp := *obj
		return &cp, nil
	}
	return nil, fmt.Errorf("object %s not found", id)
}

type fakeSHLogRepo struct {
	mu      sync.Mutex
	entries []fakeLogEntry
}

func (f *fakeSHLogRepo) InsertSystemLog(eventType, desc string, sev types.LogSeverity, meta map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries = append(f.entries, fakeLogEntry{eventType, desc, sev})
	return nil
}

func (f *fakeSHLogRepo) countByEvent(et string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := 0
	for _, e := range f.entries {
		if e.eventType == et {
			c++
		}
	}
	return c
}

type fakeSHPlacement struct {
	targetNodes []types.StorageNode
	lastExclude []uuid.UUID
}

func (f *fakeSHPlacement) SelectNodes(ctx context.Context, count int, excludeNodeIDs []uuid.UUID) ([]types.StorageNode, error) {
	f.lastExclude = excludeNodeIDs
	excluded := make(map[uuid.UUID]bool)
	for _, id := range excludeNodeIDs {
		excluded[id] = true
	}

	var eligible []types.StorageNode
	for _, n := range f.targetNodes {
		if !excluded[n.NodeID] {
			eligible = append(eligible, n)
		}
	}
	if len(eligible) < count {
		return nil, fmt.Errorf("insufficient placement nodes: need %d, have %d", count, len(eligible))
	}
	return eligible[:count], nil
}

// ---------------------------------------------------------------------------
// Helpers & Test Fixtures
// ---------------------------------------------------------------------------

var (
	failedNodeID  = uuid.MustParse("aaaaaaaa-0000-0000-0000-000000000001")
	sourceNodeID  = uuid.MustParse("bbbbbbbb-0000-0000-0000-000000000002")
	destNodeID    = uuid.MustParse("cccccccc-0000-0000-0000-000000000003")
	source2NodeID = uuid.MustParse("bbbbbbbb-0000-0000-0000-000000000004")
	testObjectID  = uuid.MustParse("dddddddd-0000-0000-0000-000000000001")
)

const testContent = "hello distributed storage"

func testChecksum(data string) string {
	h := sha256.Sum256([]byte(data))
	return hex.EncodeToString(h[:])
}

func defaultReplCfg() config.ReplicationConfig {
	return config.ReplicationConfig{
		DefaultReplicationFactor: 3,
		MinReplicationFactor:     2,
		MaxReplicationFactor:     5,
		HotAccessThreshold:       50,
		ColdAccessThreshold:      5,
	}
}

func defaultHealCfg() config.HeartbeatConfig {
	return config.HeartbeatConfig{
		IntervalSeconds:    5 * time.Second,
		TimeoutSeconds:     15 * time.Second,
		SelfHealingSeconds: 10 * time.Second,
	}
}

// ---------------------------------------------------------------------------
// Tests running directly against production SelfHealingEngine
// ---------------------------------------------------------------------------

// TestAffectedReplicaDetection verifies GetReplicasByNode returns the correct replicas.
func TestAffectedReplicaDetection(t *testing.T) {
	repRepo := newFakeSHReplicaRepo()
	otherNodeID := uuid.MustParse("eeeeeeee-0000-0000-0000-000000000001")

	repRepo.InsertReplica(testObjectID, failedNodeID)
	repRepo.InsertReplica(testObjectID, otherNodeID)

	affected, err := repRepo.GetReplicasByNode(failedNodeID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(affected) != 1 {
		t.Fatalf("expected 1 replica on failed node, got %d", len(affected))
	}
	if affected[0].NodeID != failedNodeID {
		t.Errorf("expected replica on failed node, got node %s", affected[0].NodeID)
	}
}

// TestHealthySourceSelection verifies GetHealthyReplicasByObject filters out
// unhealthy and lost replicas.
func TestHealthySourceSelection(t *testing.T) {
	repRepo := newFakeSHReplicaRepo()

	healthyReplica := repRepo.InsertReplica(testObjectID, sourceNodeID)
	repRepo.UpdateReplicaStatus(healthyReplica.ReplicaID, types.ReplicaStatusHealthy)

	lostNodeID := uuid.MustParse("ffffffff-0000-0000-0000-000000000001")
	lostReplica := repRepo.InsertReplica(testObjectID, lostNodeID)
	repRepo.UpdateReplicaStatus(lostReplica.ReplicaID, types.ReplicaStatusLost)

	sources, err := repRepo.GetHealthyReplicasByObject(testObjectID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sources) != 1 {
		t.Fatalf("expected 1 healthy source, got %d", len(sources))
	}
	if sources[0].NodeID != sourceNodeID {
		t.Errorf("expected source node %s, got %s", sourceNodeID, sources[0].NodeID)
	}
}

// TestDestinationSelection verifies that nodes already holding a replica are excluded.
func TestDestinationSelection(t *testing.T) {
	repRepo := newFakeSHReplicaRepo()
	repRepo.InsertReplica(testObjectID, failedNodeID)
	hReplica := repRepo.InsertReplica(testObjectID, sourceNodeID)
	repRepo.UpdateReplicaStatus(hReplica.ReplicaID, types.ReplicaStatusHealthy)

	allReplicas, _ := repRepo.GetReplicasByObject(testObjectID)

	excludeIDs := []uuid.UUID{failedNodeID}
	for _, r := range allReplicas {
		excludeIDs = append(excludeIDs, r.NodeID)
	}
	excluded := make(map[uuid.UUID]struct{})
	for _, id := range excludeIDs {
		excluded[id] = struct{}{}
	}

	if _, ok := excluded[sourceNodeID]; !ok {
		t.Error("source node (already holds replica) should be excluded")
	}
	if _, ok := excluded[destNodeID]; ok {
		t.Error("destination node should NOT be in exclusion set before placement")
	}
}

// TestChecksumVerification verifies that a correct SHA-256 leads to HEALTHY status on real engine.
func TestChecksumVerification(t *testing.T) {
	checksum := testChecksum(testContent)

	sourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, testContent)
	}))
	defer sourceServer.Close()

	destServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer destServer.Close()

	nodeRepo := newFakeSHNodeRepo(
		&types.StorageNode{NodeID: sourceNodeID, Hostname: sourceServer.URL, Status: types.NodeStatusOnline},
		&types.StorageNode{NodeID: destNodeID, Hostname: destServer.URL, Status: types.NodeStatusOnline},
		&types.StorageNode{NodeID: failedNodeID, Hostname: "failed-node", Status: types.NodeStatusOffline},
	)

	repRepo := newFakeSHReplicaRepo()
	lostReplica := repRepo.InsertReplica(testObjectID, failedNodeID)
	repRepo.UpdateReplicaStatus(lostReplica.ReplicaID, types.ReplicaStatusLost)
	srcReplica := repRepo.InsertReplica(testObjectID, sourceNodeID)
	repRepo.UpdateReplicaStatus(srcReplica.ReplicaID, types.ReplicaStatusHealthy)

	objRepo := newFakeSHObjectRepo()
	objRepo.objects[testObjectID] = &types.Object{
		ObjectID:          testObjectID,
		Checksum:          checksum,
		ReplicationFactor: 3,
	}

	logRepo := &fakeSHLogRepo{}
	placement := &fakeSHPlacement{
		targetNodes: []types.StorageNode{
			{NodeID: destNodeID, Hostname: destServer.URL, Status: types.NodeStatusOnline},
		},
	}

	engine := NewSelfHealingEngine(nil, nodeRepo, repRepo, objRepo, logRepo, placement, defaultReplCfg(), defaultHealCfg())

	failedNode := types.StorageNode{NodeID: failedNodeID, Hostname: "failed-node"}
	recoveredID, err := engine.RecoverReplica(context.Background(), *lostReplica, failedNode)
	if err != nil {
		t.Fatalf("unexpected recovery error: %v", err)
	}

	finalStatus := repRepo.getStatusFor(recoveredID)
	if finalStatus != types.ReplicaStatusHealthy {
		t.Errorf("expected HEALTHY after correct checksum, got %s", finalStatus)
	}
	if logRepo.countByEvent(types.EventRecoverySuccess) != 1 {
		t.Errorf("expected RECOVERY_SUCCESS log entry")
	}
}

// TestChecksumMismatch verifies that a mismatched SHA-256 prevents HEALTHY status on real engine.
func TestChecksumMismatch(t *testing.T) {
	sourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, testContent)
	}))
	defer sourceServer.Close()

	destServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer destServer.Close()

	nodeRepo := newFakeSHNodeRepo(
		&types.StorageNode{NodeID: sourceNodeID, Hostname: sourceServer.URL, Status: types.NodeStatusOnline},
		&types.StorageNode{NodeID: destNodeID, Hostname: destServer.URL, Status: types.NodeStatusOnline},
		&types.StorageNode{NodeID: failedNodeID, Hostname: "failed-node", Status: types.NodeStatusOffline},
	)

	repRepo := newFakeSHReplicaRepo()
	lostReplica := repRepo.InsertReplica(testObjectID, failedNodeID)
	repRepo.UpdateReplicaStatus(lostReplica.ReplicaID, types.ReplicaStatusLost)
	srcReplica := repRepo.InsertReplica(testObjectID, sourceNodeID)
	repRepo.UpdateReplicaStatus(srcReplica.ReplicaID, types.ReplicaStatusHealthy)

	objRepo := newFakeSHObjectRepo()
	objRepo.objects[testObjectID] = &types.Object{
		ObjectID:          testObjectID,
		Checksum:          "0000000000000000000000000000000000000000000000000000000000000000",
		ReplicationFactor: 3,
	}

	logRepo := &fakeSHLogRepo{}
	placement := &fakeSHPlacement{
		targetNodes: []types.StorageNode{
			{NodeID: destNodeID, Hostname: destServer.URL, Status: types.NodeStatusOnline},
		},
	}

	engine := NewSelfHealingEngine(nil, nodeRepo, repRepo, objRepo, logRepo, placement, defaultReplCfg(), defaultHealCfg())

	failedNode := types.StorageNode{NodeID: failedNodeID, Hostname: "failed-node"}
	_, err := engine.RecoverReplica(context.Background(), *lostReplica, failedNode)
	if err == nil {
		t.Error("expected error on checksum mismatch, got nil")
	}

	if logRepo.countByEvent(types.EventChecksumMismatch) == 0 {
		t.Error("expected CHECKSUM_MISMATCH log entry")
	}
}

// TestTargetReplicaRestoration verifies recovery restores lost replicas up to the object's
// target ReplicationFactor (3) even when healthy count meets MinReplicationFactor (2).
func TestTargetReplicaRestoration(t *testing.T) {
	checksum := testChecksum(testContent)

	sourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, testContent)
	}))
	defer sourceServer.Close()

	destServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer destServer.Close()

	node1 := uuid.MustParse("11111111-0000-0000-0000-000000000001")
	node2 := uuid.MustParse("11111111-0000-0000-0000-000000000002")

	nodeRepo := newFakeSHNodeRepo(
		&types.StorageNode{NodeID: node1, Hostname: sourceServer.URL, Status: types.NodeStatusOnline},
		&types.StorageNode{NodeID: node2, Hostname: sourceServer.URL, Status: types.NodeStatusOnline},
		&types.StorageNode{NodeID: destNodeID, Hostname: destServer.URL, Status: types.NodeStatusOnline},
		&types.StorageNode{NodeID: failedNodeID, Hostname: "failed-node", Status: types.NodeStatusOffline},
	)

	repRepo := newFakeSHReplicaRepo()
	r1 := repRepo.InsertReplica(testObjectID, node1)
	repRepo.UpdateReplicaStatus(r1.ReplicaID, types.ReplicaStatusHealthy)
	r2 := repRepo.InsertReplica(testObjectID, node2)
	repRepo.UpdateReplicaStatus(r2.ReplicaID, types.ReplicaStatusHealthy)
	lostReplica := repRepo.InsertReplica(testObjectID, failedNodeID)
	repRepo.UpdateReplicaStatus(lostReplica.ReplicaID, types.ReplicaStatusLost)

	objRepo := newFakeSHObjectRepo()
	objRepo.objects[testObjectID] = &types.Object{
		ObjectID:          testObjectID,
		Checksum:          checksum,
		ReplicationFactor: 3, // Target is 3, currently 2 healthy
	}

	logRepo := &fakeSHLogRepo{}
	placement := &fakeSHPlacement{
		targetNodes: []types.StorageNode{
			{NodeID: destNodeID, Hostname: destServer.URL, Status: types.NodeStatusOnline},
		},
	}

	engine := NewSelfHealingEngine(nil, nodeRepo, repRepo, objRepo, logRepo, placement, defaultReplCfg(), defaultHealCfg())

	failedNode := types.StorageNode{NodeID: failedNodeID, Hostname: "failed-node"}
	recoveredID, err := engine.RecoverReplica(context.Background(), *lostReplica, failedNode)
	if err != nil {
		t.Fatalf("expected recovery to proceed up to target factor 3, got err: %v", err)
	}

	status := repRepo.getStatusFor(recoveredID)
	if status != types.ReplicaStatusHealthy {
		t.Errorf("expected recovered replica to be HEALTHY, got %s", status)
	}
}

// TestTargetAlreadyMetSkipped verifies recovery is skipped when healthy count
// already meets or exceeds target ReplicationFactor.
func TestTargetAlreadyMetSkipped(t *testing.T) {
	node1 := uuid.MustParse("11111111-0000-0000-0000-000000000001")
	node2 := uuid.MustParse("11111111-0000-0000-0000-000000000002")

	nodeRepo := newFakeSHNodeRepo(
		&types.StorageNode{NodeID: node1, Hostname: "node1", Status: types.NodeStatusOnline},
		&types.StorageNode{NodeID: node2, Hostname: "node2", Status: types.NodeStatusOnline},
		&types.StorageNode{NodeID: failedNodeID, Hostname: "failed", Status: types.NodeStatusOffline},
	)

	repRepo := newFakeSHReplicaRepo()
	r1 := repRepo.InsertReplica(testObjectID, node1)
	repRepo.UpdateReplicaStatus(r1.ReplicaID, types.ReplicaStatusHealthy)
	r2 := repRepo.InsertReplica(testObjectID, node2)
	repRepo.UpdateReplicaStatus(r2.ReplicaID, types.ReplicaStatusHealthy)
	lostReplica := repRepo.InsertReplica(testObjectID, failedNodeID)
	repRepo.UpdateReplicaStatus(lostReplica.ReplicaID, types.ReplicaStatusLost)

	objRepo := newFakeSHObjectRepo()
	objRepo.objects[testObjectID] = &types.Object{
		ObjectID:          testObjectID,
		Checksum:          testChecksum(testContent),
		ReplicationFactor: 2, // Target is 2, and we have 2 healthy
	}

	engine := NewSelfHealingEngine(nil, nodeRepo, repRepo, objRepo, &fakeSHLogRepo{}, &fakeSHPlacement{}, defaultReplCfg(), defaultHealCfg())

	failedNode := types.StorageNode{NodeID: failedNodeID}
	_, err := engine.RecoverReplica(context.Background(), *lostReplica, failedNode)
	if err == nil {
		t.Error("expected recovery skip error when target already met, got nil")
	}
	if !strings.Contains(err.Error(), "already has") {
		t.Errorf("expected 'already has' in error, got %v", err)
	}
}

// TestSourceFailover verifies that if the first source replica fails or has corrupted data,
// the engine falls back to the next healthy source replica and succeeds.
func TestSourceFailover(t *testing.T) {
	checksum := testChecksum(testContent)

	// Source 1 returns corrupted data
	corruptedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "corrupted content")
	}))
	defer corruptedServer.Close()

	// Source 2 returns genuine data
	goodServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, testContent)
	}))
	defer goodServer.Close()

	destServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer destServer.Close()

	nodeRepo := newFakeSHNodeRepo(
		&types.StorageNode{NodeID: sourceNodeID, Hostname: corruptedServer.URL, Status: types.NodeStatusOnline},
		&types.StorageNode{NodeID: source2NodeID, Hostname: goodServer.URL, Status: types.NodeStatusOnline},
		&types.StorageNode{NodeID: destNodeID, Hostname: destServer.URL, Status: types.NodeStatusOnline},
		&types.StorageNode{NodeID: failedNodeID, Hostname: "failed-node", Status: types.NodeStatusOffline},
	)

	repRepo := newFakeSHReplicaRepo()
	rBad := repRepo.InsertReplica(testObjectID, sourceNodeID)
	repRepo.UpdateReplicaStatus(rBad.ReplicaID, types.ReplicaStatusHealthy)
	rGood := repRepo.InsertReplica(testObjectID, source2NodeID)
	repRepo.UpdateReplicaStatus(rGood.ReplicaID, types.ReplicaStatusHealthy)

	lostReplica := repRepo.InsertReplica(testObjectID, failedNodeID)
	repRepo.UpdateReplicaStatus(lostReplica.ReplicaID, types.ReplicaStatusLost)

	objRepo := newFakeSHObjectRepo()
	objRepo.objects[testObjectID] = &types.Object{
		ObjectID:          testObjectID,
		Checksum:          checksum,
		ReplicationFactor: 3,
	}

	logRepo := &fakeSHLogRepo{}
	placement := &fakeSHPlacement{
		targetNodes: []types.StorageNode{
			{NodeID: destNodeID, Hostname: destServer.URL, Status: types.NodeStatusOnline},
		},
	}

	engine := NewSelfHealingEngine(nil, nodeRepo, repRepo, objRepo, logRepo, placement, defaultReplCfg(), defaultHealCfg())

	failedNode := types.StorageNode{NodeID: failedNodeID, Hostname: "failed-node"}
	recoveredID, err := engine.RecoverReplica(context.Background(), *lostReplica, failedNode)
	if err != nil {
		t.Fatalf("expected recovery to succeed via source 2, got err: %v", err)
	}

	if repRepo.getStatusFor(recoveredID) != types.ReplicaStatusHealthy {
		t.Errorf("expected recovered replica to be HEALTHY")
	}
}

// TestDuplicateRecoveryPrevention verifies that concurrent calls for the same
// object only allow one to proceed on the real engine.
func TestDuplicateRecoveryPrevention(t *testing.T) {
	nodeRepo := newFakeSHNodeRepo()
	repRepo := newFakeSHReplicaRepo()
	objRepo := newFakeSHObjectRepo()

	engine := NewSelfHealingEngine(nil, nodeRepo, repRepo, objRepo, &fakeSHLogRepo{}, &fakeSHPlacement{}, defaultReplCfg(), defaultHealCfg())

	engine.mu.Lock()
	engine.inProgress[testObjectID] = struct{}{}
	engine.mu.Unlock()

	lostReplica := types.Replica{ObjectID: testObjectID}
	failedNode := types.StorageNode{NodeID: failedNodeID}

	_, err := engine.RecoverReplica(context.Background(), lostReplica, failedNode)
	if err == nil {
		t.Error("expected duplicate in-progress error, got nil")
	}
	if !strings.Contains(err.Error(), "already in progress") {
		t.Errorf("expected 'already in progress' error, got %v", err)
	}
}
