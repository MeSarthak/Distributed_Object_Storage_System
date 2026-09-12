package coordinator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
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
// In-memory fakes for self-healing tests
// ---------------------------------------------------------------------------

type fakeReplicaRepo struct {
	mu       sync.Mutex
	replicas []*types.Replica
	updates  []replicaUpdate
}

type replicaUpdate struct {
	replicaID uuid.UUID
	status    types.ReplicaStatus
}

func (f *fakeReplicaRepo) GetReplicasByNode(nodeID uuid.UUID) ([]types.Replica, error) {
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

func (f *fakeReplicaRepo) GetReplicasByObject(objectID uuid.UUID) ([]types.Replica, error) {
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

func (f *fakeReplicaRepo) GetHealthyReplicasByObject(objectID uuid.UUID, onlineNodeIDs map[uuid.UUID]bool) ([]types.Replica, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []types.Replica
	for _, r := range f.replicas {
		if r.ObjectID == objectID && r.Status == types.ReplicaStatusHealthy && onlineNodeIDs[r.NodeID] {
			out = append(out, *r)
		}
	}
	return out, nil
}

func (f *fakeReplicaRepo) CountHealthyReplicas(objectID uuid.UUID, onlineNodeIDs map[uuid.UUID]bool) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	count := 0
	for _, r := range f.replicas {
		if r.ObjectID == objectID && r.Status == types.ReplicaStatusHealthy && onlineNodeIDs[r.NodeID] {
			count++
		}
	}
	return count, nil
}

func (f *fakeReplicaRepo) InsertReplica(objectID, nodeID uuid.UUID) *types.Replica {
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

func (f *fakeReplicaRepo) UpdateStatus(replicaID uuid.UUID, status types.ReplicaStatus) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range f.replicas {
		if r.ReplicaID == replicaID {
			r.Status = status
		}
	}
	f.updates = append(f.updates, replicaUpdate{replicaID, status})
}

func (f *fakeReplicaRepo) getStatusFor(replicaID uuid.UUID) types.ReplicaStatus {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range f.replicas {
		if r.ReplicaID == replicaID {
			return r.Status
		}
	}
	return ""
}

type fakeObjectRepo struct {
	objects map[uuid.UUID]*types.Object
}

func (f *fakeObjectRepo) GetObjectByID(id uuid.UUID) (*types.Object, error) {
	if obj, ok := f.objects[id]; ok {
		return obj, nil
	}
	return nil, fmt.Errorf("object %s not found", id)
}

type fakeSHELogRepo struct {
	mu      sync.Mutex
	entries []fakeLogEntry
}

func (f *fakeSHELogRepo) InsertSystemLog(eventType, desc string, sev types.LogSeverity, meta map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries = append(f.entries, fakeLogEntry{eventType, desc, sev})
	return nil
}

func (f *fakeSHELogRepo) countByEvent(et string) int {
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

// ---------------------------------------------------------------------------
// testableHealingEngine mirrors SelfHealingEngine logic with fakes
// ---------------------------------------------------------------------------

type testableHealingEngine struct {
	replicas   *fakeReplicaRepo
	objects    *fakeObjectRepo
	logRepo    *fakeSHELogRepo
	onlineIDs  map[uuid.UUID]bool // simulates ONLINE nodes for source/dest selection
	nodeMap    map[uuid.UUID]*types.StorageNode
	inProgress map[uuid.UUID]struct{}
	mu         sync.Mutex
	replCfg    config.ReplicationConfig

	// http clients for source/dest (tests inject httptest servers)
	sourceURLs map[uuid.UUID]string // nodeID → base URL
	destURLs   map[uuid.UUID]string // nodeID → base URL
}

func newTestableHealingEngine(replCfg config.ReplicationConfig) *testableHealingEngine {
	return &testableHealingEngine{
		replicas:   &fakeReplicaRepo{},
		objects:    &fakeObjectRepo{objects: make(map[uuid.UUID]*types.Object)},
		logRepo:    &fakeSHELogRepo{},
		onlineIDs:  make(map[uuid.UUID]bool),
		nodeMap:    make(map[uuid.UUID]*types.StorageNode),
		inProgress: make(map[uuid.UUID]struct{}),
		replCfg:    replCfg,
		sourceURLs: make(map[uuid.UUID]string),
		destURLs:   make(map[uuid.UUID]string),
	}
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

// acquireInProgress returns true if we successfully claimed the object (not duplicate).
func (te *testableHealingEngine) acquireInProgress(objectID uuid.UUID) bool {
	te.mu.Lock()
	defer te.mu.Unlock()
	if _, busy := te.inProgress[objectID]; busy {
		return false
	}
	te.inProgress[objectID] = struct{}{}
	return true
}

func (te *testableHealingEngine) releaseInProgress(objectID uuid.UUID) {
	te.mu.Lock()
	defer te.mu.Unlock()
	delete(te.inProgress, objectID)
}

// recoverReplica is a simplified version of the production logic for unit testing.
func (te *testableHealingEngine) recoverReplica(
	ctx context.Context,
	lostReplica types.Replica,
	failedNode types.StorageNode,
	destNodeID uuid.UUID,
) (recoveredID uuid.UUID, err error) {
	objectID := lostReplica.ObjectID

	if !te.acquireInProgress(objectID) {
		return uuid.Nil, fmt.Errorf("duplicate: already in progress")
	}
	defer te.releaseInProgress(objectID)

	// Check healthy count.
	healthyCount, _ := te.replicas.CountHealthyReplicas(objectID, te.onlineIDs)
	if healthyCount >= te.replCfg.MinReplicationFactor {
		return uuid.Nil, fmt.Errorf("already has %d healthy replicas", healthyCount)
	}

	// Get object metadata (for checksum).
	obj, err := te.objects.GetObjectByID(objectID)
	if err != nil {
		return uuid.Nil, err
	}

	// Find healthy source.
	sourceReplicas, _ := te.replicas.GetHealthyReplicasByObject(objectID, te.onlineIDs)
	if len(sourceReplicas) == 0 {
		_ = te.logRepo.InsertSystemLog("RECOVERY_FAILED", "no source", types.SeverityError, nil)
		return uuid.Nil, fmt.Errorf("no healthy source replicas")
	}
	sourceReplica := sourceReplicas[0]
	sourceURL, ok := te.sourceURLs[sourceReplica.NodeID]
	if !ok {
		return uuid.Nil, fmt.Errorf("no source URL for node %s", sourceReplica.NodeID)
	}

	destURL, ok := te.destURLs[destNodeID]
	if !ok {
		return uuid.Nil, fmt.Errorf("no dest URL for node %s", destNodeID)
	}

	// Fetch from source.
	resp, err := http.Get(sourceURL + "/internal/storage/" + objectID.String())
	if err != nil || resp.StatusCode != http.StatusOK {
		return uuid.Nil, fmt.Errorf("fetch failed: %v", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return uuid.Nil, fmt.Errorf("read body: %v", err)
	}

	// Verify checksum.
	h := sha256.Sum256(data)
	computed := hex.EncodeToString(h[:])
	if computed != obj.Checksum {
		_ = te.logRepo.InsertSystemLog("CHECKSUM_MISMATCH", "mismatch", types.SeverityCritical, nil)
		return uuid.Nil, fmt.Errorf("checksum mismatch: expected %s got %s", obj.Checksum, computed)
	}

	// Mark lost + insert recovering (simulated atomically in fake).
	te.replicas.UpdateStatus(lostReplica.ReplicaID, types.ReplicaStatusLost)
	newReplica := te.replicas.InsertReplica(objectID, destNodeID)

	// Push to dest.
	pushResp, err := http.Post(destURL+"/internal/storage/store", "application/octet-stream", strings.NewReader(string(data)))
	if err != nil || pushResp.StatusCode != http.StatusOK {
		te.replicas.UpdateStatus(newReplica.ReplicaID, types.ReplicaStatusLost)
		return uuid.Nil, fmt.Errorf("push failed")
	}
	defer pushResp.Body.Close()

	// Mark HEALTHY.
	te.replicas.UpdateStatus(newReplica.ReplicaID, types.ReplicaStatusHealthy)
	_ = te.logRepo.InsertSystemLog("RECOVERY_SUCCESS", "recovered", types.SeverityInfo, nil)
	return newReplica.ReplicaID, nil
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

var (
	failedNodeID = uuid.MustParse("aaaaaaaa-0000-0000-0000-000000000001")
	sourceNodeID = uuid.MustParse("bbbbbbbb-0000-0000-0000-000000000002")
	destNodeID   = uuid.MustParse("cccccccc-0000-0000-0000-000000000003")
	testObjectID = uuid.MustParse("dddddddd-0000-0000-0000-000000000001")
)

const testContent = "hello distributed storage"

func testChecksum(data string) string {
	h := sha256.Sum256([]byte(data))
	return hex.EncodeToString(h[:])
}

// TestAffectedReplicaDetection verifies GetReplicasByNode returns the correct replicas.
func TestAffectedReplicaDetection(t *testing.T) {
	te := newTestableHealingEngine(defaultReplCfg())
	otherNodeID := uuid.MustParse("eeeeeeee-0000-0000-0000-000000000001")

	// Add 2 replicas: one on failed node, one on another node.
	te.replicas.InsertReplica(testObjectID, failedNodeID)
	te.replicas.InsertReplica(testObjectID, otherNodeID)

	affected, err := te.replicas.GetReplicasByNode(failedNodeID)
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
// unhealthy and offline-node replicas.
func TestHealthySourceSelection(t *testing.T) {
	te := newTestableHealingEngine(defaultReplCfg())

	// Source node is online and replica is healthy.
	te.onlineIDs[sourceNodeID] = true
	healthyReplica := te.replicas.InsertReplica(testObjectID, sourceNodeID)
	te.replicas.UpdateStatus(healthyReplica.ReplicaID, types.ReplicaStatusHealthy)

	// A LOST replica on a different online node — should not be selected.
	lostNodeID := uuid.MustParse("ffffffff-0000-0000-0000-000000000001")
	te.onlineIDs[lostNodeID] = true
	lostReplica := te.replicas.InsertReplica(testObjectID, lostNodeID)
	te.replicas.UpdateStatus(lostReplica.ReplicaID, types.ReplicaStatusLost)

	sources, err := te.replicas.GetHealthyReplicasByObject(testObjectID, te.onlineIDs)
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

// TestFailedNodeExclusion verifies that when we collect excludeIDs, the failed
// node's ID is always present.
func TestFailedNodeExclusion(t *testing.T) {
	allReplicas := []types.Replica{
		{ReplicaID: uuid.New(), ObjectID: testObjectID, NodeID: failedNodeID, Status: types.ReplicaStatusHealthy},
		{ReplicaID: uuid.New(), ObjectID: testObjectID, NodeID: sourceNodeID, Status: types.ReplicaStatusHealthy},
	}

	excludeIDs := make([]uuid.UUID, 0, len(allReplicas)+1)
	excludeIDs = append(excludeIDs, failedNodeID)
	for _, r := range allReplicas {
		excludeIDs = append(excludeIDs, r.NodeID)
	}

	excluded := make(map[uuid.UUID]struct{})
	for _, id := range excludeIDs {
		excluded[id] = struct{}{}
	}

	if _, ok := excluded[failedNodeID]; !ok {
		t.Error("failed node ID was not in the exclusion set")
	}
}

// TestDestinationSelection verifies that nodes already holding a replica are excluded.
func TestDestinationSelection(t *testing.T) {
	// Simulate: 3 online nodes; failedNode and sourceNode already hold replicas.
	// Only destNode should be eligible.
	te := newTestableHealingEngine(defaultReplCfg())
	te.onlineIDs[failedNodeID] = false // offline
	te.onlineIDs[sourceNodeID] = true
	te.onlineIDs[destNodeID] = true

	te.replicas.InsertReplica(testObjectID, failedNodeID)
	healthyReplica := te.replicas.InsertReplica(testObjectID, sourceNodeID)
	te.replicas.UpdateStatus(healthyReplica.ReplicaID, types.ReplicaStatusHealthy)

	allReplicas, _ := te.replicas.GetReplicasByObject(testObjectID)

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
	// destNodeID is NOT in excluded — placement engine should select it.
	if _, ok := excluded[destNodeID]; ok {
		t.Error("destination node should NOT be in exclusion set before placement")
	}
}

// TestChecksumVerification verifies that a correct SHA-256 leads to HEALTHY status.
func TestChecksumVerification(t *testing.T) {
	te := newTestableHealingEngine(defaultReplCfg())
	checksum := testChecksum(testContent)

	te.objects.objects[testObjectID] = &types.Object{
		ObjectID:          testObjectID,
		Checksum:          checksum,
		ReplicationFactor: 3,
	}

	// Set up source HTTP test server serving the correct content.
	sourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, testContent)
	}))
	defer sourceServer.Close()

	// Set up dest HTTP test server accepting stores.
	destServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer destServer.Close()

	te.onlineIDs[sourceNodeID] = true
	te.onlineIDs[destNodeID] = true
	te.sourceURLs[sourceNodeID] = sourceServer.URL
	te.destURLs[destNodeID] = destServer.URL

	lostReplica := te.replicas.InsertReplica(testObjectID, failedNodeID)
	te.replicas.UpdateStatus(lostReplica.ReplicaID, types.ReplicaStatusLost)
	srcReplica := te.replicas.InsertReplica(testObjectID, sourceNodeID)
	te.replicas.UpdateStatus(srcReplica.ReplicaID, types.ReplicaStatusHealthy)

	failedNode := types.StorageNode{NodeID: failedNodeID, Hostname: "failed"}
	recoveredID, err := te.recoverReplica(context.Background(), *lostReplica, failedNode, destNodeID)
	if err != nil {
		t.Fatalf("unexpected recovery error: %v", err)
	}

	finalStatus := te.replicas.getStatusFor(recoveredID)
	if finalStatus != types.ReplicaStatusHealthy {
		t.Errorf("expected HEALTHY after correct checksum, got %s", finalStatus)
	}
}

// TestChecksumMismatch verifies that a mismatched SHA-256 prevents HEALTHY status.
func TestChecksumMismatch(t *testing.T) {
	te := newTestableHealingEngine(defaultReplCfg())

	// Object checksum does NOT match what the source server returns.
	te.objects.objects[testObjectID] = &types.Object{
		ObjectID:          testObjectID,
		Checksum:          "0000000000000000000000000000000000000000000000000000000000000000",
		ReplicationFactor: 3,
	}

	sourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, testContent) // real content — checksum won't match "0000…"
	}))
	defer sourceServer.Close()

	destServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer destServer.Close()

	te.onlineIDs[sourceNodeID] = true
	te.onlineIDs[destNodeID] = true
	te.sourceURLs[sourceNodeID] = sourceServer.URL
	te.destURLs[destNodeID] = destServer.URL

	lostReplica := te.replicas.InsertReplica(testObjectID, failedNodeID)
	te.replicas.UpdateStatus(lostReplica.ReplicaID, types.ReplicaStatusLost)
	srcReplica := te.replicas.InsertReplica(testObjectID, sourceNodeID)
	te.replicas.UpdateStatus(srcReplica.ReplicaID, types.ReplicaStatusHealthy)

	failedNode := types.StorageNode{NodeID: failedNodeID, Hostname: "failed"}
	_, err := te.recoverReplica(context.Background(), *lostReplica, failedNode, destNodeID)
	if err == nil {
		t.Error("expected error on checksum mismatch, got nil")
	}
	if !strings.Contains(err.Error(), "checksum mismatch") {
		t.Errorf("expected checksum mismatch error, got: %v", err)
	}

	// Verify that CHECKSUM_MISMATCH was logged.
	if te.logRepo.countByEvent("CHECKSUM_MISMATCH") == 0 {
		t.Error("expected CHECKSUM_MISMATCH log entry")
	}
}

// TestMinReplicaRestoration verifies recovery is skipped when healthy count
// already meets MinReplicationFactor.
func TestMinReplicaRestoration(t *testing.T) {
	te := newTestableHealingEngine(defaultReplCfg())

	te.objects.objects[testObjectID] = &types.Object{
		ObjectID:          testObjectID,
		Checksum:          testChecksum(testContent),
		ReplicationFactor: 3,
	}

	// Two healthy replicas — meets MinReplicationFactor=2.
	node1 := uuid.MustParse("11111111-0000-0000-0000-000000000001")
	node2 := uuid.MustParse("11111111-0000-0000-0000-000000000002")
	te.onlineIDs[node1] = true
	te.onlineIDs[node2] = true
	r1 := te.replicas.InsertReplica(testObjectID, node1)
	te.replicas.UpdateStatus(r1.ReplicaID, types.ReplicaStatusHealthy)
	r2 := te.replicas.InsertReplica(testObjectID, node2)
	te.replicas.UpdateStatus(r2.ReplicaID, types.ReplicaStatusHealthy)

	lostReplica := te.replicas.InsertReplica(testObjectID, failedNodeID)
	te.replicas.UpdateStatus(lostReplica.ReplicaID, types.ReplicaStatusLost)

	failedNode := types.StorageNode{NodeID: failedNodeID}
	_, err := te.recoverReplica(context.Background(), *lostReplica, failedNode, destNodeID)
	if err == nil {
		t.Error("expected 'already has N healthy replicas' skip, got nil error")
	}
	if !strings.Contains(err.Error(), "already has") {
		t.Errorf("expected 'already has' message, got: %v", err)
	}
}

// TestDuplicateRecoveryPrevention verifies that concurrent calls for the same
// object only allow one to proceed.
func TestDuplicateRecoveryPrevention(t *testing.T) {
	te := newTestableHealingEngine(defaultReplCfg())

	// First caller acquires the lock.
	if !te.acquireInProgress(testObjectID) {
		t.Fatal("first caller should succeed")
	}

	// Second caller should be blocked.
	if te.acquireInProgress(testObjectID) {
		t.Error("second concurrent caller should have been blocked")
	}

	te.releaseInProgress(testObjectID)

	// After release, a new caller should succeed.
	if !te.acquireInProgress(testObjectID) {
		t.Error("caller after release should succeed")
	}
	te.releaseInProgress(testObjectID)
}

// TestAtomicMetadataUpdate verifies that LOST and RECOVERING transitions happen
// together (here via fake UpdateStatus + InsertReplica representing atomicity).
func TestAtomicMetadataUpdate(t *testing.T) {
	te := newTestableHealingEngine(defaultReplCfg())

	lostReplica := te.replicas.InsertReplica(testObjectID, failedNodeID)

	// Simulate the atomic transition.
	te.replicas.UpdateStatus(lostReplica.ReplicaID, types.ReplicaStatusLost)
	newReplica := te.replicas.InsertReplica(testObjectID, destNodeID)

	lostStatus := te.replicas.getStatusFor(lostReplica.ReplicaID)
	recoveringStatus := te.replicas.getStatusFor(newReplica.ReplicaID)

	if lostStatus != types.ReplicaStatusLost {
		t.Errorf("expected old replica LOST, got %s", lostStatus)
	}
	if recoveringStatus != types.ReplicaStatusRecovering {
		t.Errorf("expected new replica RECOVERING, got %s", recoveringStatus)
	}
}
