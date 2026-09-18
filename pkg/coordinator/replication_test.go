package coordinator

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"distributed-storage/pkg/types"

	"github.com/google/uuid"
)

// ---------------------------------------------------------------------------
// In-memory fakes for replication tests
// ---------------------------------------------------------------------------

type fakeAccessLogRepo struct {
	mu     sync.Mutex
	counts map[uuid.UUID]int64
}

func newFakeAccessLogRepo() *fakeAccessLogRepo {
	return &fakeAccessLogRepo{counts: make(map[uuid.UUID]int64)}
}

func (f *fakeAccessLogRepo) GetAccessCountSince(objectID uuid.UUID, since time.Time) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.counts[objectID], nil
}

func (f *fakeAccessLogRepo) GetAccessCountsByObjectSince(since time.Time) (map[uuid.UUID]int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make(map[uuid.UUID]int64)
	for k, v := range f.counts {
		cp[k] = v
	}
	return cp, nil
}

type fakeReplicaRepoForRepl struct {
	mu       sync.Mutex
	replicas []*types.Replica
}

func newFakeReplicaRepoForRepl() *fakeReplicaRepoForRepl {
	return &fakeReplicaRepoForRepl{}
}

func (f *fakeReplicaRepoForRepl) GetReplicasByObject(objectID uuid.UUID) ([]types.Replica, error) {
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

func (f *fakeReplicaRepoForRepl) GetHealthyReplicasByObject(objectID uuid.UUID) ([]types.Replica, error) {
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

func (f *fakeReplicaRepoForRepl) CountHealthyReplicas(objectID uuid.UUID) (int, error) {
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

func (f *fakeReplicaRepoForRepl) InsertHealthyReplica(objectID, nodeID uuid.UUID) (*types.Replica, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := &types.Replica{
		ReplicaID: uuid.New(),
		ObjectID:  objectID,
		NodeID:    nodeID,
		Status:    types.ReplicaStatusHealthy,
		CreatedAt: time.Now(),
	}
	f.replicas = append(f.replicas, r)
	return r, nil
}

func (f *fakeReplicaRepoForRepl) InsertHealthyReplicaTx(tx *sql.Tx, objectID, nodeID uuid.UUID) (*types.Replica, error) {
	return f.InsertHealthyReplica(objectID, nodeID)
}

func (f *fakeReplicaRepoForRepl) DeleteReplica(replicaID uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	var remaining []*types.Replica
	found := false
	for _, r := range f.replicas {
		if r.ReplicaID == replicaID {
			found = true
		} else {
			remaining = append(remaining, r)
		}
	}
	if !found {
		return fmt.Errorf("replica %s not found", replicaID)
	}
	f.replicas = remaining
	return nil
}

func (f *fakeReplicaRepoForRepl) DeleteReplicaTx(tx *sql.Tx, replicaID uuid.UUID) error {
	return f.DeleteReplica(replicaID)
}

type fakeObjectRepoForRepl struct {
	mu      sync.Mutex
	objects map[uuid.UUID]*types.Object
}

func newFakeObjectRepoForRepl() *fakeObjectRepoForRepl {
	return &fakeObjectRepoForRepl{objects: make(map[uuid.UUID]*types.Object)}
}

func (f *fakeObjectRepoForRepl) GetAllObjects() ([]types.Object, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []types.Object
	for _, obj := range f.objects {
		out = append(out, *obj)
	}
	return out, nil
}

func (f *fakeObjectRepoForRepl) GetObjectByID(id uuid.UUID) (*types.Object, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if obj, ok := f.objects[id]; ok {
		cp := *obj
		return &cp, nil
	}
	return nil, fmt.Errorf("object %s not found", id)
}

func (f *fakeObjectRepoForRepl) UpdateReplicationFactor(objectID uuid.UUID, factor int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if obj, ok := f.objects[objectID]; ok {
		obj.ReplicationFactor = factor
		return nil
	}
	return fmt.Errorf("object %s not found", objectID)
}

func (f *fakeObjectRepoForRepl) UpdateReplicationFactorTx(tx *sql.Tx, objectID uuid.UUID, factor int) error {
	return f.UpdateReplicationFactor(objectID, factor)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestClassifyTier(t *testing.T) {
	cfg := defaultReplCfg()
	rm := &ReplicationManager{cfg: cfg}

	// Hot threshold = 50, cold threshold = 5
	// Case 1: Hot
	tier, factor := rm.ClassifyTier(50)
	if tier != types.TierHot || factor != 5 {
		t.Errorf("expected HOT / factor 5 for 50 accesses, got %s / %d", tier, factor)
	}

	tier, factor = rm.ClassifyTier(100)
	if tier != types.TierHot || factor != 5 {
		t.Errorf("expected HOT / factor 5 for 100 accesses, got %s / %d", tier, factor)
	}

	// Case 2: Warm
	tier, factor = rm.ClassifyTier(5)
	if tier != types.TierWarm || factor != 3 {
		t.Errorf("expected WARM / factor 3 for 5 accesses, got %s / %d", tier, factor)
	}

	tier, factor = rm.ClassifyTier(49)
	if tier != types.TierWarm || factor != 3 {
		t.Errorf("expected WARM / factor 3 for 49 accesses, got %s / %d", tier, factor)
	}

	// Case 3: Cold
	tier, factor = rm.ClassifyTier(4)
	if tier != types.TierCold || factor != 2 {
		t.Errorf("expected COLD / factor 2 for 4 accesses, got %s / %d", tier, factor)
	}

	tier, factor = rm.ClassifyTier(0)
	if tier != types.TierCold || factor != 2 {
		t.Errorf("expected COLD / factor 2 for 0 accesses, got %s / %d", tier, factor)
	}
}

// TestAdaptiveReplicationScaleUpHot verifies that an object with high access frequency
// triggers scale-up to MaxReplicationFactor (5).
func TestAdaptiveReplicationScaleUpHot(t *testing.T) {
	objID := uuid.New()
	content := "hot object replication data"
	h := sha256.Sum256([]byte(content))
	checksum := hex.EncodeToString(h[:])

	// Mock existing storage server
	srcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, content)
	}))
	defer srcServer.Close()

	// Mock new destination storage servers
	dest1Server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer dest1Server.Close()

	dest2Server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer dest2Server.Close()

	node1 := uuid.MustParse("10000000-0000-0000-0000-000000000001")
	node2 := uuid.MustParse("20000000-0000-0000-0000-000000000002")
	node3 := uuid.MustParse("30000000-0000-0000-0000-000000000003")
	node4 := uuid.MustParse("40000000-0000-0000-0000-000000000004")
	node5 := uuid.MustParse("50000000-0000-0000-0000-000000000005")

	nodeRepo := newFakeSHNodeRepo(
		&types.StorageNode{NodeID: node1, Hostname: srcServer.URL, Status: types.NodeStatusOnline},
		&types.StorageNode{NodeID: node2, Hostname: srcServer.URL, Status: types.NodeStatusOnline},
		&types.StorageNode{NodeID: node3, Hostname: srcServer.URL, Status: types.NodeStatusOnline},
		&types.StorageNode{NodeID: node4, Hostname: dest1Server.URL, Status: types.NodeStatusOnline},
		&types.StorageNode{NodeID: node5, Hostname: dest2Server.URL, Status: types.NodeStatusOnline},
	)

	repRepo := newFakeReplicaRepoForRepl()
	r1, _ := repRepo.InsertHealthyReplica(objID, node1)
	r2, _ := repRepo.InsertHealthyReplica(objID, node2)
	r3, _ := repRepo.InsertHealthyReplica(objID, node3)
	_ = r1
	_ = r2
	_ = r3

	objRepo := newFakeObjectRepoForRepl()
	objRepo.objects[objID] = &types.Object{
		ObjectID:          objID,
		ObjectName:        "hot-file.bin",
		Checksum:          checksum,
		ReplicationFactor: 3, // currently 3
	}

	accessRepo := newFakeAccessLogRepo()
	accessRepo.counts[objID] = 75 // HOT tier! (threshold 50)

	logRepo := &fakeSHLogRepo{}
	placement := &fakeSHPlacement{
		targetNodes: []types.StorageNode{
			{NodeID: node4, Hostname: dest1Server.URL, Status: types.NodeStatusOnline},
			{NodeID: node5, Hostname: dest2Server.URL, Status: types.NodeStatusOnline},
		},
	}

	cfg := defaultReplCfg()
	rm := NewReplicationManager(nil, accessRepo, objRepo, repRepo, nodeRepo, logRepo, placement, cfg)

	err := rm.ReconcileObject(context.Background(), objID)
	if err != nil {
		t.Fatalf("ReconcileObject failed: %v", err)
	}

	// Should now have 5 healthy replicas
	count, _ := repRepo.CountHealthyReplicas(objID)
	if count != 5 {
		t.Errorf("expected 5 replicas after scale-up, got %d", count)
	}

	obj, _ := objRepo.GetObjectByID(objID)
	if obj.ReplicationFactor != 5 {
		t.Errorf("expected object ReplicationFactor=5, got %d", obj.ReplicationFactor)
	}

	if logRepo.countByEvent(types.EventReplicationScaleUp) != 1 {
		t.Errorf("expected REPLICATION_SCALE_UP log entry")
	}
}

// TestAdaptiveReplicationScaleDownCold verifies that an object with low access frequency
// triggers scale-down to MinReplicationFactor (2), deleting chunks and records.
func TestAdaptiveReplicationScaleDownCold(t *testing.T) {
	objID := uuid.New()

	deletedChunk := false
	deletedNodeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deletedChunk = true
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer deletedNodeServer.Close()

	aliveNodeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer aliveNodeServer.Close()

	node1 := uuid.MustParse("10000000-0000-0000-0000-000000000001")
	node2 := uuid.MustParse("20000000-0000-0000-0000-000000000002")
	node3 := uuid.MustParse("30000000-0000-0000-0000-000000000003")

	nodeRepo := newFakeSHNodeRepo(
		&types.StorageNode{NodeID: node1, Hostname: aliveNodeServer.URL, CPUUsage: 10, Status: types.NodeStatusOnline},
		&types.StorageNode{NodeID: node2, Hostname: aliveNodeServer.URL, CPUUsage: 20, Status: types.NodeStatusOnline},
		&types.StorageNode{NodeID: node3, Hostname: deletedNodeServer.URL, CPUUsage: 80, Status: types.NodeStatusOnline}, // High load -> should be pruned first
	)

	repRepo := newFakeReplicaRepoForRepl()
	_, _ = repRepo.InsertHealthyReplica(objID, node1)
	_, _ = repRepo.InsertHealthyReplica(objID, node2)
	_, _ = repRepo.InsertHealthyReplica(objID, node3)

	objRepo := newFakeObjectRepoForRepl()
	objRepo.objects[objID] = &types.Object{
		ObjectID:          objID,
		ObjectName:        "cold-file.bin",
		Checksum:          "somechecksum",
		ReplicationFactor: 3, // currently 3
	}

	accessRepo := newFakeAccessLogRepo()
	accessRepo.counts[objID] = 2 // COLD tier (< 5)

	logRepo := &fakeSHLogRepo{}
	cfg := defaultReplCfg()
	rm := NewReplicationManager(nil, accessRepo, objRepo, repRepo, nodeRepo, logRepo, &fakeSHPlacement{}, cfg)

	err := rm.ReconcileObject(context.Background(), objID)
	if err != nil {
		t.Fatalf("ReconcileObject failed: %v", err)
	}

	// Should now be scaled down to MinReplicationFactor = 2
	count, _ := repRepo.CountHealthyReplicas(objID)
	if count != 2 {
		t.Errorf("expected 2 replicas after scale-down, got %d", count)
	}

	if !deletedChunk {
		t.Errorf("expected HTTP DELETE to have been sent to pruned storage node")
	}

	obj, _ := objRepo.GetObjectByID(objID)
	if obj.ReplicationFactor != 2 {
		t.Errorf("expected object ReplicationFactor=2, got %d", obj.ReplicationFactor)
	}

	if logRepo.countByEvent(types.EventReplicationScaleDown) != 1 {
		t.Errorf("expected REPLICATION_SCALE_DOWN log entry")
	}
}

// TestAdaptiveReplicationMinFloorEnforcement verifies that cold objects already at
// MinReplicationFactor (2) are NEVER reduced below the floor.
func TestAdaptiveReplicationMinFloorEnforcement(t *testing.T) {
	objID := uuid.New()

	node1 := uuid.MustParse("10000000-0000-0000-0000-000000000001")
	node2 := uuid.MustParse("20000000-0000-0000-0000-000000000002")

	nodeRepo := newFakeSHNodeRepo(
		&types.StorageNode{NodeID: node1, Hostname: "node1", Status: types.NodeStatusOnline},
		&types.StorageNode{NodeID: node2, Hostname: "node2", Status: types.NodeStatusOnline},
	)

	repRepo := newFakeReplicaRepoForRepl()
	_, _ = repRepo.InsertHealthyReplica(objID, node1)
	_, _ = repRepo.InsertHealthyReplica(objID, node2)

	objRepo := newFakeObjectRepoForRepl()
	objRepo.objects[objID] = &types.Object{
		ObjectID:          objID,
		ObjectName:        "floor-file.bin",
		Checksum:          "somechecksum",
		ReplicationFactor: 2, // At minimum
	}

	accessRepo := newFakeAccessLogRepo()
	accessRepo.counts[objID] = 0 // 0 accesses -> still cannot go below 2!

	logRepo := &fakeSHLogRepo{}
	cfg := defaultReplCfg()
	rm := NewReplicationManager(nil, accessRepo, objRepo, repRepo, nodeRepo, logRepo, &fakeSHPlacement{}, cfg)

	err := rm.ReconcileObject(context.Background(), objID)
	if err != nil {
		t.Fatalf("ReconcileObject failed: %v", err)
	}

	count, _ := repRepo.CountHealthyReplicas(objID)
	if count != 2 {
		t.Errorf("expected replica count to remain 2, got %d", count)
	}
	if logRepo.countByEvent(types.EventReplicationScaleDown) != 0 {
		t.Errorf("no scale-down should have occurred")
	}
}

// TestAdaptiveReplicationWarmNoOp verifies that warm objects already at DefaultReplicationFactor
// remain untouched.
func TestAdaptiveReplicationWarmNoOp(t *testing.T) {
	objID := uuid.New()

	node1 := uuid.MustParse("10000000-0000-0000-0000-000000000001")
	node2 := uuid.MustParse("20000000-0000-0000-0000-000000000002")
	node3 := uuid.MustParse("30000000-0000-0000-0000-000000000003")

	nodeRepo := newFakeSHNodeRepo(
		&types.StorageNode{NodeID: node1, Hostname: "node1", Status: types.NodeStatusOnline},
		&types.StorageNode{NodeID: node2, Hostname: "node2", Status: types.NodeStatusOnline},
		&types.StorageNode{NodeID: node3, Hostname: "node3", Status: types.NodeStatusOnline},
	)

	repRepo := newFakeReplicaRepoForRepl()
	_, _ = repRepo.InsertHealthyReplica(objID, node1)
	_, _ = repRepo.InsertHealthyReplica(objID, node2)
	_, _ = repRepo.InsertHealthyReplica(objID, node3)

	objRepo := newFakeObjectRepoForRepl()
	objRepo.objects[objID] = &types.Object{
		ObjectID:          objID,
		ObjectName:        "warm-file.bin",
		Checksum:          "somechecksum",
		ReplicationFactor: 3,
	}

	accessRepo := newFakeAccessLogRepo()
	accessRepo.counts[objID] = 25 // WARM (5 <= 25 < 50)

	logRepo := &fakeSHLogRepo{}
	cfg := defaultReplCfg()
	rm := NewReplicationManager(nil, accessRepo, objRepo, repRepo, nodeRepo, logRepo, &fakeSHPlacement{}, cfg)

	err := rm.ReconcileObject(context.Background(), objID)
	if err != nil {
		t.Fatalf("ReconcileObject failed: %v", err)
	}

	count, _ := repRepo.CountHealthyReplicas(objID)
	if count != 3 {
		t.Errorf("expected replica count to remain 3, got %d", count)
	}
}
