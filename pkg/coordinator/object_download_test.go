package coordinator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"distributed-storage/pkg/auth"
	"distributed-storage/pkg/types"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// ---------------------------------------------------------------------------
// Test Doubles for Download & Delete Handler Tests
// ---------------------------------------------------------------------------

type downloadMockStorageClient struct {
	mu              sync.Mutex
	fetchPerNode    map[uuid.UUID][]byte
	fetchErrPerNode map[uuid.UUID]error
	deletedChunks   []uuid.UUID
}

func newDownloadMockStorageClient() *downloadMockStorageClient {
	return &downloadMockStorageClient{
		fetchPerNode:    make(map[uuid.UUID][]byte),
		fetchErrPerNode: make(map[uuid.UUID]error),
	}
}

func (m *downloadMockStorageClient) StoreChunk(ctx context.Context, node types.StorageNode, objectID uuid.UUID, fileData []byte) error {
	return nil
}

func (m *downloadMockStorageClient) FetchChunk(ctx context.Context, node types.StorageNode, objectID uuid.UUID) (io.ReadCloser, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err, exists := m.fetchErrPerNode[node.NodeID]; exists && err != nil {
		return nil, 0, err
	}

	if data, exists := m.fetchPerNode[node.NodeID]; exists {
		return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
	}

	return nil, 0, errors.New("node chunk not found")
}

func (m *downloadMockStorageClient) DeleteChunk(ctx context.Context, node types.StorageNode, objectID uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deletedChunks = append(m.deletedChunks, objectID)
	return nil
}

func (m *downloadMockStorageClient) VerifyChunk(ctx context.Context, node types.StorageNode, objectID uuid.UUID) (*types.VerifyResult, error) {
	return &types.VerifyResult{Valid: true}, nil
}

// mockObjectRepo implements ObjectRepositoryReader and ObjectRepositoryDeleter
type mockObjectRepo struct {
	mu      sync.RWMutex
	objects map[uuid.UUID]*types.Object
}

func newMockObjectRepo() *mockObjectRepo {
	return &mockObjectRepo{objects: make(map[uuid.UUID]*types.Object)}
}

func (m *mockObjectRepo) GetObjectByID(objectID uuid.UUID) (*types.Object, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if obj, exists := m.objects[objectID]; exists {
		cp := *obj
		return &cp, nil
	}
	return nil, sql.ErrNoRows
}

func (m *mockObjectRepo) GetObjectsByOwner(ownerID uuid.UUID, limit, offset int) ([]types.Object, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var res []types.Object
	for _, obj := range m.objects {
		if obj.OwnerID == ownerID {
			res = append(res, *obj)
		}
	}
	return res, nil
}

func (m *mockObjectRepo) SearchObjects(ownerID uuid.UUID, queryStr string, limit, offset int) ([]types.Object, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var res []types.Object
	for _, obj := range m.objects {
		if obj.OwnerID == ownerID && strings.Contains(strings.ToLower(obj.ObjectName), strings.ToLower(queryStr)) {
			res = append(res, *obj)
		}
	}
	return res, nil
}

func (m *mockObjectRepo) DeleteObject(objectID uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, objectID)
	return nil
}

func (m *mockObjectRepo) UpdateLastAccessed(objectID uuid.UUID, t time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if obj, exists := m.objects[objectID]; exists {
		obj.LastAccessed = t
	}
	return nil
}

// mockReplicaRepo implements ReplicaRepositoryReader
type mockReplicaRepo struct {
	mu       sync.RWMutex
	replicas map[uuid.UUID][]types.Replica
}

func newMockReplicaRepo() *mockReplicaRepo {
	return &mockReplicaRepo{replicas: make(map[uuid.UUID][]types.Replica)}
}

func (m *mockReplicaRepo) GetHealthyReplicasByObject(objectID uuid.UUID) ([]types.Replica, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var healthy []types.Replica
	for _, r := range m.replicas[objectID] {
		if r.Status == types.ReplicaStatusHealthy {
			healthy = append(healthy, r)
		}
	}
	return healthy, nil
}

func (m *mockReplicaRepo) GetReplicasByObject(objectID uuid.UUID) ([]types.Replica, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.replicas[objectID], nil
}

// mockNodeRepo implements NodeRepositoryReader
type mockNodeRepo struct {
	mu    sync.RWMutex
	nodes map[uuid.UUID]*types.StorageNode
}

func newMockNodeRepo() *mockNodeRepo {
	return &mockNodeRepo{nodes: make(map[uuid.UUID]*types.StorageNode)}
}

func (m *mockNodeRepo) GetNodeByID(nodeID uuid.UUID) (*types.StorageNode, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if n, exists := m.nodes[nodeID]; exists {
		cp := *n
		return &cp, nil
	}
	return nil, errors.New("node not found")
}

// mockAccessLogRepo implements AccessLogRepositoryRecorder
type mockAccessLogRepo struct {
	mu     sync.Mutex
	logged int
}

func (m *mockAccessLogRepo) RecordAccess(objectID *uuid.UUID, userID *uuid.UUID, responseTimeMs int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logged++
	return nil
}

// mockLogRepo implements LogRepositoryWriter
type mockLogRepo struct {
	mu     sync.Mutex
	events []string
}

func (m *mockLogRepo) InsertSystemLog(eventType, description string, severity types.LogSeverity, metadata map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, eventType)
	return nil
}

// ---------------------------------------------------------------------------
// Download Handler Tests
// ---------------------------------------------------------------------------

func TestDownloadUnauthenticated(t *testing.T) {
	handler := &ObjectDownloadHandler{}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest(http.MethodGet, "/api/objects/"+uuid.New().String(), nil)

	handler.Download(c)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected HTTP 401, got %d", w.Code)
	}

	var res types.StandardResponse
	_ = json.Unmarshal(w.Body.Bytes(), &res)
	if res.ErrorCode != types.ErrCodeAuthUnauthorized {
		t.Errorf("expected error code %s, got %s", types.ErrCodeAuthUnauthorized, res.ErrorCode)
	}
}

func TestDownloadInvalidUUID(t *testing.T) {
	handler := &ObjectDownloadHandler{}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest(http.MethodGet, "/api/objects/not-a-valid-uuid", nil)
	c.Params = gin.Params{{Key: "id", Value: "not-a-valid-uuid"}}
	c.Set(auth.ContextUserID, uuid.New())
	c.Set(auth.ContextUserRole, types.RoleUser)

	handler.Download(c)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected HTTP 400, got %d", w.Code)
	}

	var res types.StandardResponse
	_ = json.Unmarshal(w.Body.Bytes(), &res)
	if res.ErrorCode != types.ErrCodeValidationFailed {
		t.Errorf("expected %s, got %s", types.ErrCodeValidationFailed, res.ErrorCode)
	}
}

func TestDownloadObjectNotFound(t *testing.T) {
	mockObj := newMockObjectRepo()
	handler := &ObjectDownloadHandler{
		objectRepo: mockObj,
	}

	missingID := uuid.New()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest(http.MethodGet, "/api/objects/"+missingID.String(), nil)
	c.Params = gin.Params{{Key: "id", Value: missingID.String()}}
	c.Set(auth.ContextUserID, uuid.New())
	c.Set(auth.ContextUserRole, types.RoleUser)

	handler.Download(c)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected HTTP 404, got %d", w.Code)
	}

	var res types.StandardResponse
	_ = json.Unmarshal(w.Body.Bytes(), &res)
	if res.ErrorCode != types.ErrCodeObjectNotFound {
		t.Errorf("expected %s, got %s", types.ErrCodeObjectNotFound, res.ErrorCode)
	}
}

func TestDownloadForbiddenNonOwner(t *testing.T) {
	ownerID := uuid.New()
	otherUserID := uuid.New()
	objectID := uuid.New()

	mockObj := newMockObjectRepo()
	mockObj.objects[objectID] = &types.Object{
		ObjectID:   objectID,
		OwnerID:    ownerID,
		ObjectName: "private.doc",
	}

	handler := &ObjectDownloadHandler{
		objectRepo: mockObj,
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest(http.MethodGet, "/api/objects/"+objectID.String(), nil)
	c.Params = gin.Params{{Key: "id", Value: objectID.String()}}
	c.Set(auth.ContextUserID, otherUserID)
	c.Set(auth.ContextUserRole, types.RoleUser)

	handler.Download(c)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected HTTP 403 for non-owner, got %d", w.Code)
	}

	var res types.StandardResponse
	_ = json.Unmarshal(w.Body.Bytes(), &res)
	if res.ErrorCode != types.ErrCodeAuthForbidden {
		t.Errorf("expected %s, got %s", types.ErrCodeAuthForbidden, res.ErrorCode)
	}
}

func TestDownloadNoReplicas(t *testing.T) {
	userID := uuid.New()
	objectID := uuid.New()

	mockObj := newMockObjectRepo()
	mockObj.objects[objectID] = &types.Object{
		ObjectID:   objectID,
		OwnerID:    userID,
		ObjectName: "test.txt",
	}

	mockRep := newMockReplicaRepo() // empty replicas

	handler := &ObjectDownloadHandler{
		objectRepo:  mockObj,
		replicaRepo: mockRep,
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest(http.MethodGet, "/api/objects/"+objectID.String(), nil)
	c.Params = gin.Params{{Key: "id", Value: objectID.String()}}
	c.Set(auth.ContextUserID, userID)
	c.Set(auth.ContextUserRole, types.RoleUser)

	handler.Download(c)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected HTTP 503, got %d", w.Code)
	}

	var res types.StandardResponse
	_ = json.Unmarshal(w.Body.Bytes(), &res)
	if res.ErrorCode != types.ErrCodeNodeUnavailable {
		t.Errorf("expected %s, got %s", types.ErrCodeNodeUnavailable, res.ErrorCode)
	}
}

func TestDownloadFailoverSuccess(t *testing.T) {
	userID := uuid.New()
	objectID := uuid.New()

	fileContent := []byte("critical distributed object data")
	hasher := sha256.New()
	hasher.Write(fileContent)
	checksum := hex.EncodeToString(hasher.Sum(nil))

	mockObj := newMockObjectRepo()
	mockObj.objects[objectID] = &types.Object{
		ObjectID:   objectID,
		OwnerID:    userID,
		ObjectName: "critical.bin",
		FileSize:   int64(len(fileContent)),
		MimeType:   "application/octet-stream",
		Checksum:   checksum,
	}

	node1ID := uuid.New()
	node2ID := uuid.New()

	mockNode := newMockNodeRepo()
	mockNode.nodes[node1ID] = &types.StorageNode{
		NodeID:   node1ID,
		Hostname: "node-1",
		Status:   types.NodeStatusOnline,
	}
	mockNode.nodes[node2ID] = &types.StorageNode{
		NodeID:   node2ID,
		Hostname: "node-2",
		Status:   types.NodeStatusOnline,
	}

	mockRep := newMockReplicaRepo()
	mockRep.replicas[objectID] = []types.Replica{
		{ReplicaID: uuid.New(), ObjectID: objectID, NodeID: node1ID, Status: types.ReplicaStatusHealthy},
		{ReplicaID: uuid.New(), ObjectID: objectID, NodeID: node2ID, Status: types.ReplicaStatusHealthy},
	}

	mockClient := newDownloadMockStorageClient()
	// Node 1 fails with network error
	mockClient.fetchErrPerNode[node1ID] = errors.New("connection refused by storage daemon")
	// Node 2 succeeds with valid file bytes
	mockClient.fetchPerNode[node2ID] = fileContent

	mockAccess := &mockAccessLogRepo{}
	mockLogs := &mockLogRepo{}

	handler := &ObjectDownloadHandler{
		objectRepo:    mockObj,
		replicaRepo:   mockRep,
		nodeRepo:      mockNode,
		accessLogRepo: mockAccess,
		logRepo:       mockLogs,
		storageClient: mockClient,
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest(http.MethodGet, "/api/objects/"+objectID.String(), nil)
	c.Params = gin.Params{{Key: "id", Value: objectID.String()}}
	c.Set(auth.ContextUserID, userID)
	c.Set(auth.ContextUserRole, types.RoleUser)

	handler.Download(c)

	if w.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d: %s", w.Code, w.Body.String())
	}

	if !bytes.Equal(w.Body.Bytes(), fileContent) {
		t.Errorf("expected body %q, got %q", string(fileContent), w.Body.String())
	}

	if w.Header().Get("X-Checksum-SHA256") != checksum {
		t.Errorf("expected X-Checksum-SHA256 %s, got %s", checksum, w.Header().Get("X-Checksum-SHA256"))
	}
	if !strings.Contains(w.Header().Get("Content-Disposition"), "critical.bin") {
		t.Errorf("expected Content-Disposition with critical.bin, got %s", w.Header().Get("Content-Disposition"))
	}
}

func TestDownloadChecksumMismatch(t *testing.T) {
	userID := uuid.New()
	objectID := uuid.New()

	validContent := []byte("original content")
	hasher := sha256.New()
	hasher.Write(validContent)
	expectedChecksum := hex.EncodeToString(hasher.Sum(nil))

	mockObj := newMockObjectRepo()
	mockObj.objects[objectID] = &types.Object{
		ObjectID:   objectID,
		OwnerID:    userID,
		ObjectName: "corrupted.bin",
		FileSize:   int64(len(validContent)),
		MimeType:   "application/octet-stream",
		Checksum:   expectedChecksum,
	}

	node1ID := uuid.New()
	mockNode := newMockNodeRepo()
	mockNode.nodes[node1ID] = &types.StorageNode{
		NodeID:   node1ID,
		Hostname: "node-1",
		Status:   types.NodeStatusOnline,
	}

	mockRep := newMockReplicaRepo()
	mockRep.replicas[objectID] = []types.Replica{
		{ReplicaID: uuid.New(), ObjectID: objectID, NodeID: node1ID, Status: types.ReplicaStatusHealthy},
	}

	mockClient := newDownloadMockStorageClient()
	// Storage client returns tampered content
	mockClient.fetchPerNode[node1ID] = []byte("corrupted tampered data")

	mockLogs := &mockLogRepo{}

	handler := &ObjectDownloadHandler{
		objectRepo:    mockObj,
		replicaRepo:   mockRep,
		nodeRepo:      mockNode,
		logRepo:       mockLogs,
		storageClient: mockClient,
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest(http.MethodGet, "/api/objects/"+objectID.String(), nil)
	c.Params = gin.Params{{Key: "id", Value: objectID.String()}}
	c.Set(auth.ContextUserID, userID)
	c.Set(auth.ContextUserRole, types.RoleUser)

	handler.Download(c)

	if w.Code != http.StatusConflict {
		t.Fatalf("expected HTTP 409 Conflict for corruption, got %d", w.Code)
	}

	var res types.StandardResponse
	_ = json.Unmarshal(w.Body.Bytes(), &res)
	if res.ErrorCode != types.ErrCodeReplicaFailure {
		t.Errorf("expected error code %s, got %s", types.ErrCodeReplicaFailure, res.ErrorCode)
	}
}

// ---------------------------------------------------------------------------
// List and Search Tests
// ---------------------------------------------------------------------------

func TestListObjectsSuccess(t *testing.T) {
	userID := uuid.New()
	mockObj := newMockObjectRepo()
	mockObj.objects[uuid.New()] = &types.Object{ObjectID: uuid.New(), OwnerID: userID, ObjectName: "doc1.pdf"}
	mockObj.objects[uuid.New()] = &types.Object{ObjectID: uuid.New(), OwnerID: userID, ObjectName: "doc2.pdf"}

	handler := &ObjectDownloadHandler{objectRepo: mockObj}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest(http.MethodGet, "/api/objects?limit=10&offset=0", nil)
	c.Set(auth.ContextUserID, userID)

	handler.List(c)

	if w.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", w.Code)
	}

	var res types.StandardResponse
	_ = json.Unmarshal(w.Body.Bytes(), &res)
	if !res.Success {
		t.Errorf("expected success true, got false")
	}
}

func TestSearchMissingQuery(t *testing.T) {
	handler := &ObjectDownloadHandler{}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest(http.MethodGet, "/api/objects/search", nil)
	c.Set(auth.ContextUserID, uuid.New())

	handler.Search(c)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected HTTP 400 for missing query, got %d", w.Code)
	}

	var res types.StandardResponse
	_ = json.Unmarshal(w.Body.Bytes(), &res)
	if res.ErrorCode != types.ErrCodeValidationFailed {
		t.Errorf("expected error code %s, got %s", types.ErrCodeValidationFailed, res.ErrorCode)
	}
}

func TestSearchObjectsSuccess(t *testing.T) {
	userID := uuid.New()
	mockObj := newMockObjectRepo()
	mockObj.objects[uuid.New()] = &types.Object{ObjectID: uuid.New(), OwnerID: userID, ObjectName: "invoice_2026.pdf"}
	mockObj.objects[uuid.New()] = &types.Object{ObjectID: uuid.New(), OwnerID: userID, ObjectName: "photo.jpg"}

	handler := &ObjectDownloadHandler{objectRepo: mockObj}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest(http.MethodGet, "/api/objects/search?q=invoice", nil)
	c.Set(auth.ContextUserID, userID)

	handler.Search(c)

	if w.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", w.Code)
	}

	var res struct {
		Success bool `json:"success"`
		Data    struct {
			Objects []types.Object `json:"objects"`
			Count   int            `json:"count"`
		} `json:"data"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &res)

	if !res.Success || res.Data.Count != 1 {
		t.Errorf("expected 1 search result, got %d", res.Data.Count)
	}
}

// ---------------------------------------------------------------------------
// Delete Handler Tests
// ---------------------------------------------------------------------------

func TestDeleteForbiddenNonOwner(t *testing.T) {
	ownerID := uuid.New()
	otherUserID := uuid.New()
	objectID := uuid.New()

	mockObj := newMockObjectRepo()
	mockObj.objects[objectID] = &types.Object{
		ObjectID:   objectID,
		OwnerID:    ownerID,
		ObjectName: "confidential.pdf",
	}

	handler := &ObjectDeleteHandler{
		objectRepo: mockObj,
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest(http.MethodDelete, "/api/objects/"+objectID.String(), nil)
	c.Params = gin.Params{{Key: "id", Value: objectID.String()}}
	c.Set(auth.ContextUserID, otherUserID)
	c.Set(auth.ContextUserRole, types.RoleUser)

	handler.Delete(c)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected HTTP 403, got %d", w.Code)
	}

	var res types.StandardResponse
	_ = json.Unmarshal(w.Body.Bytes(), &res)
	if res.ErrorCode != types.ErrCodeAuthForbidden {
		t.Errorf("expected error code %s, got %s", types.ErrCodeAuthForbidden, res.ErrorCode)
	}
}

func TestDeleteSuccess(t *testing.T) {
	userID := uuid.New()
	objectID := uuid.New()

	mockObj := newMockObjectRepo()
	mockObj.objects[objectID] = &types.Object{
		ObjectID:   objectID,
		OwnerID:    userID,
		ObjectName: "file_to_delete.txt",
	}

	nodeID := uuid.New()
	mockNode := newMockNodeRepo()
	mockNode.nodes[nodeID] = &types.StorageNode{
		NodeID:   nodeID,
		Hostname: "node-1",
		Status:   types.NodeStatusOnline,
	}

	mockRep := newMockReplicaRepo()
	mockRep.replicas[objectID] = []types.Replica{
		{ReplicaID: uuid.New(), ObjectID: objectID, NodeID: nodeID, Status: types.ReplicaStatusHealthy},
	}

	mockClient := newDownloadMockStorageClient()
	mockLogs := &mockLogRepo{}

	handler := &ObjectDeleteHandler{
		objectRepo:    mockObj,
		replicaRepo:   mockRep,
		nodeRepo:      mockNode,
		logRepo:       mockLogs,
		storageClient: mockClient,
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest(http.MethodDelete, "/api/objects/"+objectID.String(), nil)
	c.Params = gin.Params{{Key: "id", Value: objectID.String()}}
	c.Set(auth.ContextUserID, userID)
	c.Set(auth.ContextUserRole, types.RoleUser)

	handler.Delete(c)

	if w.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", w.Code)
	}

	// Verify object was deleted from repository
	if _, err := mockObj.GetObjectByID(objectID); err == nil {
		t.Error("expected object to be deleted from repo")
	}

	// Verify physical chunk delete was triggered
	if len(mockClient.deletedChunks) != 1 || mockClient.deletedChunks[0] != objectID {
		t.Errorf("expected delete chunk call for %s, got %v", objectID, mockClient.deletedChunks)
	}
}
