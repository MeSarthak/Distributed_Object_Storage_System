package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"distributed-storage/pkg/auth"
	"distributed-storage/pkg/types"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// fakeStorageClient implements StorageClientInterface for testing.
type fakeStorageClient struct {
	storeErr  error
	deleteErr error
	fetchData []byte
	fetchErr  error
}

func (f *fakeStorageClient) StoreChunk(ctx context.Context, node types.StorageNode, objectID uuid.UUID, fileData []byte) error {
	return f.storeErr
}

func (f *fakeStorageClient) FetchChunk(ctx context.Context, node types.StorageNode, objectID uuid.UUID) (io.ReadCloser, int64, error) {
	if f.fetchErr != nil {
		return nil, 0, f.fetchErr
	}
	return io.NopCloser(bytes.NewReader(f.fetchData)), int64(len(f.fetchData)), nil
}

func (f *fakeStorageClient) DeleteChunk(ctx context.Context, node types.StorageNode, objectID uuid.UUID) error {
	return f.deleteErr
}

func (f *fakeStorageClient) VerifyChunk(ctx context.Context, node types.StorageNode, objectID uuid.UUID) (*types.VerifyResult, error) {
	return &types.VerifyResult{Valid: true}, nil
}

func init() {
	gin.SetMode(gin.TestMode)
}

// TestUploadUnauthenticated verifies that an upload request without user identity is rejected.
func TestUploadUnauthenticated(t *testing.T) {
	handler := &ObjectUploadHandler{}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest(http.MethodPost, "/api/objects", nil)

	handler.Upload(c)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected HTTP 401, got %d", w.Code)
	}

	var res types.StandardResponse
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if res.Success || res.ErrorCode != types.ErrCodeAuthUnauthorized {
		t.Errorf("expected error code %s, got %s", types.ErrCodeAuthUnauthorized, res.ErrorCode)
	}
}

// TestUploadMissingFile verifies that a request without a multipart file is rejected with 400.
func TestUploadMissingFile(t *testing.T) {
	handler := &ObjectUploadHandler{}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	_ = writer.WriteField("object_name", "test.txt")
	writer.Close()

	c.Request, _ = http.NewRequest(http.MethodPost, "/api/objects", &buf)
	c.Request.Header.Set("Content-Type", writer.FormDataContentType())
	c.Set(auth.ContextUserID, uuid.New())

	handler.Upload(c)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected HTTP 400, got %d", w.Code)
	}

	var res types.StandardResponse
	_ = json.Unmarshal(w.Body.Bytes(), &res)
	if res.ErrorCode != types.ErrCodeValidationFailed {
		t.Errorf("expected %s, got %s", types.ErrCodeValidationFailed, res.ErrorCode)
	}
}

// TestUploadNoNodesAvailable verifies that 503 is returned when no storage nodes are online.
func TestUploadNoNodesAvailable(t *testing.T) {
	emptyReader := &fakeNodeReader{nodes: []types.StorageNode{}}
	pe := NewPlacementEngine(emptyReader, defaultPlacementCfg())

	handler := &ObjectUploadHandler{
		placementEngine: pe,
	}

	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	part, _ := writer.CreateFormFile("file", "test.txt")
	_, _ = part.Write([]byte("hello distributed storage"))
	writer.Close()

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest(http.MethodPost, "/api/objects", &buf)
	c.Request.Header.Set("Content-Type", writer.FormDataContentType())
	c.Set(auth.ContextUserID, uuid.New())

	handler.Upload(c)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected HTTP 503, got %d", w.Code)
	}

	var res types.StandardResponse
	_ = json.Unmarshal(w.Body.Bytes(), &res)
	if res.ErrorCode != types.ErrCodeNodeUnavailable {
		t.Errorf("expected %s, got %s", types.ErrCodeNodeUnavailable, res.ErrorCode)
	}
}

// TestUploadAllNodesFail verifies that 500 ErrCodeReplicaFailure is returned when all nodes fail to store.
func TestUploadAllNodesFail(t *testing.T) {
	nodeList := threeNodeCluster()
	reader := &fakeNodeReader{nodes: nodeList}
	pe := NewPlacementEngine(reader, defaultPlacementCfg())

	fakeClient := &fakeStorageClient{
		storeErr: errors.New("connection refused by storage daemon"),
	}

	handler := &ObjectUploadHandler{
		placementEngine: pe,
		storageClient:   fakeClient,
	}

	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	part, _ := writer.CreateFormFile("file", "payload.bin")
	_, _ = part.Write([]byte("sample payload data"))
	writer.Close()

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest(http.MethodPost, "/api/objects", &buf)
	c.Request.Header.Set("Content-Type", writer.FormDataContentType())
	c.Set(auth.ContextUserID, uuid.New())

	handler.Upload(c)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected HTTP 500, got %d", w.Code)
	}

	var res types.StandardResponse
	_ = json.Unmarshal(w.Body.Bytes(), &res)
	if res.ErrorCode != types.ErrCodeReplicaFailure {
		t.Errorf("expected error code %s, got %s", types.ErrCodeReplicaFailure, res.ErrorCode)
	}
}
