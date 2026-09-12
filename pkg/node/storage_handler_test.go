package node

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// setupTestRouter creates a Gin router with StorageHandler routes wired up
// against a temporary directory.  Returns the router and a cleanup function.
func setupTestRouter(t *testing.T) (*gin.Engine, string) {
	t.Helper()
	dir := t.TempDir()

	gin.SetMode(gin.TestMode)
	router := gin.New()
	handler := NewStorageHandler(dir)
	handler.RegisterRoutes(router)
	return router, dir
}

// --------------------------------------------------------------------------
// UUID validation tests (path-traversal prevention)
// --------------------------------------------------------------------------

// TestStoreRejectsNonUUIDObjectID verifies that POST /internal/storage/store
// returns 400 when object_id is not a valid UUID.
func TestStoreRejectsNonUUIDObjectID(t *testing.T) {
	router, _ := setupTestRouter(t)

	body := strings.NewReader("--boundary\r\nContent-Disposition: form-data; name=\"object_id\"\r\n\r\n../etc/passwd\r\n--boundary--\r\n")
	req := httptest.NewRequest(http.MethodPost, "/internal/storage/store", body)
	req.Header.Set("Content-Type", "multipart/form-data; boundary=boundary")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for non-UUID object_id, got %d", w.Code)
	}
}

// TestGetRejectsNonUUIDID verifies that GET /internal/storage/:id returns 400
// when :id is not a valid UUID (prevents path traversal such as "../etc/passwd").
func TestGetRejectsNonUUIDID(t *testing.T) {
	router, _ := setupTestRouter(t)

	req := httptest.NewRequest(http.MethodGet, "/internal/storage/..%2Fetc%2Fpasswd", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Gin will URL-decode this to "../etc/passwd" which uuid.Parse will reject.
	if w.Code == http.StatusOK {
		t.Error("expected non-200 for path traversal attempt in GET :id")
	}
}

// TestDeleteRejectsNonUUIDID verifies that DELETE /internal/storage/:id returns 400
// for non-UUID :id.
func TestDeleteRejectsNonUUIDID(t *testing.T) {
	router, _ := setupTestRouter(t)

	req := httptest.NewRequest(http.MethodDelete, "/internal/storage/not-a-uuid", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for non-UUID id, got %d", w.Code)
	}
}

// --------------------------------------------------------------------------
// Functional storage tests
// --------------------------------------------------------------------------

// TestGetReturns404ForMissingObject verifies that GET returns 404 when a valid
// UUID is provided but the object does not exist on this node.
func TestGetReturns404ForMissingObject(t *testing.T) {
	router, _ := setupTestRouter(t)

	req := httptest.NewRequest(http.MethodGet, "/internal/storage/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404 for missing object, got %d", w.Code)
	}
}

// TestDeleteReturns404ForMissingObject verifies that DELETE returns 404 when the
// object does not exist.
func TestDeleteReturns404ForMissingObject(t *testing.T) {
	router, _ := setupTestRouter(t)

	req := httptest.NewRequest(http.MethodDelete, "/internal/storage/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404 for missing object, got %d", w.Code)
	}
}

// TestGetReturnsStoredFile verifies that a file pre-written to the storage
// directory is returned by GET /internal/storage/:id.
func TestGetReturnsStoredFile(t *testing.T) {
	router, dir := setupTestRouter(t)

	const objectID = "12345678-1234-1234-1234-123456789abc"
	content := []byte("test file content for storage handler")
	filePath := filepath.Join(dir, objectID)
	if err := os.WriteFile(filePath, content, 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/internal/storage/"+objectID, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 for existing object, got %d: %s", w.Code, w.Body.String())
	}
	if w.Body.Len() != len(content) {
		t.Errorf("expected %d bytes, got %d", len(content), w.Body.Len())
	}
}

// TestDeleteRemovesFile verifies that DELETE /internal/storage/:id actually
// removes the file from disk.
func TestDeleteRemovesFile(t *testing.T) {
	router, dir := setupTestRouter(t)

	const objectID = "abcdef01-abcd-abcd-abcd-abcdef012345"
	filePath := filepath.Join(dir, objectID)
	if err := os.WriteFile(filePath, []byte("data"), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, "/internal/storage/"+objectID, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 on delete, got %d: %s", w.Code, w.Body.String())
	}

	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		t.Error("file still exists after DELETE")
	}
}
