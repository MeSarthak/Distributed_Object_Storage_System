package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"distributed-storage/pkg/auth"
	"distributed-storage/pkg/config"
	"distributed-storage/pkg/coordinator"
	"distributed-storage/pkg/node"
	"distributed-storage/pkg/types"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// In-memory test store for user repository in E2E tests
type memUserStore struct {
	users map[string]*types.User
}

func (m *memUserStore) CreateUser(user *types.User) error {
	m.users[user.Email] = user
	return nil
}

func (m *memUserStore) GetUserByEmail(email string) (*types.User, error) {
	u, ok := m.users[email]
	if !ok {
		return nil, fmt.Errorf("user not found")
	}
	return u, nil
}

func (m *memUserStore) GetUserByID(userID uuid.UUID) (*types.User, error) {
	for _, u := range m.users {
		if u.UserID == userID {
			return u, nil
		}
	}
	return nil, fmt.Errorf("user not found")
}

func (m *memUserStore) EmailExists(email string) (bool, error) {
	_, ok := m.users[email]
	return ok, nil
}

// TestCoordinatorAndStorageNodeE2E verifies the core workflows of Phase 2
func TestCoordinatorAndStorageNodeE2E(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// 1. Setup Auth & Coordinator router
	authCfg := config.AuthConfig{
		JWTSecret:       "super-secret-test-jwt-key-min-16-chars",
		ExpirationHours: 24,
		BcryptCost:      4, // Low cost for fast test
	}
	memStore := &memUserStore{users: make(map[string]*types.User)}
	authService := auth.NewAuthService(memStore, authCfg)
	authHandler := auth.NewAuthHandler(authService, nil)
	authMiddleware := auth.AuthMiddleware(authService)

	router := gin.New()
	authHandler.RegisterRoutes(router, authMiddleware)

	// Placement inspection endpoint
	placementCfg := config.PlacementConfig{
		WeightStorage:       0.35,
		WeightCPU:           0.20,
		WeightRAM:           0.15,
		WeightLatency:       0.15,
		WeightHealth:        0.15,
		MaxCPUThreshold:     90.0,
		MaxRAMThreshold:     90.0,
		MinFreeStorageBytes: 100 * 1024 * 1024,
	}
	placementEngine := &coordinator.PlacementEngine{}
	// Direct evaluate route
	router.GET("/api/cluster/placement/evaluate", func(c *gin.Context) {
		sampleNodes := []types.StorageNode{
			{
				NodeID:       uuid.New(),
				Hostname:     "storage-node-1",
				TotalStorage: 10 * 1024 * 1024 * 1024,
				UsedStorage:  2 * 1024 * 1024 * 1024,
				CPUUsage:     15.0,
				MemoryUsage:  20.0,
				Latency:      5.0,
				Status:       types.NodeStatusOnline,
			},
		}
		eng := coordinator.NewPlacementEngine(nil, placementCfg)
		evals := eng.EvaluateNodes(sampleNodes, 0, nil)
		c.JSON(http.StatusOK, types.StandardResponse{
			Success: true,
			Data:    evals,
		})
	})

	// 2. Test User Registration
	regBody, _ := json.Marshal(types.RegisterRequest{
		Name:     "Test Admin",
		Email:    "admin@example.com",
		Password: "strongPassword123",
		Role:     types.RoleAdmin,
	})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/auth/register", bytes.NewReader(regBody))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("registration failed: status %d, body: %s", w.Code, w.Body.String())
	}

	var regResp types.StandardResponse
	_ = json.Unmarshal(w.Body.Bytes(), &regResp)
	if !regResp.Success {
		t.Fatalf("expected registration success")
	}

	// 3. Test User Login & Token Generation
	loginBody, _ := json.Marshal(types.LoginRequest{
		Email:    "admin@example.com",
		Password: "strongPassword123",
	})
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/auth/login", bytes.NewReader(loginBody))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("login failed: status %d, body: %s", w.Code, w.Body.String())
	}

	var loginResp struct {
		Success bool `json:"success"`
		Data    types.AuthResponse
	}
	_ = json.Unmarshal(w.Body.Bytes(), &loginResp)
	jwtToken := loginResp.Data.Token
	if jwtToken == "" {
		t.Fatalf("expected non-empty JWT token")
	}

	// 4. Test Protected Route with JWT (Validate Token)
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/auth/validate", nil)
	req.Header.Set("Authorization", "Bearer "+jwtToken)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("validate token failed: status %d, body: %s", w.Code, w.Body.String())
	}

	// 5. Test Placement Evaluation Endpoint
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/cluster/placement/evaluate", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("placement evaluation failed: status %d, body: %s", w.Code, w.Body.String())
	}

	// 6. Test Storage Node Engine (Store -> Get -> Verify -> Delete)
	tempDir := t.TempDir()
	storageRouter := gin.New()
	storageHandler := node.NewStorageHandler(tempDir)
	storageHandler.RegisterRoutes(storageRouter)

	testObjID := uuid.New().String()
	testContent := "Distributed Object Storage Prototype Chunk Payload Data"

	// Multipart upload
	var b bytes.Buffer
	b.WriteString("--boundary\r\n")
	b.WriteString(fmt.Sprintf("Content-Disposition: form-data; name=\"object_id\"\r\n\r\n%s\r\n", testObjID))
	b.WriteString("--boundary\r\n")
	b.WriteString("Content-Disposition: form-data; name=\"file\"; filename=\"test.bin\"\r\n")
	b.WriteString("Content-Type: application/octet-stream\r\n\r\n")
	b.WriteString(testContent + "\r\n")
	b.WriteString("--boundary--\r\n")

	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/internal/storage/store", &b)
	req.Header.Set("Content-Type", "multipart/form-data; boundary=boundary")
	storageRouter.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("storage store failed: status %d, body: %s", w.Code, w.Body.String())
	}

	// Verify Chunk On Disk
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/internal/storage/%s/verify", testObjID), nil)
	storageRouter.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("storage verify failed: status %d, body: %s", w.Code, w.Body.String())
	}

	// Stream Chunk Download
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/internal/storage/%s", testObjID), nil)
	storageRouter.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("storage get failed: status %d", w.Code)
	}
	if w.Body.String() != testContent {
		t.Fatalf("stream content mismatch: expected '%s', got '%s'", testContent, w.Body.String())
	}

	// Delete Chunk
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/internal/storage/%s", testObjID), nil)
	storageRouter.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("storage delete failed: status %d", w.Code)
	}

	// Ensure Get returns 404 after deletion
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/internal/storage/%s", testObjID), nil)
	storageRouter.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", w.Code)
	}

	_ = time.Now()
	_ = placementEngine
}
