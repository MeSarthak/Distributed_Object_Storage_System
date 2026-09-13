package auth

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"distributed-storage/pkg/config"
	"distributed-storage/pkg/types"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// fakeUserStore implements UserStore in-memory for testing without Postgres.
type fakeUserStore struct {
	mu      sync.RWMutex
	byEmail map[string]*types.User
	byID    map[uuid.UUID]*types.User
}

func newFakeUserStore() *fakeUserStore {
	return &fakeUserStore{
		byEmail: make(map[string]*types.User),
		byID:    make(map[uuid.UUID]*types.User),
	}
}

func (f *fakeUserStore) CreateUser(u *types.User) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	email := strings.ToLower(u.Email)
	if _, exists := f.byEmail[email]; exists {
		return fmt.Errorf("duplicate email: %s", email)
	}

	copied := *u
	f.byEmail[email] = &copied
	f.byID[u.UserID] = &copied
	return nil
}

func (f *fakeUserStore) GetUserByEmail(email string) (*types.User, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	u, exists := f.byEmail[strings.ToLower(email)]
	if !exists {
		return nil, sql.ErrNoRows
	}
	copied := *u
	return &copied, nil
}

func (f *fakeUserStore) GetUserByID(userID uuid.UUID) (*types.User, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	u, exists := f.byID[userID]
	if !exists {
		return nil, sql.ErrNoRows
	}
	copied := *u
	return &copied, nil
}

func (f *fakeUserStore) EmailExists(email string) (bool, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	_, exists := f.byEmail[strings.ToLower(email)]
	return exists, nil
}

func testAuthConfig() config.AuthConfig {
	return config.AuthConfig{
		JWTSecret:       "test-secret-at-least-32-chars-long!",
		ExpirationHours: 2,
		BcryptCost:      4, // Low cost for rapid test execution
	}
}

func setupTestAuthService() (*AuthService, *fakeUserStore) {
	store := newFakeUserStore()
	svc := NewAuthService(store, testAuthConfig())
	return svc, store
}

// ---------------------------------------------------------------------------
// Registration Tests
// ---------------------------------------------------------------------------

func TestRegisterSuccess(t *testing.T) {
	svc, _ := setupTestAuthService()

	req := types.RegisterRequest{
		Name:     "Test User",
		Email:    "test@example.com",
		Password: "password123",
	}

	res, err := svc.Register(req)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	if res.Token == "" {
		t.Fatal("expected non-empty JWT token")
	}
	if res.User.Name != "Test User" {
		t.Errorf("expected user name 'Test User', got '%s'", res.User.Name)
	}
	if res.User.Email != "test@example.com" {
		t.Errorf("expected email 'test@example.com', got '%s'", res.User.Email)
	}
	if res.User.Role != types.RoleUser {
		t.Errorf("expected default role USER, got '%s'", res.User.Role)
	}
	if res.User.PasswordHash != "" {
		t.Errorf("expected password hash to be stripped from response, got '%s'", res.User.PasswordHash)
	}

	claims, err := svc.ValidateToken(res.Token)
	if err != nil {
		t.Fatalf("token validation failed: %v", err)
	}
	if claims.Email != "test@example.com" {
		t.Errorf("expected claims email 'test@example.com', got '%s'", claims.Email)
	}
}

func TestRegisterAdminRole(t *testing.T) {
	svc, _ := setupTestAuthService()

	req := types.RegisterRequest{
		Name:     "Admin User",
		Email:    "admin@example.com",
		Password: "adminpassword",
		Role:     types.RoleAdmin,
	}

	res, err := svc.Register(req)
	if err != nil {
		t.Fatalf("Register admin failed: %v", err)
	}
	if res.User.Role != types.RoleAdmin {
		t.Errorf("expected role ADMIN, got '%s'", res.User.Role)
	}
}

func TestRegisterValidationErrors(t *testing.T) {
	svc, _ := setupTestAuthService()

	tests := []struct {
		name string
		req  types.RegisterRequest
	}{
		{"empty name", types.RegisterRequest{Name: "", Email: "user@example.com", Password: "password123"}},
		{"empty email", types.RegisterRequest{Name: "User", Email: "", Password: "password123"}},
		{"invalid email", types.RegisterRequest{Name: "User", Email: "invalid-email", Password: "password123"}},
		{"short password", types.RegisterRequest{Name: "User", Email: "user@example.com", Password: "123"}},
		{"invalid role", types.RegisterRequest{Name: "User", Email: "user@example.com", Password: "password123", Role: "SUPERUSER"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.Register(tc.req)
			if err == nil {
				t.Errorf("expected validation error for %s, but got nil", tc.name)
			}
		})
	}
}

func TestRegisterDuplicateEmail(t *testing.T) {
	svc, _ := setupTestAuthService()

	req := types.RegisterRequest{
		Name:     "User One",
		Email:    "dup@example.com",
		Password: "password123",
	}

	if _, err := svc.Register(req); err != nil {
		t.Fatalf("first registration failed: %v", err)
	}

	// Try registering again with different casing
	req2 := types.RegisterRequest{
		Name:     "User Two",
		Email:    "DUP@example.com",
		Password: "password456",
	}

	if _, err := svc.Register(req2); err == nil {
		t.Fatal("expected duplicate registration to fail, but succeeded")
	}
}

// ---------------------------------------------------------------------------
// Login Tests
// ---------------------------------------------------------------------------

func TestLoginSuccess(t *testing.T) {
	svc, _ := setupTestAuthService()

	_, err := svc.Register(types.RegisterRequest{
		Name:     "Login User",
		Email:    "login@example.com",
		Password: "correctpassword",
	})
	if err != nil {
		t.Fatalf("seed user register failed: %v", err)
	}

	loginRes, err := svc.Login(types.LoginRequest{
		Email:    "login@example.com",
		Password: "correctpassword",
	})
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}

	if loginRes.Token == "" {
		t.Fatal("expected valid token upon login")
	}
	if loginRes.User.Email != "login@example.com" {
		t.Errorf("expected email login@example.com, got %s", loginRes.User.Email)
	}
}

func TestLoginWrongPassword(t *testing.T) {
	svc, _ := setupTestAuthService()

	_, err := svc.Register(types.RegisterRequest{
		Name:     "Login User",
		Email:    "login@example.com",
		Password: "correctpassword",
	})
	if err != nil {
		t.Fatalf("seed user register failed: %v", err)
	}

	_, err = svc.Login(types.LoginRequest{
		Email:    "login@example.com",
		Password: "wrongpassword",
	})
	if err == nil {
		t.Fatal("expected login failure for wrong password")
	}
}

func TestLoginNonExistentEmail(t *testing.T) {
	svc, _ := setupTestAuthService()

	_, err := svc.Login(types.LoginRequest{
		Email:    "nonexistent@example.com",
		Password: "password123",
	})
	if err == nil {
		t.Fatal("expected login failure for non-existent email")
	}
}

// ---------------------------------------------------------------------------
// Token Validation Tests
// ---------------------------------------------------------------------------

func TestValidateTokenTampering(t *testing.T) {
	svc, _ := setupTestAuthService()

	res, err := svc.Register(types.RegisterRequest{
		Name:     "Tamper Test",
		Email:    "tamper@example.com",
		Password: "password123",
	})
	if err != nil {
		t.Fatalf("registration failed: %v", err)
	}

	// Tamper with the token string
	tamperedToken := res.Token + "tampered"
	if _, err := svc.ValidateToken(tamperedToken); err == nil {
		t.Fatal("expected validation failure on tampered token")
	}
}

// ---------------------------------------------------------------------------
// Middleware & RBAC Tests
// ---------------------------------------------------------------------------

func TestAuthMiddlewareAndRBAC(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc, _ := setupTestAuthService()

	// Seed USER
	userRes, err := svc.Register(types.RegisterRequest{
		Name:     "Regular User",
		Email:    "reg@example.com",
		Password: "password123",
		Role:     types.RoleUser,
	})
	if err != nil {
		t.Fatalf("register regular user failed: %v", err)
	}

	// Seed ADMIN
	adminRes, err := svc.Register(types.RegisterRequest{
		Name:     "Admin User",
		Email:    "adm@example.com",
		Password: "password123",
		Role:     types.RoleAdmin,
	})
	if err != nil {
		t.Fatalf("register admin user failed: %v", err)
	}

	// Router setup
	router := gin.New()
	authMiddleware := AuthMiddleware(svc)

	protected := router.Group("/protected")
	protected.Use(authMiddleware)
	{
		protected.GET("/user-only", func(c *gin.Context) {
			c.JSON(http.StatusOK, gin.H{"status": "ok"})
		})
		protected.GET("/admin-only", RequireRole(types.RoleAdmin), func(c *gin.Context) {
			c.JSON(http.StatusOK, gin.H{"status": "admin_ok"})
		})
	}

	// Case 1: Missing Authorization header -> 401
	w1 := httptest.NewRecorder()
	req1, _ := http.NewRequest(http.MethodGet, "/protected/user-only", nil)
	router.ServeHTTP(w1, req1)
	if w1.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for missing header, got %d", w1.Code)
	}

	// Case 2: Malformed Bearer header -> 401
	w2 := httptest.NewRecorder()
	req2, _ := http.NewRequest(http.MethodGet, "/protected/user-only", nil)
	req2.Header.Set("Authorization", "Basic 12345")
	router.ServeHTTP(w2, req2)
	if w2.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for malformed header, got %d", w2.Code)
	}

	// Case 3: Valid User token on user-only endpoint -> 200
	w3 := httptest.NewRecorder()
	req3, _ := http.NewRequest(http.MethodGet, "/protected/user-only", nil)
	req3.Header.Set("Authorization", "Bearer "+userRes.Token)
	router.ServeHTTP(w3, req3)
	if w3.Code != http.StatusOK {
		t.Errorf("expected 200 with valid user token, got %d", w3.Code)
	}

	// Case 4: Regular User token on admin-only endpoint -> 403
	w4 := httptest.NewRecorder()
	req4, _ := http.NewRequest(http.MethodGet, "/protected/admin-only", nil)
	req4.Header.Set("Authorization", "Bearer "+userRes.Token)
	router.ServeHTTP(w4, req4)
	if w4.Code != http.StatusForbidden {
		t.Errorf("expected 403 for user accessing admin endpoint, got %d", w4.Code)
	}

	// Case 5: Admin token on admin-only endpoint -> 200
	w5 := httptest.NewRecorder()
	req5, _ := http.NewRequest(http.MethodGet, "/protected/admin-only", nil)
	req5.Header.Set("Authorization", "Bearer "+adminRes.Token)
	router.ServeHTTP(w5, req5)
	if w5.Code != http.StatusOK {
		t.Errorf("expected 200 for admin accessing admin endpoint, got %d", w5.Code)
	}
}

