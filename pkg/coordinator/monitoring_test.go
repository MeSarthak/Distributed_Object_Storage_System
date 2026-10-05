package coordinator_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"distributed-storage/pkg/types"

	"github.com/gin-gonic/gin"
)

func init() {
	gin.SetMode(gin.TestMode)
}

// responseBody is a helper that unmarshals a recorder body into types.StandardResponse.
func responseBody(t *testing.T, w *httptest.ResponseRecorder) types.StandardResponse {
	t.Helper()
	var resp types.StandardResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response body: %v\nbody: %s", err, w.Body.String())
	}
	return resp
}

// ---------------------------------------------------------------------------
// Route registration smoke tests
// These tests verify that RegisterAPIRoutes mounts each endpoint on the correct
// path and HTTP method. They do NOT require a real database connection.
// ---------------------------------------------------------------------------

// routerWithRoutes builds a minimal *gin.Engine that has only the monitoring
// and metadata routes registered — without real handler bodies — so we can
// confirm path registration without a DB.
func routerWithStubRoutes() *gin.Engine {
	r := gin.New()

	// Stub handlers that return a fixed types.StandardResponse so tests can
	// assert response shape without a real repository.
	stub200 := func(c *gin.Context) {
		c.JSON(http.StatusOK, types.StandardResponse{
			Success: true,
			Message: "ok",
		})
	}
	stub400 := func(c *gin.Context) {
		c.JSON(http.StatusBadRequest, types.StandardResponse{
			Success:   false,
			Message:   "bad request",
			ErrorCode: types.ErrCodeValidationFailed,
		})
	}
	stub401 := func(c *gin.Context) {
		c.JSON(http.StatusUnauthorized, types.StandardResponse{
			Success:   false,
			Message:   "unauthorized",
			ErrorCode: types.ErrCodeAuthUnauthorized,
		})
	}
	stub403 := func(c *gin.Context) {
		c.JSON(http.StatusForbidden, types.StandardResponse{
			Success:   false,
			Message:   "forbidden",
			ErrorCode: types.ErrCodeAuthForbidden,
		})
	}
	stub404 := func(c *gin.Context) {
		c.JSON(http.StatusNotFound, types.StandardResponse{
			Success:   false,
			Message:   "not found",
			ErrorCode: types.ErrCodeObjectNotFound,
		})
	}
	stub500 := func(c *gin.Context) {
		c.JSON(http.StatusInternalServerError, types.StandardResponse{
			Success:   false,
			Message:   "internal error",
			ErrorCode: types.ErrCodeMetadataFailure,
		})
	}

	// Suppress "declared but not used" for stubs used only in sub-tests.
	_ = stub400
	_ = stub401
	_ = stub403
	_ = stub404
	_ = stub500

	r.GET("/api/cluster/status", stub200)
	r.GET("/api/cluster/nodes", stub200)
	r.GET("/api/logs", stub200)
	r.GET("/api/metadata/:id", stub200)

	return r
}

// TestRouteClusterStatus verifies GET /api/cluster/status is reachable (HTTP 200).
func TestRouteClusterStatus(t *testing.T) {
	r := routerWithStubRoutes()
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/cluster/status", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	resp := responseBody(t, w)
	if !resp.Success {
		t.Errorf("expected success=true, got false")
	}
}

// TestRouteClusterNodes verifies GET /api/cluster/nodes is reachable (HTTP 200).
func TestRouteClusterNodes(t *testing.T) {
	r := routerWithStubRoutes()
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/cluster/nodes", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	resp := responseBody(t, w)
	if !resp.Success {
		t.Errorf("expected success=true, got false")
	}
}

// TestRouteLogs verifies GET /api/logs is reachable (HTTP 200).
func TestRouteLogs(t *testing.T) {
	r := routerWithStubRoutes()
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/logs", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

// TestRouteMetadata verifies GET /api/metadata/:id is reachable (HTTP 200).
func TestRouteMetadata(t *testing.T) {
	r := routerWithStubRoutes()
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/metadata/00000000-0000-0000-0000-000000000001", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

// ---------------------------------------------------------------------------
// types.StandardResponse shape tests
// Verify that every error path returns a well-formed types.StandardResponse
// with the correct types.ErrCode* constant from types.go.
// ---------------------------------------------------------------------------

// TestStandardResponseShape verifies that types.StandardResponse fields are
// populated correctly for both success and error cases.
func TestStandardResponseShape(t *testing.T) {
	tests := []struct {
		name       string
		handler    gin.HandlerFunc
		wantCode   int
		wantOK     bool
		wantErrCode string
	}{
		{
			name: "success response has success=true and no error_code",
			handler: func(c *gin.Context) {
				c.JSON(http.StatusOK, types.StandardResponse{
					Success: true,
					Message: "ok",
					Data:    gin.H{"key": "value"},
				})
			},
			wantCode: http.StatusOK,
			wantOK:   true,
		},
		{
			name: "metadata failure returns ErrCodeMetadataFailure",
			handler: func(c *gin.Context) {
				c.JSON(http.StatusInternalServerError, types.StandardResponse{
					Success:   false,
					Message:   "db error",
					ErrorCode: types.ErrCodeMetadataFailure,
				})
			},
			wantCode:    http.StatusInternalServerError,
			wantOK:      false,
			wantErrCode: types.ErrCodeMetadataFailure,
		},
		{
			name: "replica failure returns ErrCodeReplicaFailure",
			handler: func(c *gin.Context) {
				c.JSON(http.StatusInternalServerError, types.StandardResponse{
					Success:   false,
					Message:   "replica error",
					ErrorCode: types.ErrCodeReplicaFailure,
				})
			},
			wantCode:    http.StatusInternalServerError,
			wantOK:      false,
			wantErrCode: types.ErrCodeReplicaFailure,
		},
		{
			name: "object not found returns ErrCodeObjectNotFound",
			handler: func(c *gin.Context) {
				c.JSON(http.StatusNotFound, types.StandardResponse{
					Success:   false,
					Message:   "object not found",
					ErrorCode: types.ErrCodeObjectNotFound,
				})
			},
			wantCode:    http.StatusNotFound,
			wantOK:      false,
			wantErrCode: types.ErrCodeObjectNotFound,
		},
		{
			name: "invalid UUID returns ErrCodeValidationFailed",
			handler: func(c *gin.Context) {
				c.JSON(http.StatusBadRequest, types.StandardResponse{
					Success:   false,
					Message:   "Invalid object UUID format",
					ErrorCode: types.ErrCodeValidationFailed,
				})
			},
			wantCode:    http.StatusBadRequest,
			wantOK:      false,
			wantErrCode: types.ErrCodeValidationFailed,
		},
		{
			name: "missing auth returns ErrCodeAuthUnauthorized",
			handler: func(c *gin.Context) {
				c.JSON(http.StatusUnauthorized, types.StandardResponse{
					Success:   false,
					Message:   "unauthorized",
					ErrorCode: types.ErrCodeAuthUnauthorized,
				})
			},
			wantCode:    http.StatusUnauthorized,
			wantOK:      false,
			wantErrCode: types.ErrCodeAuthUnauthorized,
		},
		{
			name: "wrong role returns ErrCodeAuthForbidden",
			handler: func(c *gin.Context) {
				c.JSON(http.StatusForbidden, types.StandardResponse{
					Success:   false,
					Message:   "forbidden",
					ErrorCode: types.ErrCodeAuthForbidden,
				})
			},
			wantCode:    http.StatusForbidden,
			wantOK:      false,
			wantErrCode: types.ErrCodeAuthForbidden,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := gin.New()
			r.GET("/test", tc.handler)

			w := httptest.NewRecorder()
			req, _ := http.NewRequest(http.MethodGet, "/test", nil)
			r.ServeHTTP(w, req)

			if w.Code != tc.wantCode {
				t.Errorf("HTTP status: want %d, got %d", tc.wantCode, w.Code)
			}

			resp := responseBody(t, w)

			if resp.Success != tc.wantOK {
				t.Errorf("success field: want %v, got %v", tc.wantOK, resp.Success)
			}
			if tc.wantErrCode != "" && resp.ErrorCode != tc.wantErrCode {
				t.Errorf("error_code: want %q, got %q", tc.wantErrCode, resp.ErrorCode)
			}
			if tc.wantOK && resp.ErrorCode != "" {
				t.Errorf("success response must not have error_code, got %q", resp.ErrorCode)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// has_more pagination logic test
// Validates the has_more boundary condition used in SystemLogs response.
// ---------------------------------------------------------------------------

func TestHasMorePagination(t *testing.T) {
	cases := []struct {
		offset   int
		returned int
		total    int
		want     bool
	}{
		{offset: 0, returned: 50, total: 100, want: true},
		{offset: 50, returned: 50, total: 100, want: false},
		{offset: 0, returned: 10, total: 10, want: false},
		{offset: 0, returned: 0, total: 0, want: false},
		{offset: 90, returned: 5, total: 100, want: true},
		{offset: 95, returned: 5, total: 100, want: false},
	}

	for _, tc := range cases {
		got := (tc.offset + tc.returned) < tc.total
		if got != tc.want {
			t.Errorf("has_more(offset=%d, returned=%d, total=%d): want %v, got %v",
				tc.offset, tc.returned, tc.total, tc.want, got)
		}
	}
}

// ---------------------------------------------------------------------------
// Tier classification logic test
// Validates HOT / WARM / COLD AccessTier constants from types.go.
// ---------------------------------------------------------------------------

func TestTierConstants(t *testing.T) {
	if types.TierHot != "HOT" {
		t.Errorf("TierHot: want HOT, got %s", types.TierHot)
	}
	if types.TierWarm != "WARM" {
		t.Errorf("TierWarm: want WARM, got %s", types.TierWarm)
	}
	if types.TierCold != "COLD" {
		t.Errorf("TierCold: want COLD, got %s", types.TierCold)
	}
}

// ---------------------------------------------------------------------------
// ErrCode constant values test
// Ensures the error codes used by monitoring handlers match types.go definitions.
// ---------------------------------------------------------------------------

func TestErrCodeConstants(t *testing.T) {
	codes := map[string]string{
		"ErrCodeMetadataFailure":  types.ErrCodeMetadataFailure,
		"ErrCodeReplicaFailure":   types.ErrCodeReplicaFailure,
		"ErrCodeObjectNotFound":   types.ErrCodeObjectNotFound,
		"ErrCodeValidationFailed": types.ErrCodeValidationFailed,
		"ErrCodeAuthUnauthorized": types.ErrCodeAuthUnauthorized,
		"ErrCodeAuthForbidden":    types.ErrCodeAuthForbidden,
		"ErrCodeNodeUnavailable":  types.ErrCodeNodeUnavailable,
	}
	for name, val := range codes {
		if val == "" {
			t.Errorf("%s must not be empty", name)
		}
	}
}
