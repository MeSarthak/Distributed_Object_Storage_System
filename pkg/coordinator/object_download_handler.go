package coordinator

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"distributed-storage/pkg/auth"
	"distributed-storage/pkg/database"
	"distributed-storage/pkg/types"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// ObjectRepositoryReader defines object data queries required by the download handler.
type ObjectRepositoryReader interface {
	GetObjectByID(objectID uuid.UUID) (*types.Object, error)
	GetObjectsByOwner(ownerID uuid.UUID, limit, offset int) ([]types.Object, error)
	SearchObjects(ownerID uuid.UUID, queryStr string, limit, offset int) ([]types.Object, error)
	UpdateLastAccessed(objectID uuid.UUID, t time.Time) error
}

// ReplicaRepositoryReader defines replica queries required by the download handler.
type ReplicaRepositoryReader interface {
	GetHealthyReplicasByObject(objectID uuid.UUID) ([]types.Replica, error)
	GetReplicasByObject(objectID uuid.UUID) ([]types.Replica, error)
}

// NodeRepositoryReader defines node lookup queries.
type NodeRepositoryReader interface {
	GetNodeByID(nodeID uuid.UUID) (*types.StorageNode, error)
}

// AccessLogRepositoryRecorder defines telemetry access logging.
type AccessLogRepositoryRecorder interface {
	RecordAccess(objectID *uuid.UUID, userID *uuid.UUID, responseTimeMs int) error
}

// LogRepositoryWriter defines system audit log operations.
type LogRepositoryWriter interface {
	InsertSystemLog(eventType, description string, severity types.LogSeverity, metadata map[string]string) error
}

// ObjectDownloadHandler manages the read, retrieval, download failover,
// listing, searching, and access tracking of objects.
// Assigned to: Person 2
type ObjectDownloadHandler struct {
	db            *database.DB
	objectRepo    ObjectRepositoryReader
	replicaRepo   ReplicaRepositoryReader
	nodeRepo      NodeRepositoryReader
	accessLogRepo AccessLogRepositoryRecorder
	logRepo       LogRepositoryWriter
	storageClient StorageClientInterface
}

// NewObjectDownloadHandler constructs an ObjectDownloadHandler.
func NewObjectDownloadHandler(
	db *database.DB,
	objectRepo *database.ObjectRepository,
	replicaRepo *database.ReplicaRepository,
	nodeRepo *database.NodeRepository,
	accessLogRepo *database.AccessLogRepository,
	logRepo *database.LogRepository,
) *ObjectDownloadHandler {
	return &ObjectDownloadHandler{
		db:            db,
		objectRepo:    objectRepo,
		replicaRepo:   replicaRepo,
		nodeRepo:      nodeRepo,
		accessLogRepo: accessLogRepo,
		logRepo:       logRepo,
		storageClient: NewStorageClient(),
	}
}

// SetStorageClient replaces the internal storage client (useful for unit testing and mocks).
func (h *ObjectDownloadHandler) SetStorageClient(sc StorageClientInterface) {
	h.storageClient = sc
}

// SetObjectRepo overrides object repository implementation (useful for unit tests).
func (h *ObjectDownloadHandler) SetObjectRepo(r ObjectRepositoryReader) {
	h.objectRepo = r
}

// SetReplicaRepo overrides replica repository implementation (useful for unit tests).
func (h *ObjectDownloadHandler) SetReplicaRepo(r ReplicaRepositoryReader) {
	h.replicaRepo = r
}

// SetNodeRepo overrides node repository implementation (useful for unit tests).
func (h *ObjectDownloadHandler) SetNodeRepo(r NodeRepositoryReader) {
	h.nodeRepo = r
}

// SetAccessLogRepo overrides access log repository implementation (useful for unit tests).
func (h *ObjectDownloadHandler) SetAccessLogRepo(r AccessLogRepositoryRecorder) {
	h.accessLogRepo = r
}

// SetLogRepo overrides system log repository implementation (useful for unit tests).
func (h *ObjectDownloadHandler) SetLogRepo(r LogRepositoryWriter) {
	h.logRepo = r
}

// Download handles GET /api/objects/:id.
// Flow:
// 1. Authenticate user from gin context and verify ownership.
// 2. Fetch healthy replicas for the object.
// 3. Failover loop: stream chunk from primary replica; if unreachable or 5xx, fall back to next replica.
// 4. Verify SHA-256 checksum against authoritative object metadata.
// 5. Asynchronously record access telemetry in access_logs and update last_accessed.
// 6. Stream file bytes with proper Content-Disposition and Content-Type headers.
func (h *ObjectDownloadHandler) Download(c *gin.Context) {
	startTime := time.Now()

	// 1. Verify authentication
	userIDVal, exists := c.Get(auth.ContextUserID)
	if !exists {
		c.JSON(http.StatusUnauthorized, types.StandardResponse{
			Success:   false,
			Message:   "Authentication required",
			ErrorCode: types.ErrCodeAuthUnauthorized,
		})
		return
	}
	userID := userIDVal.(uuid.UUID)

	userRoleVal, _ := c.Get(auth.ContextUserRole)
	userRole, _ := userRoleVal.(types.UserRole)

	// Validate object UUID
	idStr := c.Param("id")
	objectID, err := uuid.Parse(idStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, types.StandardResponse{
			Success:   false,
			Message:   "Invalid object UUID format: " + err.Error(),
			ErrorCode: types.ErrCodeValidationFailed,
		})
		return
	}

	// Fetch object metadata
	if h.objectRepo == nil {
		c.JSON(http.StatusInternalServerError, types.StandardResponse{
			Success:   false,
			Message:   "Object repository is not configured",
			ErrorCode: types.ErrCodeMetadataFailure,
		})
		return
	}

	obj, err := h.objectRepo.GetObjectByID(objectID)
	if err != nil {
		c.JSON(http.StatusNotFound, types.StandardResponse{
			Success:   false,
			Message:   "Object not found: " + objectID.String(),
			ErrorCode: types.ErrCodeObjectNotFound,
		})
		return
	}

	// Verify ownership (Admin role can access any file)
	if obj.OwnerID != userID && userRole != types.RoleAdmin {
		c.JSON(http.StatusForbidden, types.StandardResponse{
			Success:   false,
			Message:   "You do not have permission to access this object",
			ErrorCode: types.ErrCodeAuthForbidden,
		})
		return
	}

	// 2. Fetch healthy replicas
	if h.replicaRepo == nil {
		c.JSON(http.StatusInternalServerError, types.StandardResponse{
			Success:   false,
			Message:   "Replica repository is not configured",
			ErrorCode: types.ErrCodeMetadataFailure,
		})
		return
	}

	replicas, err := h.replicaRepo.GetHealthyReplicasByObject(objectID)
	if err != nil || len(replicas) == 0 {
		c.JSON(http.StatusServiceUnavailable, types.StandardResponse{
			Success:   false,
			Message:   "No healthy replicas currently available for this object",
			ErrorCode: types.ErrCodeNodeUnavailable,
		})
		return
	}

	// 3. Automatic failover loop across healthy replicas
	var bodyStream io.ReadCloser
	var activeNode types.StorageNode

	for _, rep := range replicas {
		if h.nodeRepo == nil {
			continue
		}
		node, err := h.nodeRepo.GetNodeByID(rep.NodeID)
		if err != nil || node.Status != types.NodeStatusOnline {
			continue
		}

		if h.storageClient == nil {
			continue
		}

		stream, _, err := h.storageClient.FetchChunk(c.Request.Context(), *node, objectID)
		if err == nil {
			bodyStream = stream
			activeNode = *node
			break
		}

		log.Printf("[DOWNLOAD FAILOVER] Replica on node %s (%s) unreachable: %v, falling back to next replica...",
			node.Hostname, node.NodeID, err)
	}

	if bodyStream == nil {
		c.JSON(http.StatusServiceUnavailable, types.StandardResponse{
			Success:   false,
			Message:   "Failed to retrieve object from all healthy replica nodes",
			ErrorCode: types.ErrCodeNodeUnavailable,
		})
		return
	}
	defer bodyStream.Close()

	// 4. Read bytes and verify SHA-256 checksum
	data, err := io.ReadAll(bodyStream)
	if err != nil {
		c.JSON(http.StatusInternalServerError, types.StandardResponse{
			Success:   false,
			Message:   "Failed reading stream from storage node: " + err.Error(),
			ErrorCode: types.ErrCodeReplicaFailure,
		})
		return
	}

	hasher := sha256.New()
	hasher.Write(data)
	computedHash := hex.EncodeToString(hasher.Sum(nil))

	if !strings.EqualFold(computedHash, obj.Checksum) {
		log.Printf("[INTEGRITY ERROR] Checksum mismatch for object %s: expected %s, got %s",
			objectID, obj.Checksum, computedHash)

		if h.logRepo != nil {
			_ = h.logRepo.InsertSystemLog(
				types.EventChecksumMismatch,
				fmt.Sprintf("Integrity failure downloading object %s from node %s", objectID, activeNode.Hostname),
				types.SeverityError,
				map[string]string{
					"object_id": objectID.String(),
					"node_id":   activeNode.NodeID.String(),
					"expected":  obj.Checksum,
					"actual":    computedHash,
				},
			)
		}

		c.JSON(http.StatusConflict, types.StandardResponse{
			Success:   false,
			Message:   "Data corruption detected: SHA-256 checksum mismatch",
			ErrorCode: types.ErrCodeReplicaFailure,
		})
		return
	}

	// 5. Asynchronously record access telemetry
	durationMs := int(time.Since(startTime).Milliseconds())
	go func() {
		if h.accessLogRepo != nil {
			_ = h.accessLogRepo.RecordAccess(&objectID, &userID, durationMs)
		}
		if h.objectRepo != nil {
			_ = h.objectRepo.UpdateLastAccessed(objectID, time.Now().UTC())
		}
	}()

	// 6. Serve download to client.
	// Content-Length is set from len(data): we always buffer the full body
	// via io.ReadAll, so this is the only correct length after decompression.
	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", obj.ObjectName))
	c.Header("Content-Type", obj.MimeType)
	c.Header("Content-Length", strconv.FormatInt(int64(len(data)), 10))
	c.Header("X-Checksum-SHA256", obj.Checksum)
	c.Data(http.StatusOK, obj.MimeType, data)
}

// List handles GET /api/objects.
// Returns paginated list of objects owned by the authenticated user.
func (h *ObjectDownloadHandler) List(c *gin.Context) {
	userIDVal, exists := c.Get(auth.ContextUserID)
	if !exists {
		c.JSON(http.StatusUnauthorized, types.StandardResponse{
			Success:   false,
			Message:   "Authentication required",
			ErrorCode: types.ErrCodeAuthUnauthorized,
		})
		return
	}
	userID := userIDVal.(uuid.UUID)

	limit := 50
	offset := 0
	if l := c.Query("limit"); l != "" {
		if val, err := strconv.Atoi(l); err == nil && val > 0 {
			limit = val
		}
	}
	if o := c.Query("offset"); o != "" {
		if val, err := strconv.Atoi(o); err == nil && val >= 0 {
			offset = val
		}
	}

	if h.objectRepo == nil {
		c.JSON(http.StatusInternalServerError, types.StandardResponse{
			Success:   false,
			Message:   "Object repository is not configured",
			ErrorCode: types.ErrCodeMetadataFailure,
		})
		return
	}

	objects, err := h.objectRepo.GetObjectsByOwner(userID, limit, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, types.StandardResponse{
			Success:   false,
			Message:   "Failed to fetch objects: " + err.Error(),
			ErrorCode: types.ErrCodeMetadataFailure,
		})
		return
	}

	if objects == nil {
		objects = []types.Object{}
	}

	c.JSON(http.StatusOK, types.StandardResponse{
		Success: true,
		Message: "Objects retrieved successfully",
		Data: gin.H{
			"objects": objects,
			"count":   len(objects),
			"limit":   limit,
			"offset":  offset,
		},
	})
}

// Search handles GET /api/objects/search.
// Searches user's objects matching the query string parameter `q` (or `name` / `prefix`).
func (h *ObjectDownloadHandler) Search(c *gin.Context) {
	userIDVal, exists := c.Get(auth.ContextUserID)
	if !exists {
		c.JSON(http.StatusUnauthorized, types.StandardResponse{
			Success:   false,
			Message:   "Authentication required",
			ErrorCode: types.ErrCodeAuthUnauthorized,
		})
		return
	}
	userID := userIDVal.(uuid.UUID)

	query := c.Query("q")
	if query == "" {
		query = c.Query("query")
	}
	if query == "" {
		query = c.Query("name")
	}
	if query == "" {
		query = c.Query("prefix")
	}

	if strings.TrimSpace(query) == "" {
		c.JSON(http.StatusBadRequest, types.StandardResponse{
			Success:   false,
			Message:   "Query parameter 'q' is required",
			ErrorCode: types.ErrCodeValidationFailed,
		})
		return
	}

	limit := 50
	offset := 0
	if l := c.Query("limit"); l != "" {
		if val, err := strconv.Atoi(l); err == nil && val > 0 {
			limit = val
		}
	}
	if o := c.Query("offset"); o != "" {
		if val, err := strconv.Atoi(o); err == nil && val >= 0 {
			offset = val
		}
	}

	if h.objectRepo == nil {
		c.JSON(http.StatusInternalServerError, types.StandardResponse{
			Success:   false,
			Message:   "Object repository is not configured",
			ErrorCode: types.ErrCodeMetadataFailure,
		})
		return
	}

	objects, err := h.objectRepo.SearchObjects(userID, strings.TrimSpace(query), limit, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, types.StandardResponse{
			Success:   false,
			Message:   "Failed to search objects: " + err.Error(),
			ErrorCode: types.ErrCodeMetadataFailure,
		})
		return
	}

	if objects == nil {
		objects = []types.Object{}
	}

	c.JSON(http.StatusOK, types.StandardResponse{
		Success: true,
		Message: "Search results retrieved successfully",
		Data: gin.H{
			"objects": objects,
			"query":   query,
			"count":   len(objects),
			"limit":   limit,
			"offset":  offset,
		},
	})
}
