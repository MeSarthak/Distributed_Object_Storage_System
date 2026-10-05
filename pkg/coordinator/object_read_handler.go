package coordinator

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"distributed-storage/pkg/auth"
	"distributed-storage/pkg/database"
	"distributed-storage/pkg/types"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// ObjectReadHandler manages download, deletion, listing, and searching of objects.
// Assigned to: Developer 2
type ObjectReadHandler struct {
	db            *database.DB
	objectRepo    *database.ObjectRepository
	replicaRepo   *database.ReplicaRepository
	nodeRepo      *database.NodeRepository
	accessLogRepo *database.AccessLogRepository
	logRepo       *database.LogRepository
	storageClient *StorageClient
}

// NewObjectReadHandler constructs an ObjectReadHandler.
func NewObjectReadHandler(
	db *database.DB,
	objectRepo *database.ObjectRepository,
	replicaRepo *database.ReplicaRepository,
	nodeRepo *database.NodeRepository,
	accessLogRepo *database.AccessLogRepository,
	logRepo *database.LogRepository,
) *ObjectReadHandler {
	return &ObjectReadHandler{
		db:            db,
		objectRepo:    objectRepo,
		replicaRepo:   replicaRepo,
		nodeRepo:      nodeRepo,
		accessLogRepo: accessLogRepo,
		logRepo:       logRepo,
		storageClient: NewStorageClient(),
	}
}

// Download handles GET /api/objects/:id.
// Flow:
// 1. Authenticate user and verify object ownership.
// 2. Query healthy replicas and online storage nodes.
// 3. Failover: attempt download from the primary replica; fall back to secondary on failure.
// 4. Stream bytes to client and verify SHA-256 checksum.
// 5. Asynchronously log access in access_logs and update last_accessed.
func (h *ObjectReadHandler) Download(c *gin.Context) {
	startTime := time.Now()
	idStr := c.Param("id")
	objectID, err := uuid.Parse(idStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, types.StandardResponse{
			Success:   false,
			Message:   "Invalid object UUID format",
			ErrorCode: types.ErrCodeValidationFailed,
		})
		return
	}

	userIDVal, _ := c.Get(auth.ContextUserID)
	userID, _ := userIDVal.(uuid.UUID)
	userRoleVal, _ := c.Get(auth.ContextUserRole)
	userRole, _ := userRoleVal.(types.UserRole)

	// Fetch object metadata
	obj, err := h.objectRepo.GetObjectByID(objectID)
	if err != nil {
		c.JSON(http.StatusNotFound, types.StandardResponse{
			Success:   false,
			Message:   "Object not found",
			ErrorCode: types.ErrCodeObjectNotFound,
		})
		return
	}

	// Verify ownership (Admin can download any file)
	if obj.OwnerID != userID && userRole != types.RoleAdmin {
		c.JSON(http.StatusForbidden, types.StandardResponse{
			Success:   false,
			Message:   "You do not have permission to access this object",
			ErrorCode: types.ErrCodeAuthForbidden,
		})
		return
	}

	// Get healthy replicas
	replicas, err := h.replicaRepo.GetHealthyReplicasByObject(objectID)
	if err != nil || len(replicas) == 0 {
		c.JSON(http.StatusServiceUnavailable, types.StandardResponse{
			Success:   false,
			Message:   "No healthy replicas currently available for this object",
			ErrorCode: types.ErrCodeNodeUnavailable,
		})
		return
	}

	// Failover loop across available replica nodes
	var bodyStream io.ReadCloser
	var streamLen int64
	var activeNode types.StorageNode

	for _, rep := range replicas {
		node, err := h.nodeRepo.GetNodeByID(rep.NodeID)
		if err != nil || node.Status != types.NodeStatusOnline {
			continue
		}

		stream, length, err := h.storageClient.FetchChunk(c.Request.Context(), *node, objectID)
		if err == nil {
			bodyStream = stream
			streamLen = length
			activeNode = *node
			break
		}
		log.Printf("[DOWNLOAD] Replica on node %s (%s) unreachable: %v, trying next...", node.Hostname, node.NodeID, err)
	}

	if bodyStream == nil {
		c.JSON(http.StatusServiceUnavailable, types.StandardResponse{
			Success:   false,
			Message:   "Failed to retrieve object from all replica nodes",
			ErrorCode: types.ErrCodeNodeUnavailable,
		})
		return
	}
	defer bodyStream.Close()

	// Read and verify checksum
	data, err := io.ReadAll(bodyStream)
	if err != nil {
		c.JSON(http.StatusInternalServerError, types.StandardResponse{
			Success:   false,
			Message:   "Failed reading stream from storage node",
			ErrorCode: types.ErrCodeReplicaFailure,
		})
		return
	}

	hasher := sha256.New()
	hasher.Write(data)
	computedHash := hex.EncodeToString(hasher.Sum(nil))

	if computedHash != obj.Checksum {
		log.Printf("[INTEGRITY WARNING] Checksum mismatch for object %s: expected %s, got %s", objectID, obj.Checksum, computedHash)
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

	// Record access metrics asynchronously
	durationMs := int(time.Since(startTime).Milliseconds())
	go func() {
		if h.accessLogRepo != nil {
			_ = h.accessLogRepo.RecordAccess(&objectID, &userID, durationMs)
		}
		_ = h.objectRepo.UpdateLastAccessed(objectID, time.Now().UTC())
	}()

	// Serve file download
	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", obj.ObjectName))
	c.Header("Content-Type", obj.MimeType)
	c.Header("Content-Length", strconv.FormatInt(int64(len(data)), 10))
	c.Header("X-Checksum-SHA256", obj.Checksum)
	if streamLen > 0 {
		c.Header("Content-Length", strconv.FormatInt(streamLen, 10))
	}
	c.Data(http.StatusOK, obj.MimeType, data)
}

// Delete handles DELETE /api/objects/:id.
// Flow:
// 1. Authenticate user and verify ownership.
// 2. Fetch all replicas.
// 3. Dispatch delete request to all holding storage nodes.
// 4. Delete metadata record (Postgres cascade cleans up replicas).
// 5. Write audit log.
func (h *ObjectReadHandler) Delete(c *gin.Context) {
	idStr := c.Param("id")
	objectID, err := uuid.Parse(idStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, types.StandardResponse{
			Success:   false,
			Message:   "Invalid object UUID format",
			ErrorCode: types.ErrCodeValidationFailed,
		})
		return
	}

	userIDVal, _ := c.Get(auth.ContextUserID)
	userID, _ := userIDVal.(uuid.UUID)
	userRoleVal, _ := c.Get(auth.ContextUserRole)
	userRole, _ := userRoleVal.(types.UserRole)

	obj, err := h.objectRepo.GetObjectByID(objectID)
	if err != nil {
		c.JSON(http.StatusNotFound, types.StandardResponse{
			Success:   false,
			Message:   "Object not found",
			ErrorCode: types.ErrCodeObjectNotFound,
		})
		return
	}

	if obj.OwnerID != userID && userRole != types.RoleAdmin {
		c.JSON(http.StatusForbidden, types.StandardResponse{
			Success:   false,
			Message:   "You do not have permission to delete this object",
			ErrorCode: types.ErrCodeAuthForbidden,
		})
		return
	}

	// Purge physical chunks from storage nodes
	replicas, _ := h.replicaRepo.GetReplicasByObject(objectID)
	for _, rep := range replicas {
		node, err := h.nodeRepo.GetNodeByID(rep.NodeID)
		if err == nil {
			_ = h.storageClient.DeleteChunk(c.Request.Context(), *node, objectID)
		}
	}

	// Delete metadata in PostgreSQL
	if err := h.objectRepo.DeleteObject(objectID); err != nil {
		c.JSON(http.StatusInternalServerError, types.StandardResponse{
			Success:   false,
			Message:   "Failed to delete object metadata: " + err.Error(),
			ErrorCode: types.ErrCodeMetadataFailure,
		})
		return
	}

	// Audit log
	if h.logRepo != nil {
		_ = h.logRepo.InsertSystemLog(
			"OBJECT_DELETE",
			fmt.Sprintf("Object %s (%s) deleted by user %s", obj.ObjectName, objectID, userID),
			types.SeverityInfo,
			map[string]string{
				"object_id": objectID.String(),
				"user_id":   userID.String(),
			},
		)
	}

	c.JSON(http.StatusOK, types.StandardResponse{
		Success: true,
		Message: "Object and all replicas deleted successfully",
	})
}

// List handles GET /api/objects.
// Returns paginated list of objects owned by the authenticated user.
func (h *ObjectReadHandler) List(c *gin.Context) {
	userIDVal, _ := c.Get(auth.ContextUserID)
	userID, _ := userIDVal.(uuid.UUID)

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
// Searches user's objects matching the query string parameter `q`.
func (h *ObjectReadHandler) Search(c *gin.Context) {
	userIDVal, _ := c.Get(auth.ContextUserID)
	userID, _ := userIDVal.(uuid.UUID)

	query := c.Query("q")
	if query == "" {
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

	objects, err := h.objectRepo.SearchObjects(userID, query, limit, offset)
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
		},
	})
}
