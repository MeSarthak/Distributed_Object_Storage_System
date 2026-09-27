package coordinator

import (
	"fmt"
	"log"
	"net/http"

	"distributed-storage/pkg/auth"
	"distributed-storage/pkg/database"
	"distributed-storage/pkg/types"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// ObjectRepositoryDeleter defines object query & delete operations for the delete handler.
type ObjectRepositoryDeleter interface {
	GetObjectByID(objectID uuid.UUID) (*types.Object, error)
	DeleteObject(objectID uuid.UUID) error
}

// ObjectDeleteHandler manages the cascade deletion of objects across physical
// storage nodes and the PostgreSQL metadata database.
// Assigned to: Person 2
type ObjectDeleteHandler struct {
	db            *database.DB
	objectRepo    ObjectRepositoryDeleter
	replicaRepo   ReplicaRepositoryReader
	nodeRepo      NodeRepositoryReader
	logRepo       LogRepositoryWriter
	storageClient StorageClientInterface
}

// NewObjectDeleteHandler constructs an ObjectDeleteHandler.
func NewObjectDeleteHandler(
	db *database.DB,
	objectRepo *database.ObjectRepository,
	replicaRepo *database.ReplicaRepository,
	nodeRepo *database.NodeRepository,
	logRepo *database.LogRepository,
) *ObjectDeleteHandler {
	return &ObjectDeleteHandler{
		db:            db,
		objectRepo:    objectRepo,
		replicaRepo:   replicaRepo,
		nodeRepo:      nodeRepo,
		logRepo:       logRepo,
		storageClient: NewStorageClient(),
	}
}

// SetStorageClient replaces the internal storage client (useful for unit testing and mocks).
func (h *ObjectDeleteHandler) SetStorageClient(sc StorageClientInterface) {
	h.storageClient = sc
}

// SetObjectRepo overrides object repository implementation (useful for unit tests).
func (h *ObjectDeleteHandler) SetObjectRepo(r ObjectRepositoryDeleter) {
	h.objectRepo = r
}

// SetReplicaRepo overrides replica repository implementation (useful for unit tests).
func (h *ObjectDeleteHandler) SetReplicaRepo(r ReplicaRepositoryReader) {
	h.replicaRepo = r
}

// SetNodeRepo overrides node repository implementation (useful for unit tests).
func (h *ObjectDeleteHandler) SetNodeRepo(r NodeRepositoryReader) {
	h.nodeRepo = r
}

// SetLogRepo overrides system log repository implementation (useful for unit tests).
func (h *ObjectDeleteHandler) SetLogRepo(r LogRepositoryWriter) {
	h.logRepo = r
}

// Delete handles DELETE /api/objects/:id.
// Flow:
// 1. Authenticate user from gin context.
// 2. Validate object UUID format.
// 3. Fetch object metadata and verify ownership (or ADMIN role).
// 4. Query all holding replicas for the object.
// 5. Send DELETE /internal/storage/:id to every holding storage node to purge raw disk chunks.
// 6. Delete metadata record in PostgreSQL (cascades to replicas).
// 7. Write an audit log entry to system_logs.
func (h *ObjectDeleteHandler) Delete(c *gin.Context) {
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

	// 2. Validate object UUID
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

	// 3. Fetch object metadata
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

	// Verify ownership (Admin role can delete any object)
	if obj.OwnerID != userID && userRole != types.RoleAdmin {
		c.JSON(http.StatusForbidden, types.StandardResponse{
			Success:   false,
			Message:   "You do not have permission to delete this object",
			ErrorCode: types.ErrCodeAuthForbidden,
		})
		return
	}

	// 4. Query all replicas holding chunks
	var replicas []types.Replica
	if h.replicaRepo != nil {
		reps, err := h.replicaRepo.GetReplicasByObject(objectID)
		if err != nil {
			log.Printf("[DELETE WARNING] Failed to query replicas for object %s: %v", objectID, err)
		} else {
			replicas = reps
		}
	}

	// 5. Purge physical chunks from holding storage nodes
	if h.nodeRepo != nil && h.storageClient != nil {
		for _, rep := range replicas {
			node, err := h.nodeRepo.GetNodeByID(rep.NodeID)
			if err == nil {
				if delErr := h.storageClient.DeleteChunk(c.Request.Context(), *node, objectID); delErr != nil {
					log.Printf("[DELETE WARNING] Failed to delete chunk for object %s on node %s (%s): %v",
						objectID, node.Hostname, node.NodeID, delErr)
				}
			}
		}
	}

	// 6. Delete metadata record in PostgreSQL (cascades to replicas table)
	if err := h.objectRepo.DeleteObject(objectID); err != nil {
		c.JSON(http.StatusInternalServerError, types.StandardResponse{
			Success:   false,
			Message:   "Failed to delete object metadata: " + err.Error(),
			ErrorCode: types.ErrCodeMetadataFailure,
		})
		return
	}

	// 7. Write audit log
	if h.logRepo != nil {
		_ = h.logRepo.InsertSystemLog(
			"OBJECT_DELETE",
			fmt.Sprintf("Object %s (%s) deleted by user %s", obj.ObjectName, objectID, userID),
			types.SeverityInfo,
			map[string]string{
				"object_id":   objectID.String(),
				"object_name": obj.ObjectName,
				"user_id":     userID.String(),
			},
		)
	}

	c.JSON(http.StatusOK, types.StandardResponse{
		Success: true,
		Message: "Object and all replicas deleted successfully",
		Data: gin.H{
			"object_id": objectID,
		},
	})
}
