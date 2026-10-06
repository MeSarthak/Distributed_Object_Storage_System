package coordinator

import (
	"fmt"
	"net/http"
	"time"

	"distributed-storage/pkg/database"
	"distributed-storage/pkg/types"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// MetadataHandler provides the deep object metadata inspection endpoint.
// Assigned to: Developer 3
type MetadataHandler struct {
	objectRepo    *database.ObjectRepository
	nodeRepo      *database.NodeRepository
	replicaRepo   *database.ReplicaRepository
	accessLogRepo *database.AccessLogRepository
}

// NewMetadataHandler constructs a MetadataHandler.
func NewMetadataHandler(
	objectRepo *database.ObjectRepository,
	nodeRepo *database.NodeRepository,
	replicaRepo *database.ReplicaRepository,
	accessLogRepo *database.AccessLogRepository,
) *MetadataHandler {
	return &MetadataHandler{
		objectRepo:    objectRepo,
		nodeRepo:      nodeRepo,
		replicaRepo:   replicaRepo,
		accessLogRepo: accessLogRepo,
	}
}

// GetObjectMetadata handles GET /api/metadata/:id.
// Returns types.ObjectMetadataWithReplicas containing:
//   - Core object stats (size, checksum, mime type, replication factor, timestamps)
//   - Physical storage node details for each replica (hostname, IP, internal URL, node status)
//   - Current access tier: HOT / WARM / COLD derived from 24-hour access count
//
// All responses are wrapped in types.StandardResponse.
// Error codes are sourced from types.ErrCode* constants defined in types.go.
func (h *MetadataHandler) GetObjectMetadata(c *gin.Context) {
	// 1. Parse and validate the object UUID path param.
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

	// 2. Fetch core object metadata.
	obj, err := h.objectRepo.GetObjectByID(objectID)
	if err != nil {
		c.JSON(http.StatusNotFound, types.StandardResponse{
			Success:   false,
			Message:   "Object not found",
			ErrorCode: types.ErrCodeObjectNotFound,
		})
		return
	}

	// 3. Fetch all replicas for the object.
	replicas, err := h.replicaRepo.GetReplicasByObject(objectID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, types.StandardResponse{
			Success:   false,
			Message:   "Failed to fetch replicas: " + err.Error(),
			ErrorCode: types.ErrCodeReplicaFailure,
		})
		return
	}

	// 4. Resolve physical node details for each replica.
	locations := make([]types.ReplicaLocation, 0, len(replicas))
	for _, rep := range replicas {
		loc := types.ReplicaLocation{
			ReplicaID: rep.ReplicaID,
			NodeID:    rep.NodeID,
			Status:    rep.Status,
		}
		// Enrich with live node info; tolerate individual node lookup failures.
		if node, err := h.nodeRepo.GetNodeByID(rep.NodeID); err == nil {
			loc.Hostname = node.Hostname
			loc.IPAddress = node.IPAddress
			loc.NodeStatus = node.Status
			loc.InternalURL = fmt.Sprintf("%s/internal/storage/%s", BuildNodeURL(*node), objectID)
		}
		locations = append(locations, loc)
	}

	// 5. Determine HOT / WARM / COLD tier from 24-hour access count.
	// Tier constants are sourced from types.AccessTier (types.go).
	tier := string(types.TierWarm) // default
	if h.accessLogRepo != nil {
		since := time.Now().Add(-24 * time.Hour)
		count, err := h.accessLogRepo.GetAccessCountSince(objectID, since)
		if err == nil {
			if count >= 10 {
				tier = string(types.TierHot)
			} else if count == 0 && time.Since(obj.LastAccessed) > 7*24*time.Hour {
				tier = string(types.TierCold)
			}
		}
	}

	// 6. Build types.ObjectMetadataWithReplicas and return.
	meta := types.ObjectMetadataWithReplicas{
		Object:   *obj,
		Replicas: locations,
		Tier:     tier,
	}

	c.JSON(http.StatusOK, types.StandardResponse{
		Success: true,
		Message: "Object metadata retrieved successfully",
		Data:    meta,
	})
}
