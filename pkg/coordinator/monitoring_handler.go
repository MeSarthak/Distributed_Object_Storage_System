package coordinator

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"distributed-storage/pkg/database"
	"distributed-storage/pkg/types"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// MonitoringHandler provides cluster telemetry, node metrics, audit logs,
// and deep object metadata inspection endpoints.
// Assigned to: Developer 3
type MonitoringHandler struct {
	db            *database.DB
	nodeRepo      *database.NodeRepository
	objectRepo    *database.ObjectRepository
	replicaRepo   *database.ReplicaRepository
	logRepo       *database.LogRepository
	accessLogRepo *database.AccessLogRepository
}

// NewMonitoringHandler constructs a MonitoringHandler.
func NewMonitoringHandler(
	db *database.DB,
	nodeRepo *database.NodeRepository,
	objectRepo *database.ObjectRepository,
	replicaRepo *database.ReplicaRepository,
	logRepo *database.LogRepository,
	accessLogRepo *database.AccessLogRepository,
) *MonitoringHandler {
	return &MonitoringHandler{
		db:            db,
		nodeRepo:      nodeRepo,
		objectRepo:    objectRepo,
		replicaRepo:   replicaRepo,
		logRepo:       logRepo,
		accessLogRepo: accessLogRepo,
	}
}

// ClusterStatus handles GET /api/cluster/status.
// Returns aggregate cluster capacity, node counts, average CPU/memory/latency,
// storage utilisation percentage, total object count, and an overall health label.
func (h *MonitoringHandler) ClusterStatus(c *gin.Context) {
	nodes, err := h.nodeRepo.GetAllNodes()
	if err != nil {
		c.JSON(http.StatusInternalServerError, types.StandardResponse{
			Success:   false,
			Message:   "Failed to fetch nodes for cluster status: " + err.Error(),
			ErrorCode: types.ErrCodeMetadataFailure,
		})
		return
	}

	var totalStorage, usedStorage int64
	var onlineNodes, offlineNodes, degradedNodes int
	var totalCPU, totalMemory, totalLatency float64

	for _, node := range nodes {
		totalStorage += node.TotalStorage
		usedStorage += node.UsedStorage
		totalCPU += node.CPUUsage
		totalMemory += node.MemoryUsage
		totalLatency += node.Latency

		switch node.Status {
		case types.NodeStatusOnline:
			onlineNodes++
		case types.NodeStatusDegraded:
			degradedNodes++
		case types.NodeStatusOffline:
			offlineNodes++
		}
	}

	nodeCount := len(nodes)
	var avgCPU, avgMemory, avgLatency float64
	if nodeCount > 0 {
		avgCPU = totalCPU / float64(nodeCount)
		avgMemory = totalMemory / float64(nodeCount)
		avgLatency = totalLatency / float64(nodeCount)
	}

	var storageUsedPct float64
	if totalStorage > 0 {
		storageUsedPct = float64(usedStorage) / float64(totalStorage) * 100.0
	}

	var totalObjects int64
	_ = h.db.QueryRow("SELECT COUNT(*) FROM objects").Scan(&totalObjects)

	var hot, warm, cold int
	_ = h.db.QueryRow("SELECT COUNT(*) FROM objects WHERE replication_factor >= 5").Scan(&hot)
	_ = h.db.QueryRow("SELECT COUNT(*) FROM objects WHERE replication_factor >= 3 AND replication_factor <= 4").Scan(&warm)
	_ = h.db.QueryRow("SELECT COUNT(*) FROM objects WHERE replication_factor <= 2").Scan(&cold)

	clusterHealth := "HEALTHY"
	if degradedNodes > 0 || offlineNodes > 0 {
		clusterHealth = "DEGRADED"
	}
	if onlineNodes == 0 && nodeCount > 0 {
		clusterHealth = "CRITICAL"
	}

	c.JSON(http.StatusOK, types.StandardResponse{
		Success: true,
		Message: "Cluster status retrieved successfully",
		Data: gin.H{
			"status":               clusterHealth,
			"total_nodes":          nodeCount,
			"online_nodes":         onlineNodes,
			"offline_nodes":        offlineNodes,
			"degraded_nodes":       degradedNodes,
			"total_storage_bytes":  totalStorage,
			"used_storage_bytes":   usedStorage,
			"storage_used_percent": fmt.Sprintf("%.2f", storageUsedPct),
			"avg_cpu_percent":      fmt.Sprintf("%.2f", avgCPU),
			"avg_memory_percent":   fmt.Sprintf("%.2f", avgMemory),
			"avg_latency_ms":       fmt.Sprintf("%.2f", avgLatency),
			"total_objects":        totalObjects,
			"tier_hot":             hot,
			"tier_warm":            warm,
			"tier_cold":            cold,
			"timestamp":            time.Now().UTC(),
		},
	})
}

// ClusterNodes handles GET /api/cluster/nodes.
// Returns detailed node metrics for every registered storage node.
func (h *MonitoringHandler) ClusterNodes(c *gin.Context) {
	nodes, err := h.nodeRepo.GetAllNodes()
	if err != nil {
		c.JSON(http.StatusInternalServerError, types.StandardResponse{
			Success:   false,
			Message:   "Failed to retrieve node list: " + err.Error(),
			ErrorCode: types.ErrCodeMetadataFailure,
		})
		return
	}

	if nodes == nil {
		nodes = []types.StorageNode{}
	}
	c.JSON(http.StatusOK, types.StandardResponse{
		Success: true,
		Message: "Storage nodes retrieved successfully",
		Data: gin.H{
			"nodes": nodes,
			"count": len(nodes),
		},
	})
}

// SystemLogs handles GET /api/logs.
// Returns paginated audit logs filtered by severity and event_type.
// Query params: severity, event_type, limit (default 50), offset (default 0).
func (h *MonitoringHandler) SystemLogs(c *gin.Context) {
	severity := types.LogSeverity(c.Query("severity"))
	eventType := c.Query("event_type")

	limit := 50
	offset := 0
	if l := c.Query("limit"); l != "" {
		if val, err := strconv.Atoi(l); err == nil && val > 0 {
			if val > 200 {
				val = 200
			}
			limit = val
		}
	}
	if o := c.Query("offset"); o != "" {
		if val, err := strconv.Atoi(o); err == nil && val >= 0 {
			offset = val
		}
	}

	logs, err := h.logRepo.GetSystemLogs(severity, eventType, limit, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, types.StandardResponse{
			Success:   false,
			Message:   "Failed to fetch system logs: " + err.Error(),
			ErrorCode: types.ErrCodeMetadataFailure,
		})
		return
	}

	// Fetch total count for client-side pagination — non-fatal if it fails.
	total, _ := h.logRepo.CountLogs(severity, eventType)

	if logs == nil {
		logs = []types.SystemLog{}
	}

	c.JSON(http.StatusOK, types.StandardResponse{
		Success: true,
		Message: "System audit logs retrieved successfully",
		Data: gin.H{
			"logs":     logs,
			"count":    len(logs),
			"total":    total,
			"limit":    limit,
			"offset":   offset,
			"has_more": (offset + len(logs)) < total,
		},
	})
}

// Metadata handles GET /api/metadata/:id.
// Returns deep object metadata including physical replica locations and current access tier.
func (h *MonitoringHandler) Metadata(c *gin.Context) {
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

	obj, err := h.objectRepo.GetObjectByID(objectID)
	if err != nil {
		c.JSON(http.StatusNotFound, types.StandardResponse{
			Success:   false,
			Message:   "Object not found",
			ErrorCode: types.ErrCodeObjectNotFound,
		})
		return
	}

	// Fetch replicas
	replicas, err := h.replicaRepo.GetReplicasByObject(objectID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, types.StandardResponse{
			Success:   false,
			Message:   "Failed to fetch replicas: " + err.Error(),
			ErrorCode: types.ErrCodeMetadataFailure,
		})
		return
	}

	locations := make([]types.ReplicaLocation, 0, len(replicas))
	for _, rep := range replicas {
		loc := types.ReplicaLocation{
			ReplicaID: rep.ReplicaID,
			NodeID:    rep.NodeID,
			Status:    rep.Status,
		}

		if node, err := h.nodeRepo.GetNodeByID(rep.NodeID); err == nil {
			loc.Hostname = node.Hostname
			loc.IPAddress = node.IPAddress
			loc.NodeStatus = node.Status
			loc.InternalURL = fmt.Sprintf("%s/internal/storage/%s", BuildNodeURL(*node), objectID)
		}
		locations = append(locations, loc)
	}

	// Calculate recent access frequency (last 24 hours) for tier classification
	tier := string(types.TierWarm)
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
