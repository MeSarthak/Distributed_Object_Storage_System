package coordinator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"sync"
	"time"

	"distributed-storage/pkg/config"
	"distributed-storage/pkg/database"
	"distributed-storage/pkg/types"

	"github.com/google/uuid"
)

// RebalanceAction describes what action was taken on an object replica set.
type RebalanceAction string

const (
	ActionScaleUp   RebalanceAction = "SCALE_UP"
	ActionScaleDown RebalanceAction = "SCALE_DOWN"
	ActionNone      RebalanceAction = "NONE"
)

// RebalanceResult summarizes the outcome of evaluating and adapting an object's replication.
type RebalanceResult struct {
	ObjectID         uuid.UUID       `json:"object_id"`
	ObjectName       string          `json:"object_name"`
	Tier             string          `json:"tier"` // HOT, WARM, COLD
	AccessCount24h   int             `json:"access_count_24h"`
	InitialReplicas  int             `json:"initial_replicas"`
	TargetReplicas   int             `json:"target_replicas"`
	Action           RebalanceAction `json:"action"`
	ReplicasAdded    int             `json:"replicas_added"`
	ReplicasRemoved  int             `json:"replicas_removed"`
	Error            string          `json:"error,omitempty"`
}

// ReplicationManager handles adaptive replication based on access frequency (HOT/WARM/COLD)
// and ensures objects maintain replica counts within [MinReplicationFactor, MaxReplicationFactor].
type ReplicationManager struct {
	db          *database.DB
	objectRepo  *database.ObjectRepository
	replicaRepo *database.ReplicaRepository
	nodeRepo    *database.NodeRepository
	logRepo     *database.LogRepository
	placement   *PlacementEngine
	cfg         config.ReplicationConfig
	interval    time.Duration

	inProgress map[uuid.UUID]struct{}
	mu         sync.Mutex
	httpClient *http.Client
}

// NewReplicationManager constructs a ReplicationManager.
func NewReplicationManager(
	db *database.DB,
	objectRepo *database.ObjectRepository,
	replicaRepo *database.ReplicaRepository,
	nodeRepo *database.NodeRepository,
	logRepo *database.LogRepository,
	placement *PlacementEngine,
	cfg config.ReplicationConfig,
	interval time.Duration,
) *ReplicationManager {
	if interval <= 0 {
		interval = 60 * time.Second
	}
	return &ReplicationManager{
		db:          db,
		objectRepo:  objectRepo,
		replicaRepo: replicaRepo,
		nodeRepo:    nodeRepo,
		logRepo:     logRepo,
		placement:   placement,
		cfg:         cfg,
		interval:    interval,
		inProgress:  make(map[uuid.UUID]struct{}),
		httpClient:  &http.Client{Timeout: 5 * time.Minute},
	}
}

// ClassifyTier maps a 24h access count to an adaptive tier and target replication factor.
func (rm *ReplicationManager) ClassifyTier(accessCount24h int) (tier string, targetFactor int) {
	if accessCount24h >= rm.cfg.HotAccessThreshold {
		return "HOT", rm.cfg.MaxReplicationFactor
	}
	if accessCount24h <= rm.cfg.ColdAccessThreshold {
		return "COLD", rm.cfg.MinReplicationFactor
	}
	return "WARM", rm.cfg.DefaultReplicationFactor
}

// GetObjectClassification returns the current tier, access frequency, and replica recommendations for an object.
func (rm *ReplicationManager) GetObjectClassification(objectID uuid.UUID) (string, int, int, int, error) {
	obj, err := rm.objectRepo.GetObjectByID(objectID)
	if err != nil {
		return "", 0, 0, 0, err
	}

	count, err := rm.objectRepo.GetAccessCount24h(objectID)
	if err != nil {
		return "", 0, 0, 0, err
	}

	tier, targetFactor := rm.ClassifyTier(count)
	return tier, count, targetFactor, obj.ReplicationFactor, nil
}

// Run starts the periodic adaptive replication evaluation loop.
func (rm *ReplicationManager) Run(ctx context.Context) {
	log.Printf("[REPLICATION_MANAGER] Starting adaptive replication loop — interval=%v", rm.interval)

	ticker := time.NewTicker(rm.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[REPLICATION_MANAGER] Shutting down.")
			return
		case <-ticker.C:
			if _, err := rm.RebalanceAll(ctx); err != nil {
				log.Printf("[REPLICATION_MANAGER] Error during rebalance sweep: %v", err)
			}
		}
	}
}

// RebalanceAll scans all objects, evaluates their 24h access frequency, and scales replicas up or down.
func (rm *ReplicationManager) RebalanceAll(ctx context.Context) ([]RebalanceResult, error) {
	objects, err := rm.objectRepo.GetAllObjects()
	if err != nil {
		return nil, fmt.Errorf("rebalance: get all objects: %w", err)
	}

	accessCounts, err := rm.objectRepo.GetObjectAccessCounts24h()
	if err != nil {
		return nil, fmt.Errorf("rebalance: get access counts: %w", err)
	}

	results := make([]RebalanceResult, 0, len(objects))
	for _, obj := range objects {
		select {
		case <-ctx.Done():
			return results, ctx.Err()
		default:
		}

		count := accessCounts[obj.ObjectID]
		res, err := rm.RebalanceObject(ctx, obj, count)
		if err != nil {
			log.Printf("[REPLICATION_MANAGER] ERROR rebalancing object %s (%s): %v", obj.ObjectID, obj.ObjectName, err)
			results = append(results, RebalanceResult{
				ObjectID:   obj.ObjectID,
				ObjectName: obj.ObjectName,
				Error:      err.Error(),
			})
			continue
		}
		if res != nil {
			results = append(results, *res)
		}
	}

	return results, nil
}

// RebalanceObject adjusts replica counts for a single object based on its classified tier.
func (rm *ReplicationManager) RebalanceObject(
	ctx context.Context,
	obj types.Object,
	accessCount int,
) (*RebalanceResult, error) {
	rm.mu.Lock()
	if _, busy := rm.inProgress[obj.ObjectID]; busy {
		rm.mu.Unlock()
		return nil, nil // Already being rebalanced or recovered
	}
	rm.inProgress[obj.ObjectID] = struct{}{}
	rm.mu.Unlock()

	defer func() {
		rm.mu.Lock()
		delete(rm.inProgress, obj.ObjectID)
		rm.mu.Unlock()
	}()

	tier, targetFactor := rm.ClassifyTier(accessCount)

	healthyReplicas, err := rm.replicaRepo.GetHealthyReplicasByObject(obj.ObjectID)
	if err != nil {
		return nil, fmt.Errorf("fetch healthy replicas: %w", err)
	}
	healthyCount := len(healthyReplicas)

	result := &RebalanceResult{
		ObjectID:        obj.ObjectID,
		ObjectName:      obj.ObjectName,
		Tier:            tier,
		AccessCount24h:  accessCount,
		InitialReplicas: healthyCount,
		TargetReplicas:  targetFactor,
		Action:          ActionNone,
	}

	// -------------------------------------------------------------------------
	// SCALE UP: Object is HOT or under-replicated (healthyCount < targetFactor)
	// -------------------------------------------------------------------------
	if healthyCount < targetFactor {
		if healthyCount == 0 {
			return nil, fmt.Errorf("cannot scale up object %s: zero healthy source replicas available", obj.ObjectID)
		}

		needed := targetFactor - healthyCount
		allReplicas, err := rm.replicaRepo.GetReplicasByObject(obj.ObjectID)
		if err != nil {
			return nil, fmt.Errorf("fetch all replicas: %w", err)
		}

		excludeIDs := make([]uuid.UUID, 0, len(allReplicas))
		for _, r := range allReplicas {
			excludeIDs = append(excludeIDs, r.NodeID)
		}

		destNodes, err := rm.placement.SelectNodesWithFileSize(ctx, needed, obj.FileSize, excludeIDs)
		if err != nil {
			return nil, fmt.Errorf("select destination nodes for scale up: %w", err)
		}

		donorReplica := healthyReplicas[0]
		donorNode, err := rm.nodeRepo.GetNodeByID(donorReplica.NodeID)
		if err != nil {
			return nil, fmt.Errorf("fetch donor node %s: %w", donorReplica.NodeID, err)
		}

		addedCount := 0
		for _, destNode := range destNodes {
			if err := rm.replicateChunk(ctx, obj, *donorNode, destNode); err != nil {
				log.Printf("[REPLICATION_MANAGER] Failed to replicate to %s: %v", destNode.Hostname, err)
				continue
			}
			addedCount++
		}

		if addedCount > 0 {
			result.Action = ActionScaleUp
			result.ReplicasAdded = addedCount
			_ = rm.objectRepo.UpdateReplicationFactor(obj.ObjectID, healthyCount+addedCount)

			_ = rm.logRepo.InsertSystemLog(
				"REPLICATION_SCALE_UP",
				fmt.Sprintf("Adaptive scale-up for object '%s' (%s): tier=%s, accesses=%d, replicas %d -> %d",
					obj.ObjectName, obj.ObjectID, tier, accessCount, healthyCount, healthyCount+addedCount),
				types.SeverityInfo,
				map[string]string{
					"object_id":     obj.ObjectID.String(),
					"object_name":   obj.ObjectName,
					"tier":          tier,
					"access_count":  fmt.Sprint(accessCount),
					"prev_replicas": fmt.Sprint(healthyCount),
					"new_replicas":  fmt.Sprint(healthyCount + addedCount),
				},
			)
		}
		return result, nil
	}

	// -------------------------------------------------------------------------
	// SCALE DOWN: Object is COLD or over-replicated (healthyCount > targetFactor)
	// Guard: Never scale down below MinReplicationFactor!
	// -------------------------------------------------------------------------
	if healthyCount > targetFactor && healthyCount > rm.cfg.MinReplicationFactor {
		excess := healthyCount - targetFactor
		// Ensure floor is respected
		if healthyCount-excess < rm.cfg.MinReplicationFactor {
			excess = healthyCount - rm.cfg.MinReplicationFactor
		}

		if excess <= 0 {
			return result, nil
		}

		removedCount := 0
		// Remove excess replicas (from the end of the slice)
		for i := 0; i < excess; i++ {
			repToRemove := healthyReplicas[len(healthyReplicas)-1-i]
			node, err := rm.nodeRepo.GetNodeByID(repToRemove.NodeID)
			if err != nil {
				log.Printf("[REPLICATION_MANAGER] Node %s not found for replica deletion: %v", repToRemove.NodeID, err)
				continue
			}

			// Delete chunk from physical storage node via internal API
			delURL := fmt.Sprintf("http://%s:%d/internal/storage/%s", node.Hostname, nodePort(node.IPAddress), obj.ObjectID)
			req, err := http.NewRequestWithContext(ctx, http.MethodDelete, delURL, nil)
			if err == nil {
				resp, err := rm.httpClient.Do(req)
				if err == nil {
					resp.Body.Close()
				}
			}

			// Delete replica record in DB
			if err := rm.replicaRepo.DeleteReplica(repToRemove.ReplicaID); err != nil {
				log.Printf("[REPLICATION_MANAGER] Error deleting replica record %s: %v", repToRemove.ReplicaID, err)
				continue
			}

			removedCount++
		}

		if removedCount > 0 {
			result.Action = ActionScaleDown
			result.ReplicasRemoved = removedCount
			_ = rm.objectRepo.UpdateReplicationFactor(obj.ObjectID, healthyCount-removedCount)

			_ = rm.logRepo.InsertSystemLog(
				"REPLICATION_SCALE_DOWN",
				fmt.Sprintf("Adaptive scale-down for object '%s' (%s): tier=%s, accesses=%d, replicas %d -> %d",
					obj.ObjectName, obj.ObjectID, tier, accessCount, healthyCount, healthyCount-removedCount),
				types.SeverityInfo,
				map[string]string{
					"object_id":     obj.ObjectID.String(),
					"object_name":   obj.ObjectName,
					"tier":          tier,
					"access_count":  fmt.Sprint(accessCount),
					"prev_replicas": fmt.Sprint(healthyCount),
					"new_replicas":  fmt.Sprint(healthyCount - removedCount),
				},
			)
		}
		return result, nil
	}

	return result, nil
}

// replicateChunk copies an object chunk from donorNode to destNode and registers the new replica.
func (rm *ReplicationManager) replicateChunk(
	ctx context.Context,
	obj types.Object,
	donorNode types.StorageNode,
	destNode types.StorageNode,
) error {
	donorURL := fmt.Sprintf("http://%s:%d/internal/storage/%s", donorNode.Hostname, nodePort(donorNode.IPAddress), obj.ObjectID)
	getReq, err := http.NewRequestWithContext(ctx, http.MethodGet, donorURL, nil)
	if err != nil {
		return fmt.Errorf("build donor request: %w", err)
	}

	getResp, err := rm.httpClient.Do(getReq)
	if err != nil {
		return fmt.Errorf("call donor node %s: %w", donorNode.Hostname, err)
	}
	defer getResp.Body.Close()

	if getResp.StatusCode != http.StatusOK {
		return fmt.Errorf("donor node returned HTTP %d", getResp.StatusCode)
	}

	var bodyBuf bytes.Buffer
	bodyWriter := multipart.NewWriter(&bodyBuf)

	if err := bodyWriter.WriteField("object_id", obj.ObjectID.String()); err != nil {
		return fmt.Errorf("write object_id field: %w", err)
	}

	filePart, err := bodyWriter.CreateFormFile("file", obj.ObjectID.String())
	if err != nil {
		return fmt.Errorf("create file part: %w", err)
	}

	hasher := sha256.New()
	tee := io.TeeReader(getResp.Body, hasher)

	written, err := io.Copy(filePart, tee)
	if err != nil {
		return fmt.Errorf("stream donor bytes: %w", err)
	}
	_ = bodyWriter.Close()

	computedChecksum := hex.EncodeToString(hasher.Sum(nil))
	if computedChecksum != obj.Checksum {
		return fmt.Errorf("checksum mismatch during replication: expected %s, got %s", obj.Checksum, computedChecksum)
	}

	destURL := fmt.Sprintf("http://%s:%d/internal/storage/store", destNode.Hostname, nodePort(destNode.IPAddress))
	postReq, err := http.NewRequestWithContext(ctx, http.MethodPost, destURL, &bodyBuf)
	if err != nil {
		return fmt.Errorf("build dest request: %w", err)
	}
	postReq.Header.Set("Content-Type", bodyWriter.FormDataContentType())

	postResp, err := rm.httpClient.Do(postReq)
	if err != nil {
		return fmt.Errorf("post to dest node %s: %w", destNode.Hostname, err)
	}
	defer postResp.Body.Close()

	if postResp.StatusCode != http.StatusOK {
		return fmt.Errorf("dest node %s returned HTTP %d", destNode.Hostname, postResp.StatusCode)
	}

	// Insert replica in DB with HEALTHY status
	rep, err := rm.replicaRepo.InsertReplica(obj.ObjectID, destNode.NodeID)
	if err != nil {
		return fmt.Errorf("insert replica record: %w", err)
	}
	if err := rm.replicaRepo.UpdateReplicaStatus(rep.ReplicaID, types.ReplicaStatusHealthy); err != nil {
		return fmt.Errorf("mark replica healthy: %w", err)
	}

	log.Printf("[REPLICATION_MANAGER] Replicated object %s (%d bytes) to node %s (checksum verified)",
		obj.ObjectID, written, destNode.Hostname)
	return nil
}

// nodePort returns the storage node port (defaults to 9001 in Docker network).
func nodePort(ipAddress string) int {
	return 9001
}
