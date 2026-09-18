package coordinator

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"sort"
	"sync"
	"time"

	"distributed-storage/pkg/config"
	"distributed-storage/pkg/types"

	"github.com/google/uuid"
)

// Repository interfaces for ReplicationManager decoupling
type ReplicationAccessLogRepo interface {
	GetAccessCountSince(objectID uuid.UUID, since time.Time) (int64, error)
	GetAccessCountsByObjectSince(since time.Time) (map[uuid.UUID]int64, error)
}

type ReplicationObjectRepo interface {
	GetAllObjects() ([]types.Object, error)
	GetObjectByID(objectID uuid.UUID) (*types.Object, error)
	UpdateReplicationFactor(objectID uuid.UUID, factor int) error
	UpdateReplicationFactorTx(tx *sql.Tx, objectID uuid.UUID, factor int) error
}

type ReplicationReplicaRepo interface {
	GetReplicasByObject(objectID uuid.UUID) ([]types.Replica, error)
	GetHealthyReplicasByObject(objectID uuid.UUID) ([]types.Replica, error)
	CountHealthyReplicas(objectID uuid.UUID) (int, error)
	InsertHealthyReplica(objectID, nodeID uuid.UUID) (*types.Replica, error)
	InsertHealthyReplicaTx(tx *sql.Tx, objectID, nodeID uuid.UUID) (*types.Replica, error)
	DeleteReplica(replicaID uuid.UUID) error
	DeleteReplicaTx(tx *sql.Tx, replicaID uuid.UUID) error
}

type ReplicationNodeRepo interface {
	GetNodeByID(nodeID uuid.UUID) (*types.StorageNode, error)
	GetOnlineNodes() ([]types.StorageNode, error)
}

type ReplicationLogRepo interface {
	InsertSystemLog(eventType string, description string, severity types.LogSeverity, metadata map[string]string) error
}

// ReplicationManager handles adaptive replication by monitoring object access frequencies,
// classifying them into HOT/WARM/COLD tiers, and dynamically scaling replicas up or down.
//
// Operational Policies:
//   - HOT (accessCount >= HotAccessThreshold): Scale replicas up to MaxReplicationFactor.
//   - WARM (ColdAccessThreshold <= accessCount < HotAccessThreshold): Maintain DefaultReplicationFactor.
//   - COLD (accessCount < ColdAccessThreshold): Scale replicas down to MinReplicationFactor.
//   - Minimum replica floor (MinReplicationFactor) is strictly enforced: an object is never
//     scaled down below MinReplicationFactor regardless of access count.
//   - All scale-down actions physically delete chunks from storage nodes via internal DELETE API
//     before deleting replica metadata.
//   - All scale-up actions stream data from healthy replicas, verify SHA-256 integrity,
//     and push to newly selected nodes via PlacementEngine.
type ReplicationManager struct {
	txRunner      TxBeginner
	accessLogRepo ReplicationAccessLogRepo
	objectRepo    ReplicationObjectRepo
	replicaRepo   ReplicationReplicaRepo
	nodeRepo      ReplicationNodeRepo
	logRepo       ReplicationLogRepo
	placement     PlacementSelector
	cfg           config.ReplicationConfig

	inProgress map[uuid.UUID]struct{}
	mu         sync.Mutex

	httpClient *http.Client
}

// NewReplicationManager constructs a ReplicationManager.
func NewReplicationManager(
	txRunner TxBeginner,
	accessLogRepo ReplicationAccessLogRepo,
	objectRepo ReplicationObjectRepo,
	replicaRepo ReplicationReplicaRepo,
	nodeRepo ReplicationNodeRepo,
	logRepo ReplicationLogRepo,
	placement PlacementSelector,
	cfg config.ReplicationConfig,
) *ReplicationManager {
	if cfg.CheckIntervalSeconds <= 0 {
		cfg.CheckIntervalSeconds = 30 * time.Second
	}
	if cfg.AccessWindowHours <= 0 {
		cfg.AccessWindowHours = 24 * time.Hour
	}
	return &ReplicationManager{
		txRunner:      txRunner,
		accessLogRepo: accessLogRepo,
		objectRepo:    objectRepo,
		replicaRepo:   replicaRepo,
		nodeRepo:      nodeRepo,
		logRepo:       logRepo,
		placement:     placement,
		cfg:           cfg,
		inProgress:    make(map[uuid.UUID]struct{}),
		httpClient:    &http.Client{Timeout: 5 * time.Minute},
	}
}

// Run starts the periodic adaptive replication evaluation loop.
// It blocks until ctx is cancelled.
func (rm *ReplicationManager) Run(ctx context.Context) {
	log.Printf("[REPLICATION_MANAGER] Starting — interval=%v, window=%v, hotThreshold=%d, coldThreshold=%d",
		rm.cfg.CheckIntervalSeconds, rm.cfg.AccessWindowHours, rm.cfg.HotAccessThreshold, rm.cfg.ColdAccessThreshold)

	ticker := time.NewTicker(rm.cfg.CheckIntervalSeconds)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[REPLICATION_MANAGER] Shutting down.")
			return
		case <-ticker.C:
			if err := rm.ReconcileAll(ctx); err != nil {
				log.Printf("[REPLICATION_MANAGER] Reconcile sweep error: %v", err)
			}
		}
	}
}

// ClassifyTier maps an access count over the evaluation window to an AccessTier
// and target replication factor.
func (rm *ReplicationManager) ClassifyTier(accessCount int64) (types.AccessTier, int) {
	switch {
	case accessCount >= int64(rm.cfg.HotAccessThreshold):
		return types.TierHot, rm.cfg.MaxReplicationFactor
	case accessCount < int64(rm.cfg.ColdAccessThreshold):
		return types.TierCold, rm.cfg.MinReplicationFactor
	default:
		return types.TierWarm, rm.cfg.DefaultReplicationFactor
	}
}

// GetObjectTier computes the current access tier and access count for an object.
func (rm *ReplicationManager) GetObjectTier(ctx context.Context, objectID uuid.UUID) (types.AccessTier, int64, int, error) {
	since := time.Now().Add(-rm.cfg.AccessWindowHours)
	count, err := rm.accessLogRepo.GetAccessCountSince(objectID, since)
	if err != nil {
		return "", 0, 0, fmt.Errorf("get access count for object %s: %w", objectID, err)
	}
	tier, targetFactor := rm.ClassifyTier(count)
	return tier, count, targetFactor, nil
}

// ReconcileAll evaluates all objects in the cluster and reconciles replication levels.
func (rm *ReplicationManager) ReconcileAll(ctx context.Context) error {
	objects, err := rm.objectRepo.GetAllObjects()
	if err != nil {
		return fmt.Errorf("fetch objects for replication reconciliation: %w", err)
	}

	for _, obj := range objects {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := rm.ReconcileObject(ctx, obj.ObjectID); err != nil {
			log.Printf("[REPLICATION_MANAGER] Error reconciling object %s (%s): %v",
				obj.ObjectID, obj.ObjectName, err)
		}
	}
	return nil
}

// ReconcileObject evaluates a single object's access frequency, determines its target tier,
// and scales replicas up or down accordingly.
func (rm *ReplicationManager) ReconcileObject(ctx context.Context, objectID uuid.UUID) error {
	// Deduplication guard
	rm.mu.Lock()
	if _, busy := rm.inProgress[objectID]; busy {
		rm.mu.Unlock()
		return nil
	}
	rm.inProgress[objectID] = struct{}{}
	rm.mu.Unlock()

	defer func() {
		rm.mu.Lock()
		delete(rm.inProgress, objectID)
		rm.mu.Unlock()
	}()

	obj, err := rm.objectRepo.GetObjectByID(objectID)
	if err != nil {
		return fmt.Errorf("fetch object %s: %w", objectID, err)
	}

	tier, accessCount, targetFactor, err := rm.GetObjectTier(ctx, objectID)
	if err != nil {
		return err
	}

	healthyCount, err := rm.replicaRepo.CountHealthyReplicas(objectID)
	if err != nil {
		return fmt.Errorf("count healthy replicas for %s: %w", objectID, err)
	}

	switch {
	case healthyCount < targetFactor:
		return rm.scaleUp(ctx, obj, tier, accessCount, healthyCount, targetFactor)
	case healthyCount > targetFactor:
		return rm.scaleDown(ctx, obj, tier, accessCount, healthyCount, targetFactor)
	default:
		// Healthy replica count equals target. Update object replication_factor if tier changed.
		if obj.ReplicationFactor != targetFactor {
			_ = rm.objectRepo.UpdateReplicationFactor(objectID, targetFactor)
			_ = rm.logRepo.InsertSystemLog(types.EventReplicationTierChange,
				fmt.Sprintf("Object %s (%s) tier updated to %s with target factor %d",
					obj.ObjectName, objectID, tier, targetFactor),
				types.SeverityInfo,
				map[string]string{
					"object_id":     objectID.String(),
					"tier":          string(tier),
					"access_count":  fmt.Sprint(accessCount),
					"target_factor": fmt.Sprint(targetFactor),
				},
			)
		}
		return nil
	}
}

// scaleUp provisions additional replicas on eligible nodes.
func (rm *ReplicationManager) scaleUp(
	ctx context.Context,
	obj *types.Object,
	tier types.AccessTier,
	accessCount int64,
	currentCount int,
	targetFactor int,
) error {
	needed := targetFactor - currentCount
	log.Printf("[REPLICATION_MANAGER] Scaling UP object %s (%s) from %d to %d replicas (tier: %s, accesses: %d)",
		obj.ObjectName, obj.ObjectID, currentCount, targetFactor, tier, accessCount)

	// Fetch all current replicas to exclude existing host nodes from placement.
	allReplicas, err := rm.replicaRepo.GetReplicasByObject(obj.ObjectID)
	if err != nil {
		return fmt.Errorf("fetch replicas: %w", err)
	}
	excludeIDs := make([]uuid.UUID, 0, len(allReplicas))
	for _, r := range allReplicas {
		excludeIDs = append(excludeIDs, r.NodeID)
	}

	// Select destination nodes using PlacementEngine
	destNodes, err := rm.placement.SelectNodes(ctx, needed, excludeIDs)
	if err != nil {
		return fmt.Errorf("select destination nodes for scale up: %w", err)
	}

	// Fetch healthy source replicas to stream data from
	sources, err := rm.replicaRepo.GetHealthyReplicasByObject(obj.ObjectID)
	if err != nil || len(sources) == 0 {
		return fmt.Errorf("no healthy source replicas available to copy from")
	}

	// Stream object data with source failover & checksum verification
	var objectData []byte
	var sourceNode *types.StorageNode
	for _, sRep := range sources {
		sNode, err := rm.nodeRepo.GetNodeByID(sRep.NodeID)
		if err != nil {
			continue
		}
		data, err := rm.fetchFromNode(ctx, *sNode, obj.ObjectID)
		if err != nil {
			continue
		}
		if sha256sum(data) != obj.Checksum {
			_ = rm.logRepo.InsertSystemLog(types.EventChecksumMismatch,
				fmt.Sprintf("Checksum mismatch during scale-up replication from node %s", sNode.Hostname),
				types.SeverityCritical,
				map[string]string{
					"object_id":   obj.ObjectID.String(),
					"source_node": sNode.Hostname,
				},
			)
			continue
		}
		objectData = data
		sourceNode = sNode
		break
	}

	if sourceNode == nil || len(objectData) == 0 {
		return fmt.Errorf("failed to retrieve valid data from any source replica for scale up")
	}

	// Push to each destination node and create healthy replica record
	createdCount := 0
	for _, destNode := range destNodes {
		if err := rm.pushToNode(ctx, destNode, obj.ObjectID, objectData); err != nil {
			log.Printf("[REPLICATION_MANAGER] Failed to push to %s during scale-up: %v", destNode.Hostname, err)
			continue
		}

		err = rm.runInTx(func(tx *sql.Tx) error {
			_, err := rm.replicaRepo.InsertHealthyReplicaTx(tx, obj.ObjectID, destNode.NodeID)
			return err
		})
		if err != nil {
			log.Printf("[REPLICATION_MANAGER] Failed to record replica on %s: %v", destNode.Hostname, err)
			_ = rm.deleteFromNode(ctx, destNode, obj.ObjectID)
			continue
		}
		createdCount++
	}

	if createdCount > 0 {
		newTotal := currentCount + createdCount
		_ = rm.objectRepo.UpdateReplicationFactor(obj.ObjectID, newTotal)

		_ = rm.logRepo.InsertSystemLog(types.EventReplicationScaleUp,
			fmt.Sprintf("Scaled up object %s (%s) to %d replicas (tier %s)",
				obj.ObjectName, obj.ObjectID, newTotal, tier),
			types.SeverityInfo,
			map[string]string{
				"object_id":    obj.ObjectID.String(),
				"tier":         string(tier),
				"access_count": fmt.Sprint(accessCount),
				"replicas":     fmt.Sprint(newTotal),
			},
		)
	}

	return nil
}

// scaleDown prunes excess replicas while strictly preserving MinReplicationFactor.
func (rm *ReplicationManager) scaleDown(
	ctx context.Context,
	obj *types.Object,
	tier types.AccessTier,
	accessCount int64,
	currentCount int,
	targetFactor int,
) error {
	// Guard: Never scale down below MinReplicationFactor
	if targetFactor < rm.cfg.MinReplicationFactor {
		targetFactor = rm.cfg.MinReplicationFactor
	}
	if currentCount <= targetFactor || currentCount <= rm.cfg.MinReplicationFactor {
		return nil
	}

	excess := currentCount - targetFactor
	if currentCount-excess < rm.cfg.MinReplicationFactor {
		excess = currentCount - rm.cfg.MinReplicationFactor
	}
	if excess <= 0 {
		return nil
	}

	log.Printf("[REPLICATION_MANAGER] Scaling DOWN object %s (%s) from %d to %d replicas (tier: %s, accesses: %d)",
		obj.ObjectName, obj.ObjectID, currentCount, targetFactor, tier, accessCount)

	healthyReplicas, err := rm.replicaRepo.GetHealthyReplicasByObject(obj.ObjectID)
	if err != nil {
		return fmt.Errorf("fetch healthy replicas for scale down: %w", err)
	}
	if len(healthyReplicas) <= rm.cfg.MinReplicationFactor {
		return nil
	}

	// Pair replicas with node metrics to prune from highest-utilized or highest-latency nodes first.
	type scoredReplica struct {
		replica types.Replica
		node    types.StorageNode
		load    float64
	}

	var candidates []scoredReplica
	for _, rep := range healthyReplicas {
		node, err := rm.nodeRepo.GetNodeByID(rep.NodeID)
		if err != nil {
			candidates = append(candidates, scoredReplica{replica: rep, load: 100.0})
			continue
		}
		load := node.CPUUsage + node.MemoryUsage + (node.Latency / 10.0)
		candidates = append(candidates, scoredReplica{replica: rep, node: *node, load: load})
	}

	// Sort descending by load (prune highest load nodes first)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].load > candidates[j].load
	})

	prunedCount := 0
	for i := 0; i < excess && i < len(candidates); i++ {
		target := candidates[i]

		// Safety check: ensure at least MinReplicationFactor healthy replicas remain
		remaining, _ := rm.replicaRepo.CountHealthyReplicas(obj.ObjectID)
		if remaining <= rm.cfg.MinReplicationFactor {
			break
		}

		// 1. Physically delete chunk from storage node
		if target.node.Hostname != "" {
			if err := rm.deleteFromNode(ctx, target.node, obj.ObjectID); err != nil {
				log.Printf("[REPLICATION_MANAGER] WARN: failed to delete chunk on node %s: %v",
					target.node.Hostname, err)
			}
		}

		// 2. Delete replica record in DB
		err = rm.runInTx(func(tx *sql.Tx) error {
			return rm.replicaRepo.DeleteReplicaTx(tx, target.replica.ReplicaID)
		})
		if err != nil {
			log.Printf("[REPLICATION_MANAGER] ERROR deleting replica record %s: %v",
				target.replica.ReplicaID, err)
			continue
		}
		prunedCount++
	}

	if prunedCount > 0 {
		newTotal := currentCount - prunedCount
		_ = rm.objectRepo.UpdateReplicationFactor(obj.ObjectID, newTotal)

		_ = rm.logRepo.InsertSystemLog(types.EventReplicationScaleDown,
			fmt.Sprintf("Scaled down object %s (%s) to %d replicas (tier %s)",
				obj.ObjectName, obj.ObjectID, newTotal, tier),
			types.SeverityInfo,
			map[string]string{
				"object_id":    obj.ObjectID.String(),
				"tier":         string(tier),
				"access_count": fmt.Sprint(accessCount),
				"replicas":     fmt.Sprint(newTotal),
			},
		)
	}

	return nil
}

// --- HTTP Helpers ---

func (rm *ReplicationManager) runInTx(fn func(*sql.Tx) error) error {
	if rm.txRunner == nil {
		return fn(nil)
	}
	tx, err := rm.txRunner.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (rm *ReplicationManager) fetchFromNode(
	ctx context.Context,
	node types.StorageNode,
	objectID uuid.UUID,
) ([]byte, error) {
	url := fmt.Sprintf("%s/internal/storage/%s", buildNodeURL(node), objectID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build GET request to %s: %w", url, err)
	}

	resp, err := rm.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s returned HTTP %d", url, resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body from %s: %w", url, err)
	}
	return data, nil
}

func (rm *ReplicationManager) pushToNode(
	ctx context.Context,
	node types.StorageNode,
	objectID uuid.UUID,
	data []byte,
) error {
	url := fmt.Sprintf("%s/internal/storage/store", buildNodeURL(node))

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	if err := writer.WriteField("object_id", objectID.String()); err != nil {
		return fmt.Errorf("write object_id field: %w", err)
	}

	part, err := writer.CreateFormFile("file", objectID.String())
	if err != nil {
		return fmt.Errorf("create form file part: %w", err)
	}
	if _, err := io.Copy(part, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("write file part: %w", err)
	}
	writer.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, &body)
	if err != nil {
		return fmt.Errorf("build POST request to %s: %w", url, err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := rm.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("POST %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("POST %s returned HTTP %d", url, resp.StatusCode)
	}
	return nil
}

func (rm *ReplicationManager) deleteFromNode(
	ctx context.Context,
	node types.StorageNode,
	objectID uuid.UUID,
) error {
	url := fmt.Sprintf("%s/internal/storage/%s", buildNodeURL(node), objectID)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("build DELETE request to %s: %w", url, err)
	}

	resp, err := rm.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("DELETE %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("DELETE %s returned HTTP %d", url, resp.StatusCode)
	}
	return nil
}
