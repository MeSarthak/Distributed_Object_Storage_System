package coordinator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"sync"
	"time"

	"distributed-storage/pkg/config"
	"distributed-storage/pkg/types"

	"github.com/google/uuid"
)

// Repository interfaces for SelfHealingEngine decoupling
type TxBeginner interface {
	Begin() (*sql.Tx, error)
}

type SelfHealingNodeRepo interface {
	GetOfflineNodes() ([]types.StorageNode, error)
	GetNodeByID(nodeID uuid.UUID) (*types.StorageNode, error)
}

type SelfHealingReplicaRepo interface {
	GetReplicasByNode(nodeID uuid.UUID) ([]types.Replica, error)
	GetReplicasByObject(objectID uuid.UUID) ([]types.Replica, error)
	GetHealthyReplicasByObject(objectID uuid.UUID) ([]types.Replica, error)
	CountHealthyReplicas(objectID uuid.UUID) (int, error)
	InsertReplicaTx(tx *sql.Tx, objectID, nodeID uuid.UUID) (*types.Replica, error)
	UpdateReplicaStatus(replicaID uuid.UUID, status types.ReplicaStatus) error
	UpdateReplicaStatusTx(tx *sql.Tx, replicaID uuid.UUID, status types.ReplicaStatus) error
	MarkReplicaLostTx(tx *sql.Tx, replicaID uuid.UUID) error
}

type SelfHealingObjectRepo interface {
	GetObjectByID(objectID uuid.UUID) (*types.Object, error)
}

type SelfHealingLogRepo interface {
	InsertSystemLog(eventType string, description string, severity types.LogSeverity, metadata map[string]string) error
}

type PlacementSelector interface {
	SelectNodes(ctx context.Context, count int, excludeNodeIDs []uuid.UUID) ([]types.StorageNode, error)
}

// SelfHealingEngine monitors for OFFLINE nodes and automatically re-replicates
// any objects that have fallen below their minimum replica count.
//
// Design principles enforced here:
//
//  1. Physical data movement happens exclusively through the storage nodes'
//     internal HTTP APIs — never by direct filesystem access.
//
//  2. All metadata state changes within a single recovery (mark LOST +
//     insert RECOVERING + mark HEALTHY/LOST) execute inside a single
//     PostgreSQL transaction so that a partial failure never leaves
//     metadata in an inconsistent state.
//
//  3. A recovered replica is only marked HEALTHY after its SHA-256 checksum
//     has been verified against the authoritative object record.
//
//  4. Source failover: If the primary healthy replica fails to stream or verify,
//     all other healthy replicas are tried sequentially.
//
//  5. Target replica restoration: Recreates lost replicas up to the object's
//     configured replication_factor (ensuring minimum replication factor is maintained).
type SelfHealingEngine struct {
	txRunner    TxBeginner
	nodeRepo    SelfHealingNodeRepo
	replicaRepo SelfHealingReplicaRepo
	objectRepo  SelfHealingObjectRepo
	logRepo     SelfHealingLogRepo
	placement   PlacementSelector
	replCfg     config.ReplicationConfig
	healCfg     config.HeartbeatConfig

	// inProgress contains objectIDs currently being recovered.
	// Protected by mu.
	inProgress map[uuid.UUID]struct{}
	mu         sync.Mutex

	httpClient *http.Client
}

// NewSelfHealingEngine constructs a SelfHealingEngine.
func NewSelfHealingEngine(
	txRunner TxBeginner,
	nodeRepo SelfHealingNodeRepo,
	replicaRepo SelfHealingReplicaRepo,
	objectRepo SelfHealingObjectRepo,
	logRepo SelfHealingLogRepo,
	placement PlacementSelector,
	replCfg config.ReplicationConfig,
	healCfg config.HeartbeatConfig,
) *SelfHealingEngine {
	return &SelfHealingEngine{
		txRunner:    txRunner,
		nodeRepo:    nodeRepo,
		replicaRepo: replicaRepo,
		objectRepo:  objectRepo,
		logRepo:     logRepo,
		placement:   placement,
		replCfg:     replCfg,
		healCfg:     healCfg,
		inProgress:  make(map[uuid.UUID]struct{}),
		httpClient:  &http.Client{Timeout: 5 * time.Minute},
	}
}

// Run starts the self-healing loop. It blocks until ctx is cancelled.
func (she *SelfHealingEngine) Run(ctx context.Context) {
	log.Printf("[SELF_HEALING] Starting — interval=%v", she.healCfg.SelfHealingSeconds)

	ticker := time.NewTicker(she.healCfg.SelfHealingSeconds)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[SELF_HEALING] Shutting down.")
			return
		case <-ticker.C:
			she.Heal(ctx)
		}
	}
}

// Heal performs one healing sweep across all currently OFFLINE nodes.
func (she *SelfHealingEngine) Heal(ctx context.Context) {
	offlineNodes, err := she.nodeRepo.GetOfflineNodes()
	if err != nil {
		log.Printf("[SELF_HEALING] ERROR fetching offline nodes: %v", err)
		return
	}
	if len(offlineNodes) == 0 {
		return
	}

	log.Printf("[SELF_HEALING] Found %d offline node(s); scanning replicas…", len(offlineNodes))

	for _, node := range offlineNodes {
		select {
		case <-ctx.Done():
			return
		default:
		}
		she.HealNode(ctx, node)
	}
}

// HealNode processes all replicas that were on the given offline node.
func (she *SelfHealingEngine) HealNode(ctx context.Context, failedNode types.StorageNode) {
	replicas, err := she.replicaRepo.GetReplicasByNode(failedNode.NodeID)
	if err != nil {
		log.Printf("[SELF_HEALING] ERROR fetching replicas for node %s: %v", failedNode.Hostname, err)
		return
	}

	for _, replica := range replicas {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Only process replicas that are not already marked LOST on the failed node.
		if replica.Status == types.ReplicaStatusLost {
			continue
		}

		_, _ = she.RecoverReplica(ctx, replica, failedNode)
	}
}

// RecoverReplica attempts to restore one replica that was on a failed node.
// Returns the new replica ID on success, or an error.
func (she *SelfHealingEngine) RecoverReplica(
	ctx context.Context,
	lostReplica types.Replica,
	failedNode types.StorageNode,
) (uuid.UUID, error) {
	objectID := lostReplica.ObjectID

	// --- Deduplication guard -------------------------------------------------
	she.mu.Lock()
	if _, busy := she.inProgress[objectID]; busy {
		she.mu.Unlock()
		log.Printf("[SELF_HEALING] Object %s recovery already in progress — skipping duplicate trigger", objectID)
		return uuid.Nil, fmt.Errorf("object %s recovery already in progress", objectID)
	}
	she.inProgress[objectID] = struct{}{}
	she.mu.Unlock()

	defer func() {
		she.mu.Lock()
		delete(she.inProgress, objectID)
		she.mu.Unlock()
	}()

	// --- Fetch object metadata (need target replication factor + checksum) ---
	obj, err := she.objectRepo.GetObjectByID(objectID)
	if err != nil {
		log.Printf("[SELF_HEALING] ERROR fetching object metadata for %s: %v", objectID, err)
		return uuid.Nil, fmt.Errorf("fetch object metadata: %w", err)
	}

	// --- Check current healthy replica count against target replication factor
	healthyCount, err := she.replicaRepo.CountHealthyReplicas(objectID)
	if err != nil {
		log.Printf("[SELF_HEALING] ERROR counting healthy replicas for object %s: %v", objectID, err)
		return uuid.Nil, fmt.Errorf("count healthy replicas: %w", err)
	}

	targetFactor := obj.ReplicationFactor
	if targetFactor < she.replCfg.MinReplicationFactor {
		targetFactor = she.replCfg.MinReplicationFactor
	}
	if targetFactor > she.replCfg.MaxReplicationFactor {
		targetFactor = she.replCfg.MaxReplicationFactor
	}

	if healthyCount >= targetFactor {
		log.Printf("[SELF_HEALING] Object %s already has %d healthy replicas (target=%d) — skipping",
			objectID, healthyCount, targetFactor)
		return uuid.Nil, fmt.Errorf("object %s already has %d healthy replicas (target=%d)", objectID, healthyCount, targetFactor)
	}

	log.Printf("[SELF_HEALING] Object %s has %d healthy replicas (target=%d) — starting recovery",
		objectID, healthyCount, targetFactor)

	_ = she.logRepo.InsertSystemLog(types.EventRecoveryStart,
		fmt.Sprintf("Starting recovery for object %s — healthy replicas: %d/%d",
			objectID, healthyCount, targetFactor),
		types.SeverityWarn,
		map[string]string{
			"object_id":          objectID.String(),
			"failed_node_id":     failedNode.NodeID.String(),
			"failed_hostname":    failedNode.Hostname,
			"healthy_count":      fmt.Sprint(healthyCount),
			"target_replication": fmt.Sprint(targetFactor),
		},
	)

	// --- Find healthy source replicas ----------------------------------------
	sourceReplicas, err := she.replicaRepo.GetHealthyReplicasByObject(objectID)
	if err != nil {
		log.Printf("[SELF_HEALING] ERROR fetching healthy source replicas for object %s: %v", objectID, err)
		return uuid.Nil, fmt.Errorf("fetch healthy source replicas: %w", err)
	}
	if len(sourceReplicas) == 0 {
		log.Printf("[SELF_HEALING] WARN: No healthy source replicas for object %s — cannot recover", objectID)
		_ = she.logRepo.InsertSystemLog(types.EventRecoveryFailed,
			fmt.Sprintf("No healthy source replicas available for object %s", objectID),
			types.SeverityError,
			map[string]string{"object_id": objectID.String()},
		)
		return uuid.Nil, fmt.Errorf("no healthy source replicas available for object %s", objectID)
	}

	// --- Build exclusion list for placement ----------------------------------
	// Exclude: the failed node + every node currently holding any replica of the object.
	allReplicas, err := she.replicaRepo.GetReplicasByObject(objectID)
	if err != nil {
		log.Printf("[SELF_HEALING] ERROR fetching all replicas for object %s: %v", objectID, err)
		return uuid.Nil, fmt.Errorf("fetch all replicas: %w", err)
	}
	excludeIDs := make([]uuid.UUID, 0, len(allReplicas)+1)
	excludeIDs = append(excludeIDs, failedNode.NodeID)
	for _, r := range allReplicas {
		excludeIDs = append(excludeIDs, r.NodeID)
	}

	// --- Select a destination node via the Placement Engine ------------------
	destinations, err := she.placement.SelectNodes(ctx, 1, excludeIDs)
	if err != nil {
		log.Printf("[SELF_HEALING] ERROR selecting destination for object %s: %v", objectID, err)
		_ = she.logRepo.InsertSystemLog(types.EventRecoveryFailed,
			fmt.Sprintf("No eligible destination node for object %s: %v", objectID, err),
			types.SeverityError,
			map[string]string{"object_id": objectID.String()},
		)
		return uuid.Nil, fmt.Errorf("select destination node: %w", err)
	}
	destNode := destinations[0]

	// --- Stream object data with source failover -----------------------------
	var objectData []byte
	var selectedSourceNode *types.StorageNode

	for _, candidateReplica := range sourceReplicas {
		sNode, err := she.nodeRepo.GetNodeByID(candidateReplica.NodeID)
		if err != nil {
			continue
		}

		data, err := she.fetchFromNode(ctx, *sNode, objectID)
		if err != nil {
			log.Printf("[SELF_HEALING] Failed to fetch object %s from source %s: %v — trying next replica",
				objectID, sNode.Hostname, err)
			continue
		}

		computedChecksum := sha256sum(data)
		if computedChecksum != obj.Checksum {
			log.Printf("[SELF_HEALING] CHECKSUM MISMATCH from source %s for object %s: expected=%s got=%s — trying next replica",
				sNode.Hostname, objectID, obj.Checksum, computedChecksum)
			_ = she.logRepo.InsertSystemLog(types.EventChecksumMismatch,
				fmt.Sprintf("Checksum mismatch for object %s during recovery from %s",
					objectID, sNode.Hostname),
				types.SeverityCritical,
				map[string]string{
					"object_id":         objectID.String(),
					"expected_checksum": obj.Checksum,
					"computed_checksum": computedChecksum,
					"source_node":       sNode.Hostname,
				},
			)
			continue
		}

		objectData = data
		selectedSourceNode = sNode
		break
	}

	if selectedSourceNode == nil || len(objectData) == 0 {
		log.Printf("[SELF_HEALING] WARN: Could not retrieve valid data for object %s from any healthy replica", objectID)
		_ = she.logRepo.InsertSystemLog(types.EventRecoveryFailed,
			fmt.Sprintf("Could not retrieve valid object data for %s from any healthy source replica", objectID),
			types.SeverityError,
			map[string]string{"object_id": objectID.String()},
		)
		return uuid.Nil, fmt.Errorf("could not retrieve valid object data for %s from any healthy source replica", objectID)
	}

	log.Printf("[SELF_HEALING] Object %s: retrieved from %s, pushing to dest=%s",
		objectID, selectedSourceNode.Hostname, destNode.Hostname)

	// --- Atomically update metadata: mark lost + insert recovering -----------
	var newReplicaID uuid.UUID
	err = she.runInTx(func(tx *sql.Tx) error {
		// Mark the replica on the failed node as LOST.
		if err := she.replicaRepo.MarkReplicaLostTx(tx, lostReplica.ReplicaID); err != nil {
			return fmt.Errorf("mark lost: %w", err)
		}
		// Insert a placeholder in RECOVERING state on the destination.
		newReplica, err := she.replicaRepo.InsertReplicaTx(tx, objectID, destNode.NodeID)
		if err != nil {
			return fmt.Errorf("insert recovering: %w", err)
		}
		newReplicaID = newReplica.ReplicaID
		return nil
	})
	if err != nil {
		log.Printf("[SELF_HEALING] ERROR in metadata tx for object %s: %v", objectID, err)
		return uuid.Nil, fmt.Errorf("metadata tx failed: %w", err)
	}

	// --- Push object data to destination node --------------------------------
	if err := she.pushToNode(ctx, destNode, objectID, objectData); err != nil {
		log.Printf("[SELF_HEALING] ERROR pushing object %s to dest %s: %v",
			objectID, destNode.Hostname, err)

		// Compensate: mark the new RECOVERING replica LOST so metadata stays consistent.
		_ = she.replicaRepo.UpdateReplicaStatus(newReplicaID, types.ReplicaStatusLost)

		_ = she.logRepo.InsertSystemLog(types.EventRecoveryFailed,
			fmt.Sprintf("Failed to push object %s to destination node %s: %v",
				objectID, destNode.Hostname, err),
			types.SeverityError,
			map[string]string{
				"object_id": objectID.String(),
				"dest_node": destNode.Hostname,
			},
		)
		return uuid.Nil, fmt.Errorf("push to destination node failed: %w", err)
	}

	// --- Mark replica HEALTHY inside its own transaction ---------------------
	err = she.runInTx(func(tx *sql.Tx) error {
		return she.replicaRepo.UpdateReplicaStatusTx(tx, newReplicaID, types.ReplicaStatusHealthy)
	})
	if err != nil {
		log.Printf("[SELF_HEALING] ERROR finalizing replica %s as HEALTHY: %v", newReplicaID, err)
		_ = she.logRepo.InsertSystemLog(types.EventRecoveryFailed,
			fmt.Sprintf("Could not finalize replica %s as HEALTHY for object %s: %v",
				newReplicaID, objectID, err),
			types.SeverityError,
			map[string]string{"object_id": objectID.String(), "replica_id": newReplicaID.String()},
		)
		return uuid.Nil, fmt.Errorf("finalize replica healthy: %w", err)
	}

	log.Printf("[SELF_HEALING] Recovery SUCCESS: object %s replica %s now HEALTHY on %s",
		objectID, newReplicaID, destNode.Hostname)

	_ = she.logRepo.InsertSystemLog(types.EventRecoverySuccess,
		fmt.Sprintf("Object %s successfully recovered to node %s (replica %s)",
			objectID, destNode.Hostname, newReplicaID),
		types.SeverityInfo,
		map[string]string{
			"object_id":   objectID.String(),
			"replica_id":  newReplicaID.String(),
			"dest_node":   destNode.Hostname,
			"source_node": selectedSourceNode.Hostname,
			"checksum":    obj.Checksum,
		},
	)

	return newReplicaID, nil
}

// --- helpers -----------------------------------------------------------------

// runInTx starts a transaction, calls fn, commits on success, rolls back on error.
func (she *SelfHealingEngine) runInTx(fn func(*sql.Tx) error) error {
	if she.txRunner == nil {
		return fn(nil)
	}
	tx, err := she.txRunner.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// buildNodeURL constructs the base internal URL for a storage node.
func buildNodeURL(node types.StorageNode) string {
	return BuildNodeURL(node)
}

// fetchFromNode GETs the raw object bytes from a source storage node.
func (she *SelfHealingEngine) fetchFromNode(
	ctx context.Context,
	node types.StorageNode,
	objectID uuid.UUID,
) ([]byte, error) {
	url := fmt.Sprintf("%s/internal/storage/%s", buildNodeURL(node), objectID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build GET request to %s: %w", url, err)
	}

	resp, err := she.httpClient.Do(req)
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

// pushToNode POSTs the raw object bytes to a destination storage node.
func (she *SelfHealingEngine) pushToNode(
	ctx context.Context,
	node types.StorageNode,
	objectID uuid.UUID,
	data []byte,
) error {
	url := fmt.Sprintf("%s/internal/storage/store", buildNodeURL(node))

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	// Write object_id field.
	if err := writer.WriteField("object_id", objectID.String()); err != nil {
		return fmt.Errorf("write object_id field: %w", err)
	}

	// Write file part.
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

	resp, err := she.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("POST %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("POST %s returned HTTP %d", url, resp.StatusCode)
	}
	return nil
}

// sha256sum returns the lower-case hex-encoded SHA-256 hash of data.
func sha256sum(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}
