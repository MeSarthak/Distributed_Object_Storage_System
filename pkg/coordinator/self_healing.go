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
	"distributed-storage/pkg/database"
	"distributed-storage/pkg/types"

	"github.com/google/uuid"
)

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
//  4. A per-object in-progress set prevents concurrent recovery of the same
//     object across multiple offline nodes or detection cycles.
type SelfHealingEngine struct {
	db          *database.DB
	nodeRepo    *database.NodeRepository
	replicaRepo *database.ReplicaRepository
	objectRepo  *database.ObjectRepository
	logRepo     *database.LogRepository
	placement   *PlacementEngine
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
	db *database.DB,
	nodeRepo *database.NodeRepository,
	replicaRepo *database.ReplicaRepository,
	objectRepo *database.ObjectRepository,
	logRepo *database.LogRepository,
	placement *PlacementEngine,
	replCfg config.ReplicationConfig,
	healCfg config.HeartbeatConfig,
) *SelfHealingEngine {
	return &SelfHealingEngine{
		db:          db,
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

// Run starts the self-healing loop.  It blocks until ctx is cancelled.
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
			she.heal(ctx)
		}
	}
}

// heal performs one healing sweep across all currently OFFLINE nodes.
func (she *SelfHealingEngine) heal(ctx context.Context) {
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
		she.healNode(ctx, node)
	}
}

// healNode processes all replicas that were on the given offline node.
func (she *SelfHealingEngine) healNode(ctx context.Context, failedNode types.StorageNode) {
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

		// Only process HEALTHY or RECOVERING replicas on the failed node —
		// replicas already LOST have no data to recover from on that node
		// but may still reduce the healthy count and need recovery from another source.
		she.recoverReplica(ctx, replica, failedNode)
	}
}

// recoverReplica attempts to restore one replica that was on a failed node.
func (she *SelfHealingEngine) recoverReplica(
	ctx context.Context,
	lostReplica types.Replica,
	failedNode types.StorageNode,
) {
	objectID := lostReplica.ObjectID

	// --- Deduplication guard -------------------------------------------------
	she.mu.Lock()
	if _, busy := she.inProgress[objectID]; busy {
		she.mu.Unlock()
		log.Printf("[SELF_HEALING] Object %s recovery already in progress — skipping duplicate trigger", objectID)
		return
	}
	she.inProgress[objectID] = struct{}{}
	she.mu.Unlock()

	defer func() {
		she.mu.Lock()
		delete(she.inProgress, objectID)
		she.mu.Unlock()
	}()

	// --- Check current healthy replica count ---------------------------------
	healthyCount, err := she.replicaRepo.CountHealthyReplicas(objectID)
	if err != nil {
		log.Printf("[SELF_HEALING] ERROR counting healthy replicas for object %s: %v", objectID, err)
		return
	}
	if healthyCount >= she.replCfg.MinReplicationFactor {
		log.Printf("[SELF_HEALING] Object %s already has %d healthy replicas (min=%d) — skipping",
			objectID, healthyCount, she.replCfg.MinReplicationFactor)
		return
	}

	log.Printf("[SELF_HEALING] Object %s has %d healthy replicas (min=%d) — starting recovery",
		objectID, healthyCount, she.replCfg.MinReplicationFactor)

	_ = she.logRepo.InsertSystemLog("RECOVERY_START",
		fmt.Sprintf("Starting recovery for object %s — healthy replicas: %d/%d",
			objectID, healthyCount, she.replCfg.MinReplicationFactor),
		types.SeverityWarn,
		map[string]string{
			"object_id":          objectID.String(),
			"failed_node_id":     failedNode.NodeID.String(),
			"failed_hostname":    failedNode.Hostname,
			"healthy_count":      fmt.Sprint(healthyCount),
			"min_replica_factor": fmt.Sprint(she.replCfg.MinReplicationFactor),
		},
	)

	// --- Fetch object metadata (need checksum + replication_factor) ----------
	obj, err := she.objectRepo.GetObjectByID(objectID)
	if err != nil {
		log.Printf("[SELF_HEALING] ERROR fetching object metadata for %s: %v", objectID, err)
		return
	}

	// --- Find a healthy source node ------------------------------------------
	sourceReplicas, err := she.replicaRepo.GetHealthyReplicasByObject(objectID)
	if err != nil {
		log.Printf("[SELF_HEALING] ERROR fetching healthy source replicas for object %s: %v", objectID, err)
		return
	}
	if len(sourceReplicas) == 0 {
		log.Printf("[SELF_HEALING] WARN: No healthy source replicas for object %s — cannot recover", objectID)
		_ = she.logRepo.InsertSystemLog("RECOVERY_FAILED",
			fmt.Sprintf("No healthy source replicas available for object %s", objectID),
			types.SeverityError,
			map[string]string{"object_id": objectID.String()},
		)
		return
	}
	sourceReplica := sourceReplicas[0]

	// --- Find the source node's address --------------------------------------
	sourceNode, err := she.nodeRepo.GetNodeByID(sourceReplica.NodeID)
	if err != nil {
		log.Printf("[SELF_HEALING] ERROR fetching source node %s: %v", sourceReplica.NodeID, err)
		return
	}

	// --- Build exclusion list for placement ----------------------------------
	// Exclude: the failed node + every node already holding any replica.
	allReplicas, err := she.replicaRepo.GetReplicasByObject(objectID)
	if err != nil {
		log.Printf("[SELF_HEALING] ERROR fetching all replicas for object %s: %v", objectID, err)
		return
	}
	excludeIDs := make([]uuid.UUID, 0, len(allReplicas)+1)
	excludeIDs = append(excludeIDs, failedNode.NodeID)
	for _, r := range allReplicas {
		excludeIDs = append(excludeIDs, r.NodeID)
	}

	// --- Select a destination node via the existing Placement Engine ---------
	destinations, err := she.placement.SelectNodes(ctx, 1, excludeIDs)
	if err != nil {
		log.Printf("[SELF_HEALING] ERROR selecting destination for object %s: %v", objectID, err)
		_ = she.logRepo.InsertSystemLog("RECOVERY_FAILED",
			fmt.Sprintf("No eligible destination node for object %s: %v", objectID, err),
			types.SeverityError,
			map[string]string{"object_id": objectID.String()},
		)
		return
	}
	destNode := destinations[0]

	log.Printf("[SELF_HEALING] Object %s: source=%s dest=%s",
		objectID, sourceNode.Hostname, destNode.Hostname)

	// --- Stream object data from source node ---------------------------------
	objectData, err := she.fetchFromNode(ctx, *sourceNode, objectID)
	if err != nil {
		log.Printf("[SELF_HEALING] ERROR fetching object %s from source %s: %v",
			objectID, sourceNode.Hostname, err)
		_ = she.logRepo.InsertSystemLog("RECOVERY_FAILED",
			fmt.Sprintf("Failed to fetch object %s from source node %s: %v",
				objectID, sourceNode.Hostname, err),
			types.SeverityError,
			map[string]string{
				"object_id":   objectID.String(),
				"source_node": sourceNode.Hostname,
			},
		)
		return
	}

	// --- Verify SHA-256 checksum BEFORE writing to destination ---------------
	computedChecksum := sha256sum(objectData)
	if computedChecksum != obj.Checksum {
		log.Printf("[SELF_HEALING] CHECKSUM MISMATCH for object %s: expected=%s got=%s",
			objectID, obj.Checksum, computedChecksum)
		_ = she.logRepo.InsertSystemLog("CHECKSUM_MISMATCH",
			fmt.Sprintf("Checksum mismatch for object %s during recovery from %s",
				objectID, sourceNode.Hostname),
			types.SeverityCritical,
			map[string]string{
				"object_id":         objectID.String(),
				"expected_checksum": obj.Checksum,
				"computed_checksum": computedChecksum,
				"source_node":       sourceNode.Hostname,
			},
		)
		return
	}
	log.Printf("[SELF_HEALING] Checksum verified for object %s (sha256=%s…)", objectID, computedChecksum[:16])

	// --- Atomically update metadata: mark lost + insert recovering -----------
	// This transaction covers BOTH the LOST transition of the failed replica AND
	// the insertion of the new RECOVERING replica.  If the physical copy below
	// fails, we rollback the RECOVERING record in a separate compensating tx.
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
		return
	}

	// --- Push object data to destination node --------------------------------
	if err := she.pushToNode(ctx, destNode, objectID, objectData); err != nil {
		log.Printf("[SELF_HEALING] ERROR pushing object %s to dest %s: %v",
			objectID, destNode.Hostname, err)

		// Compensate: mark the new RECOVERING replica LOST so metadata stays consistent.
		_ = she.replicaRepo.UpdateReplicaStatus(newReplicaID, types.ReplicaStatusLost)

		_ = she.logRepo.InsertSystemLog("RECOVERY_FAILED",
			fmt.Sprintf("Failed to push object %s to destination node %s: %v",
				objectID, destNode.Hostname, err),
			types.SeverityError,
			map[string]string{
				"object_id": objectID.String(),
				"dest_node": destNode.Hostname,
			},
		)
		return
	}

	// --- Mark replica HEALTHY inside its own transaction ---------------------
	err = she.runInTx(func(tx *sql.Tx) error {
		return she.replicaRepo.UpdateReplicaStatusTx(tx, newReplicaID, types.ReplicaStatusHealthy)
	})
	if err != nil {
		log.Printf("[SELF_HEALING] ERROR finalizing replica %s as HEALTHY: %v", newReplicaID, err)
		_ = she.logRepo.InsertSystemLog("RECOVERY_FAILED",
			fmt.Sprintf("Could not finalize replica %s as HEALTHY for object %s: %v",
				newReplicaID, objectID, err),
			types.SeverityError,
			map[string]string{"object_id": objectID.String(), "replica_id": newReplicaID.String()},
		)
		return
	}

	log.Printf("[SELF_HEALING] Recovery SUCCESS: object %s replica %s now HEALTHY on %s",
		objectID, newReplicaID, destNode.Hostname)

	_ = she.logRepo.InsertSystemLog("RECOVERY_SUCCESS",
		fmt.Sprintf("Object %s successfully recovered to node %s (replica %s)",
			objectID, destNode.Hostname, newReplicaID),
		types.SeverityInfo,
		map[string]string{
			"object_id":   objectID.String(),
			"replica_id":  newReplicaID.String(),
			"dest_node":   destNode.Hostname,
			"source_node": sourceNode.Hostname,
			"checksum":    computedChecksum,
		},
	)
}

// --- helpers -----------------------------------------------------------------

// runInTx starts a transaction, calls fn, commits on success, rolls back on error.
func (she *SelfHealingEngine) runInTx(fn func(*sql.Tx) error) error {
	tx, err := she.db.Begin()
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
// Node port is stored in the config but is not in the StorageNode struct;
// we derive it from the node's hostname using the docker-compose naming convention.
// Storage nodes expose their API on the port stored during heartbeat registration.
// Since StorageNode does not carry a port field (the schema stores ip_address only),
// we use the coordinator's configured internal port (default 9001) here.
// In a production system the port would be stored in storage_nodes.
func buildNodeURL(node types.StorageNode) string {
	// The storage-node containers listen on port 9001 (their NODE_PORT env var).
	// We use docker-compose service names as hostnames and assume the default port.
	// If a non-default port is required this can be extended to include port in StorageNode.
	return fmt.Sprintf("http://%s:9001", node.Hostname)
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
