package coordinator

import (
	"context"
	"log"
	"sync"
	"time"

	"distributed-storage/pkg/config"
	"distributed-storage/pkg/database"
	"distributed-storage/pkg/types"

	"github.com/google/uuid"
)

// FailureDetector runs a background goroutine that periodically checks all
// storage nodes and transitions any node whose last_heartbeat has exceeded the
// configured timeout from ONLINE → OFFLINE.
//
// Design principles enforced here:
//   - Each ONLINE→OFFLINE transition is logged exactly once per failure event.
//   - A node that was already OFFLINE is silently skipped (no duplicate log).
//   - When a node returns to ONLINE (i.e., UpsertNode re-activates it on the
//     next valid heartbeat) it is removed from the "already reported" set so
//     that a subsequent failure will be logged again.
//   - Failures in the detection loop are logged but never crash the goroutine.
type FailureDetector struct {
	nodeRepo *database.NodeRepository
	logRepo  *database.LogRepository
	cfg      config.HeartbeatConfig

	// alreadyOffline tracks node IDs that we have already emitted a
	// NODE_FAILURE log entry for in the current failure episode.
	// Protected by mu.
	alreadyOffline map[uuid.UUID]struct{}
	mu             sync.Mutex
}

// NewFailureDetector constructs a FailureDetector.
func NewFailureDetector(
	nodeRepo *database.NodeRepository,
	logRepo *database.LogRepository,
	cfg config.HeartbeatConfig,
) *FailureDetector {
	return &FailureDetector{
		nodeRepo:       nodeRepo,
		logRepo:        logRepo,
		cfg:            cfg,
		alreadyOffline: make(map[uuid.UUID]struct{}),
	}
}

// Run starts the failure-detection loop.  It blocks until ctx is cancelled
// and should therefore be launched in its own goroutine.
func (fd *FailureDetector) Run(ctx context.Context) {
	log.Printf("[FAILURE_DETECTOR] Starting — interval=%v, timeout=%v",
		fd.cfg.IntervalSeconds, fd.cfg.TimeoutSeconds)

	ticker := time.NewTicker(fd.cfg.IntervalSeconds)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[FAILURE_DETECTOR] Shutting down.")
			return
		case <-ticker.C:
			fd.detect(ctx)
		}
	}
}

// detect performs one detection sweep.
func (fd *FailureDetector) detect(ctx context.Context) {
	nodes, err := fd.nodeRepo.GetAllNodes()
	if err != nil {
		log.Printf("[FAILURE_DETECTOR] ERROR fetching nodes: %v", err)
		return
	}

	now := time.Now()
	fd.mu.Lock()
	defer fd.mu.Unlock()

	for _, node := range nodes {
		select {
		case <-ctx.Done():
			return
		default:
		}

		timedOut := now.Sub(node.LastHeartbeat) > fd.cfg.TimeoutSeconds

		switch {
		case node.Status == types.NodeStatusOnline && timedOut:
			// Transition ONLINE → OFFLINE.
			if err := fd.nodeRepo.MarkNodeOffline(node.NodeID); err != nil {
				log.Printf("[FAILURE_DETECTOR] ERROR marking node %s offline: %v", node.Hostname, err)
				continue
			}

			fd.alreadyOffline[node.NodeID] = struct{}{}

			log.Printf("[FAILURE_DETECTOR] Node %s (%s) transitioned ONLINE → OFFLINE (last heartbeat: %v ago)",
				node.Hostname, node.NodeID, now.Sub(node.LastHeartbeat).Round(time.Second))

			_ = fd.logRepo.InsertSystemLog(
				"NODE_FAILURE",
				"Node "+node.Hostname+" ("+node.NodeID.String()+") transitioned ONLINE → OFFLINE: no heartbeat for "+
					now.Sub(node.LastHeartbeat).Round(time.Second).String(),
				types.SeverityCritical,
				map[string]string{
					"node_id":        node.NodeID.String(),
					"hostname":       node.Hostname,
					"last_heartbeat": node.LastHeartbeat.UTC().Format(time.RFC3339),
					"elapsed":        now.Sub(node.LastHeartbeat).Round(time.Second).String(),
				},
			)

		case node.Status == types.NodeStatusOffline && timedOut:
			// Still offline — do not log again.

		case node.Status == types.NodeStatusOnline && !timedOut:
			// Node is healthy; clear it from the already-offline set in case it
			// previously recovered (UpsertNode sets it back to ONLINE on heartbeat).
			if _, wasOffline := fd.alreadyOffline[node.NodeID]; wasOffline {
				delete(fd.alreadyOffline, node.NodeID)
				log.Printf("[FAILURE_DETECTOR] Node %s (%s) is back ONLINE — cleared from failure set",
					node.Hostname, node.NodeID)
			}

		case node.Status == types.NodeStatusOffline && !timedOut:
			// Node came back ONLINE at the DB level (UpsertNode re-activated it)
			// but our local cache still shows it offline.
			// This branch is a safety net to clear the cache.
			if _, wasOffline := fd.alreadyOffline[node.NodeID]; wasOffline {
				delete(fd.alreadyOffline, node.NodeID)
				log.Printf("[FAILURE_DETECTOR] Node %s (%s) now shows recent heartbeat — cleared from failure set",
					node.Hostname, node.NodeID)
			}
		}
	}
}
