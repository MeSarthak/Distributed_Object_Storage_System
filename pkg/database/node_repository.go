package database

import (
	"database/sql"
	"fmt"
	"time"

	"distributed-storage/pkg/types"

	"github.com/google/uuid"
)

// NodeRepository provides typed query methods for the storage_nodes table.
type NodeRepository struct {
	db *DB
}

// NewNodeRepository constructs a NodeRepository from the shared DB handle.
func NewNodeRepository(db *DB) *NodeRepository {
	return &NodeRepository{db: db}
}

// UpsertNode registers a node the first time it is seen or refreshes its metrics
// and heartbeat timestamp on subsequent calls. The upsert key is the node's hostname
// so that a restarted container with a new UUID still maps to the same slot.
func (r *NodeRepository) UpsertNode(payload types.HeartbeatPayload) error {
	query := `
		INSERT INTO storage_nodes
			(node_id, hostname, ip_address, total_storage, used_storage,
			 cpu_usage, memory_usage, latency, status, last_heartbeat)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'ONLINE', $9)
		ON CONFLICT (hostname) DO UPDATE SET
			ip_address     = EXCLUDED.ip_address,
			total_storage  = EXCLUDED.total_storage,
			used_storage   = EXCLUDED.used_storage,
			cpu_usage      = EXCLUDED.cpu_usage,
			memory_usage   = EXCLUDED.memory_usage,
			latency        = EXCLUDED.latency,
			last_heartbeat = EXCLUDED.last_heartbeat,
			status         = CASE
				WHEN storage_nodes.status = 'OFFLINE'
				     THEN 'ONLINE'
				ELSE storage_nodes.status
			END
	`
	_, err := r.db.Exec(query,
		payload.NodeID,
		payload.Hostname,
		payload.IPAddress,
		payload.TotalStorage,
		payload.UsedStorage,
		payload.CPUUsage,
		payload.MemoryUsage,
		payload.LatencyMs,
		payload.Timestamp,
	)
	if err != nil {
		return fmt.Errorf("upsert node %s: %w", payload.Hostname, err)
	}
	return nil
}

// UpdateHeartbeat refreshes only the metrics and last_heartbeat of an existing node.
// It does NOT change the status; that is managed exclusively by the failure detector
// and the UpsertNode re-activation logic above.
func (r *NodeRepository) UpdateHeartbeat(nodeID uuid.UUID, payload types.HeartbeatPayload) error {
	query := `
		UPDATE storage_nodes SET
			ip_address     = $2,
			total_storage  = $3,
			used_storage   = $4,
			cpu_usage      = $5,
			memory_usage   = $6,
			latency        = $7,
			last_heartbeat = $8
		WHERE node_id = $1
	`
	res, err := r.db.Exec(query,
		nodeID,
		payload.IPAddress,
		payload.TotalStorage,
		payload.UsedStorage,
		payload.CPUUsage,
		payload.MemoryUsage,
		payload.LatencyMs,
		payload.Timestamp,
	)
	if err != nil {
		return fmt.Errorf("update heartbeat for node %s: %w", nodeID, err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("node %s not found during heartbeat update", nodeID)
	}
	return nil
}

// MarkNodeOffline transitions a single node to OFFLINE status.
// This is called exclusively by the failure detector, so it is idempotent
// (calling it on an already-OFFLINE node is a no-op at the DB level).
func (r *NodeRepository) MarkNodeOffline(nodeID uuid.UUID) error {
	query := `UPDATE storage_nodes SET status = 'OFFLINE' WHERE node_id = $1`
	_, err := r.db.Exec(query, nodeID)
	if err != nil {
		return fmt.Errorf("mark node %s offline: %w", nodeID, err)
	}
	return nil
}

// GetAllNodes returns every row in storage_nodes.
func (r *NodeRepository) GetAllNodes() ([]types.StorageNode, error) {
	query := `
		SELECT node_id, hostname, ip_address, total_storage, used_storage,
		       cpu_usage, memory_usage, latency, status, last_heartbeat
		FROM storage_nodes
		ORDER BY hostname
	`
	rows, err := r.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("get all nodes: %w", err)
	}
	defer rows.Close()
	return scanNodes(rows)
}

// GetOnlineNodes returns only nodes with status = 'ONLINE'.
func (r *NodeRepository) GetOnlineNodes() ([]types.StorageNode, error) {
	query := `
		SELECT node_id, hostname, ip_address, total_storage, used_storage,
		       cpu_usage, memory_usage, latency, status, last_heartbeat
		FROM storage_nodes
		WHERE status = 'ONLINE'
		ORDER BY hostname
	`
	rows, err := r.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("get online nodes: %w", err)
	}
	defer rows.Close()
	return scanNodes(rows)
}

// GetOfflineNodes returns only nodes with status = 'OFFLINE'.
func (r *NodeRepository) GetOfflineNodes() ([]types.StorageNode, error) {
	query := `
		SELECT node_id, hostname, ip_address, total_storage, used_storage,
		       cpu_usage, memory_usage, latency, status, last_heartbeat
		FROM storage_nodes
		WHERE status = 'OFFLINE'
		ORDER BY hostname
	`
	rows, err := r.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("get offline nodes: %w", err)
	}
	defer rows.Close()
	return scanNodes(rows)
}

// GetNodeByID looks up a single node by its primary key.
func (r *NodeRepository) GetNodeByID(nodeID uuid.UUID) (*types.StorageNode, error) {
	query := `
		SELECT node_id, hostname, ip_address, total_storage, used_storage,
		       cpu_usage, memory_usage, latency, status, last_heartbeat
		FROM storage_nodes
		WHERE node_id = $1
	`
	row := r.db.QueryRow(query, nodeID)
	node, err := scanNode(row)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}
	if err != nil {
		return nil, fmt.Errorf("get node by id %s: %w", nodeID, err)
	}
	return node, nil
}

// --- internal scan helpers ---------------------------------------------------

func scanNodes(rows *sql.Rows) ([]types.StorageNode, error) {
	var nodes []types.StorageNode
	for rows.Next() {
		var n types.StorageNode
		var lastHB time.Time
		err := rows.Scan(
			&n.NodeID, &n.Hostname, &n.IPAddress,
			&n.TotalStorage, &n.UsedStorage,
			&n.CPUUsage, &n.MemoryUsage, &n.Latency,
			&n.Status, &lastHB,
		)
		if err != nil {
			return nil, fmt.Errorf("scan node row: %w", err)
		}
		n.LastHeartbeat = lastHB
		nodes = append(nodes, n)
	}
	return nodes, rows.Err()
}

func scanNode(row *sql.Row) (*types.StorageNode, error) {
	var n types.StorageNode
	var lastHB time.Time
	err := row.Scan(
		&n.NodeID, &n.Hostname, &n.IPAddress,
		&n.TotalStorage, &n.UsedStorage,
		&n.CPUUsage, &n.MemoryUsage, &n.Latency,
		&n.Status, &lastHB,
	)
	if err != nil {
		return nil, err
	}
	n.LastHeartbeat = lastHB
	return &n, nil
}
