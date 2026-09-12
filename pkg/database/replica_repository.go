package database

import (
	"database/sql"
	"fmt"
	"time"

	"distributed-storage/pkg/types"

	"github.com/google/uuid"
)

// ReplicaRepository provides typed query methods for the replicas table.
type ReplicaRepository struct {
	db *DB
}

// NewReplicaRepository constructs a ReplicaRepository from the shared DB handle.
func NewReplicaRepository(db *DB) *ReplicaRepository {
	return &ReplicaRepository{db: db}
}

// GetReplicasByNode returns all replicas that are physically stored on a given node,
// regardless of their current status.
func (r *ReplicaRepository) GetReplicasByNode(nodeID uuid.UUID) ([]types.Replica, error) {
	query := `
		SELECT replica_id, object_id, node_id, status, created_at
		FROM replicas
		WHERE node_id = $1
	`
	rows, err := r.db.Query(query, nodeID)
	if err != nil {
		return nil, fmt.Errorf("get replicas by node %s: %w", nodeID, err)
	}
	defer rows.Close()
	return scanReplicas(rows)
}

// GetReplicasByObject returns all replicas for a given object, regardless of status.
func (r *ReplicaRepository) GetReplicasByObject(objectID uuid.UUID) ([]types.Replica, error) {
	query := `
		SELECT replica_id, object_id, node_id, status, created_at
		FROM replicas
		WHERE object_id = $1
	`
	rows, err := r.db.Query(query, objectID)
	if err != nil {
		return nil, fmt.Errorf("get replicas by object %s: %w", objectID, err)
	}
	defer rows.Close()
	return scanReplicas(rows)
}

// GetHealthyReplicasByObject returns only HEALTHY replicas for an object.
// These are candidates for use as copy-source during self-healing.
func (r *ReplicaRepository) GetHealthyReplicasByObject(objectID uuid.UUID) ([]types.Replica, error) {
	query := `
		SELECT r.replica_id, r.object_id, r.node_id, r.status, r.created_at
		FROM replicas r
		JOIN storage_nodes sn ON sn.node_id = r.node_id
		WHERE r.object_id = $1
		  AND r.status    = 'HEALTHY'
		  AND sn.status   = 'ONLINE'
	`
	rows, err := r.db.Query(query, objectID)
	if err != nil {
		return nil, fmt.Errorf("get healthy replicas for object %s: %w", objectID, err)
	}
	defer rows.Close()
	return scanReplicas(rows)
}

// CountHealthyReplicas returns the number of HEALTHY replicas for an object
// whose host node is also ONLINE.
func (r *ReplicaRepository) CountHealthyReplicas(objectID uuid.UUID) (int, error) {
	query := `
		SELECT COUNT(*)
		FROM replicas r
		JOIN storage_nodes sn ON sn.node_id = r.node_id
		WHERE r.object_id = $1
		  AND r.status    = 'HEALTHY'
		  AND sn.status   = 'ONLINE'
	`
	var count int
	err := r.db.QueryRow(query, objectID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count healthy replicas for object %s: %w", objectID, err)
	}
	return count, nil
}

// InsertReplica creates a new replica record in RECOVERING state.
// The caller is responsible for ensuring the (object_id, node_id) pair is unique
// to avoid a conflict with the UNIQUE constraint.
func (r *ReplicaRepository) InsertReplica(objectID, nodeID uuid.UUID) (*types.Replica, error) {
	query := `
		INSERT INTO replicas (replica_id, object_id, node_id, status, created_at)
		VALUES ($1, $2, $3, 'RECOVERING', NOW())
		RETURNING replica_id, object_id, node_id, status, created_at
	`
	replicaID := uuid.New()
	row := r.db.QueryRow(query, replicaID, objectID, nodeID)
	replica, err := scanReplicaRow(row)
	if err != nil {
		return nil, fmt.Errorf("insert replica for object %s on node %s: %w", objectID, nodeID, err)
	}
	return replica, nil
}

// InsertReplicaTx creates a new RECOVERING replica within an existing transaction.
func (r *ReplicaRepository) InsertReplicaTx(tx *sql.Tx, objectID, nodeID uuid.UUID) (*types.Replica, error) {
	query := `
		INSERT INTO replicas (replica_id, object_id, node_id, status, created_at)
		VALUES ($1, $2, $3, 'RECOVERING', NOW())
		RETURNING replica_id, object_id, node_id, status, created_at
	`
	replicaID := uuid.New()
	row := tx.QueryRow(query, replicaID, objectID, nodeID)
	replica, err := scanReplicaRow(row)
	if err != nil {
		return nil, fmt.Errorf("insert replica tx for object %s on node %s: %w", objectID, nodeID, err)
	}
	return replica, nil
}

// UpdateReplicaStatus changes the status of a specific replica.
func (r *ReplicaRepository) UpdateReplicaStatus(replicaID uuid.UUID, status types.ReplicaStatus) error {
	query := `UPDATE replicas SET status = $2 WHERE replica_id = $1`
	res, err := r.db.Exec(query, replicaID, status)
	if err != nil {
		return fmt.Errorf("update replica %s status to %s: %w", replicaID, status, err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("replica %s not found for status update", replicaID)
	}
	return nil
}

// UpdateReplicaStatusTx changes replica status inside an existing transaction.
func (r *ReplicaRepository) UpdateReplicaStatusTx(tx *sql.Tx, replicaID uuid.UUID, status types.ReplicaStatus) error {
	query := `UPDATE replicas SET status = $2 WHERE replica_id = $1`
	res, err := tx.Exec(query, replicaID, status)
	if err != nil {
		return fmt.Errorf("tx update replica %s to %s: %w", replicaID, status, err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("replica %s not found in tx status update", replicaID)
	}
	return nil
}

// MarkReplicaLost is a convenience wrapper that sets a replica to LOST status.
func (r *ReplicaRepository) MarkReplicaLost(replicaID uuid.UUID) error {
	return r.UpdateReplicaStatus(replicaID, types.ReplicaStatusLost)
}

// MarkReplicaLostTx marks a replica LOST inside an existing transaction.
func (r *ReplicaRepository) MarkReplicaLostTx(tx *sql.Tx, replicaID uuid.UUID) error {
	return r.UpdateReplicaStatusTx(tx, replicaID, types.ReplicaStatusLost)
}

// --- internal scan helpers ---------------------------------------------------

func scanReplicas(rows *sql.Rows) ([]types.Replica, error) {
	var replicas []types.Replica
	for rows.Next() {
		r, err := scanReplicaCols(rows)
		if err != nil {
			return nil, err
		}
		replicas = append(replicas, r)
	}
	return replicas, rows.Err()
}

// scanReplicaCols scans from *sql.Rows.
func scanReplicaCols(rows *sql.Rows) (types.Replica, error) {
	var rep types.Replica
	var createdAt time.Time
	err := rows.Scan(&rep.ReplicaID, &rep.ObjectID, &rep.NodeID, &rep.Status, &createdAt)
	if err != nil {
		return rep, fmt.Errorf("scan replica row: %w", err)
	}
	rep.CreatedAt = createdAt
	return rep, nil
}

// scanReplicaRow scans from *sql.Row (single-row query result).
func scanReplicaRow(row *sql.Row) (*types.Replica, error) {
	var rep types.Replica
	var createdAt time.Time
	err := row.Scan(&rep.ReplicaID, &rep.ObjectID, &rep.NodeID, &rep.Status, &createdAt)
	if err != nil {
		return nil, fmt.Errorf("scan single replica row: %w", err)
	}
	rep.CreatedAt = createdAt
	return &rep, nil
}
