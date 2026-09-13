package database

import (
	"database/sql"
	"fmt"
	"time"

	"distributed-storage/pkg/types"

	"github.com/google/uuid"
)

// ObjectRepository provides typed query methods for the objects table.
type ObjectRepository struct {
	db *DB
}

// NewObjectRepository constructs an ObjectRepository from the shared DB handle.
func NewObjectRepository(db *DB) *ObjectRepository {
	return &ObjectRepository{db: db}
}

// GetObjectByID retrieves the full metadata for a single object by its primary key.
// Returns an error wrapping sql.ErrNoRows when the object does not exist.
func (r *ObjectRepository) GetObjectByID(objectID uuid.UUID) (*types.Object, error) {
	query := `
		SELECT object_id, owner_id, object_name, file_size, mime_type,
		       checksum, upload_time, last_accessed, replication_factor
		FROM objects
		WHERE object_id = $1
	`
	row := r.db.QueryRow(query, objectID)
	obj, err := scanObject(row)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("object %s not found: %w", objectID, err)
	}
	if err != nil {
		return nil, fmt.Errorf("get object by id %s: %w", objectID, err)
	}
	return obj, nil
}

// scanObject scans a single *sql.Row into an Object struct.
func scanObject(row *sql.Row) (*types.Object, error) {
	var obj types.Object
	var uploadTime, lastAccessed time.Time
	err := row.Scan(
		&obj.ObjectID,
		&obj.OwnerID,
		&obj.ObjectName,
		&obj.FileSize,
		&obj.MimeType,
		&obj.Checksum,
		&uploadTime,
		&lastAccessed,
		&obj.ReplicationFactor,
	)
	if err != nil {
		return nil, err
	}
	obj.UploadTime = uploadTime
	obj.LastAccessed = lastAccessed
	return &obj, nil
}

// GetAllObjects retrieves metadata for all objects.
func (r *ObjectRepository) GetAllObjects() ([]types.Object, error) {
	query := `
		SELECT object_id, owner_id, object_name, file_size, mime_type,
		       checksum, upload_time, last_accessed, replication_factor
		FROM objects
		ORDER BY upload_time DESC
	`
	rows, err := r.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("get all objects: %w", err)
	}
	defer rows.Close()

	var objects []types.Object
	for rows.Next() {
		var obj types.Object
		var uploadTime, lastAccessed time.Time
		err := rows.Scan(
			&obj.ObjectID,
			&obj.OwnerID,
			&obj.ObjectName,
			&obj.FileSize,
			&obj.MimeType,
			&obj.Checksum,
			&uploadTime,
			&lastAccessed,
			&obj.ReplicationFactor,
		)
		if err != nil {
			return nil, fmt.Errorf("scan object: %w", err)
		}
		obj.UploadTime = uploadTime
		obj.LastAccessed = lastAccessed
		objects = append(objects, obj)
	}
	return objects, rows.Err()
}

// UpdateReplicationFactor updates the target replication factor for an object.
func (r *ObjectRepository) UpdateReplicationFactor(objectID uuid.UUID, factor int) error {
	query := `UPDATE objects SET replication_factor = $2 WHERE object_id = $1`
	res, err := r.db.Exec(query, objectID, factor)
	if err != nil {
		return fmt.Errorf("update replication factor for object %s: %w", objectID, err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("object %s not found for replication factor update", objectID)
	}
	return nil
}

// GetAccessCount24h returns the number of accesses for an object in the past 24 hours.
func (r *ObjectRepository) GetAccessCount24h(objectID uuid.UUID) (int, error) {
	query := `
		SELECT COUNT(*)
		FROM access_logs
		WHERE object_id = $1
		  AND access_time >= NOW() - INTERVAL '24 HOURS'
	`
	var count int
	err := r.db.QueryRow(query, objectID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("get access count for object %s: %w", objectID, err)
	}
	return count, nil
}

// GetObjectAccessCounts24h returns a map of object_id -> 24h access count for all objects accessed in the last 24h.
func (r *ObjectRepository) GetObjectAccessCounts24h() (map[uuid.UUID]int, error) {
	query := `
		SELECT object_id, COUNT(*)
		FROM access_logs
		WHERE access_time >= NOW() - INTERVAL '24 HOURS'
		  AND object_id IS NOT NULL
		GROUP BY object_id
	`
	rows, err := r.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("get 24h object access counts: %w", err)
	}
	defer rows.Close()

	counts := make(map[uuid.UUID]int)
	for rows.Next() {
		var objID uuid.UUID
		var count int
		if err := rows.Scan(&objID, &count); err != nil {
			return nil, fmt.Errorf("scan access count row: %w", err)
		}
		counts[objID] = count
	}
	return counts, rows.Err()
}

// RecordAccess logs an access event for an object in access_logs and updates objects.last_accessed.
func (r *ObjectRepository) RecordAccess(objectID, userID *uuid.UUID, responseTimeMs int) error {
	query := `
		INSERT INTO access_logs (access_id, object_id, user_id, access_time, response_time)
		VALUES ($1, $2, $3, NOW(), $4)
	`
	_, err := r.db.Exec(query, uuid.New(), objectID, userID, responseTimeMs)
	if err != nil {
		return fmt.Errorf("insert access log: %w", err)
	}

	if objectID != nil {
		updateQuery := `UPDATE objects SET last_accessed = NOW() WHERE object_id = $1`
		_, _ = r.db.Exec(updateQuery, *objectID)
	}
	return nil
}

