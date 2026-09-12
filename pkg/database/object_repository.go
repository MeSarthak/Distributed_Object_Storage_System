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
