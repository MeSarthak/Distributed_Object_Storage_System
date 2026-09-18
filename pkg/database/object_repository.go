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

// GetAllObjects returns all object records currently in the database.
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
			return nil, fmt.Errorf("scan object row: %w", err)
		}
		obj.UploadTime = uploadTime
		obj.LastAccessed = lastAccessed
		objects = append(objects, obj)
	}
	return objects, rows.Err()
}

// UpdateReplicationFactor updates the target replication factor of an object.
func (r *ObjectRepository) UpdateReplicationFactor(objectID uuid.UUID, factor int) error {
	query := `UPDATE objects SET replication_factor = $2 WHERE object_id = $1`
	res, err := r.db.Exec(query, objectID, factor)
	if err != nil {
		return fmt.Errorf("update replication factor for %s to %d: %w", objectID, factor, err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("object %s not found for replication factor update", objectID)
	}
	return nil
}

// UpdateReplicationFactorTx updates the target replication factor within an existing transaction.
func (r *ObjectRepository) UpdateReplicationFactorTx(tx *sql.Tx, objectID uuid.UUID, factor int) error {
	query := `UPDATE objects SET replication_factor = $2 WHERE object_id = $1`
	res, err := tx.Exec(query, objectID, factor)
	if err != nil {
		return fmt.Errorf("tx update replication factor for %s to %d: %w", objectID, factor, err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("object %s not found in tx replication factor update", objectID)
	}
	return nil
}

// CreateObject inserts a new object metadata row into the objects table.
func (r *ObjectRepository) CreateObject(obj *types.Object) error {
	query := `
		INSERT INTO objects (object_id, owner_id, object_name, file_size, mime_type, checksum, upload_time, last_accessed, replication_factor)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`
	if obj.ObjectID == uuid.Nil {
		obj.ObjectID = uuid.New()
	}
	if obj.UploadTime.IsZero() {
		obj.UploadTime = time.Now().UTC()
	}
	if obj.LastAccessed.IsZero() {
		obj.LastAccessed = obj.UploadTime
	}
	if obj.ReplicationFactor <= 0 {
		obj.ReplicationFactor = 3
	}

	_, err := r.db.Exec(query,
		obj.ObjectID,
		obj.OwnerID,
		obj.ObjectName,
		obj.FileSize,
		obj.MimeType,
		obj.Checksum,
		obj.UploadTime,
		obj.LastAccessed,
		obj.ReplicationFactor,
	)
	if err != nil {
		return fmt.Errorf("create object %s: %w", obj.ObjectName, err)
	}
	return nil
}

// CreateObjectTx inserts a new object metadata row within a transaction.
func (r *ObjectRepository) CreateObjectTx(tx *sql.Tx, obj *types.Object) error {
	query := `
		INSERT INTO objects (object_id, owner_id, object_name, file_size, mime_type, checksum, upload_time, last_accessed, replication_factor)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`
	if obj.ObjectID == uuid.Nil {
		obj.ObjectID = uuid.New()
	}
	if obj.UploadTime.IsZero() {
		obj.UploadTime = time.Now().UTC()
	}
	if obj.LastAccessed.IsZero() {
		obj.LastAccessed = obj.UploadTime
	}
	if obj.ReplicationFactor <= 0 {
		obj.ReplicationFactor = 3
	}

	_, err := tx.Exec(query,
		obj.ObjectID,
		obj.OwnerID,
		obj.ObjectName,
		obj.FileSize,
		obj.MimeType,
		obj.Checksum,
		obj.UploadTime,
		obj.LastAccessed,
		obj.ReplicationFactor,
	)
	if err != nil {
		return fmt.Errorf("create object tx %s: %w", obj.ObjectName, err)
	}
	return nil
}

// UpdateLastAccessed refreshes the last_accessed timestamp for an object.
func (r *ObjectRepository) UpdateLastAccessed(objectID uuid.UUID, t time.Time) error {
	query := `UPDATE objects SET last_accessed = $2 WHERE object_id = $1`
	_, err := r.db.Exec(query, objectID, t)
	if err != nil {
		return fmt.Errorf("update last_accessed for %s: %w", objectID, err)
	}
	return nil
}

// DeleteObject deletes an object record by ID. Foreign key constraints with ON DELETE CASCADE
// will automatically clean up referencing replicas.
func (r *ObjectRepository) DeleteObject(objectID uuid.UUID) error {
	query := `DELETE FROM objects WHERE object_id = $1`
	res, err := r.db.Exec(query, objectID)
	if err != nil {
		return fmt.Errorf("delete object %s: %w", objectID, err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("object %s not found for deletion", objectID)
	}
	return nil
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
