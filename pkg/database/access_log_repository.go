package database

import (
	"fmt"
	"time"

	"distributed-storage/pkg/types"

	"github.com/google/uuid"
)

// AccessLogRepository provides typed database query methods for the access_logs table.
type AccessLogRepository struct {
	db *DB
}

// NewAccessLogRepository constructs an AccessLogRepository.
func NewAccessLogRepository(db *DB) *AccessLogRepository {
	return &AccessLogRepository{db: db}
}

// RecordAccess inserts a new access log record.
func (r *AccessLogRepository) RecordAccess(objectID *uuid.UUID, userID *uuid.UUID, responseTimeMs int) error {
	query := `
		INSERT INTO access_logs (access_id, object_id, user_id, access_time, response_time)
		VALUES ($1, $2, $3, NOW(), $4)
	`
	accessID := uuid.New()
	_, err := r.db.Exec(query, accessID, objectID, userID, responseTimeMs)
	if err != nil {
		return fmt.Errorf("record access log: %w", err)
	}
	return nil
}

// GetAccessCountSince returns the total number of accesses for a specific object since a given timestamp.
func (r *AccessLogRepository) GetAccessCountSince(objectID uuid.UUID, since time.Time) (int64, error) {
	query := `
		SELECT COUNT(*)
		FROM access_logs
		WHERE object_id = $1 AND access_time >= $2
	`
	var count int64
	err := r.db.QueryRow(query, objectID, since).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("get access count for object %s: %w", objectID, err)
	}
	return count, nil
}

// GetAccessCountsByObjectSince aggregates access counts for all objects accessed since the given timestamp.
func (r *AccessLogRepository) GetAccessCountsByObjectSince(since time.Time) (map[uuid.UUID]int64, error) {
	query := `
		SELECT object_id, COUNT(*)
		FROM access_logs
		WHERE object_id IS NOT NULL AND access_time >= $1
		GROUP BY object_id
	`
	rows, err := r.db.Query(query, since)
	if err != nil {
		return nil, fmt.Errorf("get access counts since %v: %w", since, err)
	}
	defer rows.Close()

	counts := make(map[uuid.UUID]int64)
	for rows.Next() {
		var objectID uuid.UUID
		var count int64
		if err := rows.Scan(&objectID, &count); err != nil {
			return nil, fmt.Errorf("scan access count row: %w", err)
		}
		counts[objectID] = count
	}
	return counts, rows.Err()
}

// GetRecentLogs retrieves up to limit recent access logs ordered by access_time descending.
func (r *AccessLogRepository) GetRecentLogs(limit int) ([]types.AccessLog, error) {
	if limit <= 0 {
		limit = 100
	}
	query := `
		SELECT access_id, object_id, user_id, access_time, response_time
		FROM access_logs
		ORDER BY access_time DESC
		LIMIT $1
	`
	rows, err := r.db.Query(query, limit)
	if err != nil {
		return nil, fmt.Errorf("get recent access logs: %w", err)
	}
	defer rows.Close()

	var logs []types.AccessLog
	for rows.Next() {
		var l types.AccessLog
		var objID, uID *uuid.UUID
		var accessTime time.Time
		if err := rows.Scan(&l.AccessID, &objID, &uID, &accessTime, &l.ResponseTime); err != nil {
			return nil, fmt.Errorf("scan access log: %w", err)
		}
		l.ObjectID = objID
		l.UserID = uID
		l.AccessTime = accessTime
		logs = append(logs, l)
	}
	return logs, rows.Err()
}
