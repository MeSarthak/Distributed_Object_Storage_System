package database

import (
	"encoding/json"
	"fmt"
	"time"

	"distributed-storage/pkg/types"

	"github.com/google/uuid"
)

// LogRepository provides write methods for the system_logs table.
// The system_logs table is append-only from the application's perspective.
type LogRepository struct {
	db *DB
}

// NewLogRepository constructs a LogRepository from the shared DB handle.
func NewLogRepository(db *DB) *LogRepository {
	return &LogRepository{db: db}
}

// InsertSystemLog writes a structured audit event to the system_logs table.
// metadata is an optional map of key/value pairs that will be stored as JSONB.
// If metadata is nil, an empty JSON object is stored.
func (r *LogRepository) InsertSystemLog(
	eventType string,
	description string,
	severity types.LogSeverity,
	metadata map[string]string,
) error {
	metaJSON := "{}"
	if len(metadata) > 0 {
		b, err := json.Marshal(metadata)
		if err != nil {
			// Non-fatal: fall back to empty object rather than dropping the log entry.
			metaJSON = "{}"
		} else {
			metaJSON = string(b)
		}
	}

	query := `
		INSERT INTO system_logs (log_id, event_type, timestamp, description, severity, metadata)
		VALUES ($1, $2, NOW(), $3, $4, $5::jsonb)
	`
	_, err := r.db.Exec(query, uuid.New(), eventType, description, severity, metaJSON)
	if err != nil {
		return fmt.Errorf("insert system log [%s]: %w", eventType, err)
	}
	return nil
}

// GetSystemLogs returns paginated system logs with optional severity and event_type filters.
func (r *LogRepository) GetSystemLogs(severity types.LogSeverity, eventType string, limit, offset int) ([]types.SystemLog, error) {
	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}

	query := `
		SELECT log_id, event_type, timestamp, description, severity, metadata::text
		FROM system_logs
		WHERE ($1 = '' OR severity = $1)
		  AND ($2 = '' OR event_type = $2)
		ORDER BY timestamp DESC
		LIMIT $3 OFFSET $4
	`
	rows, err := r.db.Query(query, string(severity), eventType, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("get system logs: %w", err)
	}
	defer rows.Close()

	var logs []types.SystemLog
	for rows.Next() {
		var l types.SystemLog
		var metaStr string
		err := rows.Scan(&l.LogID, &l.EventType, &l.Timestamp, &l.Description, &l.Severity, &metaStr)
		if err != nil {
			return nil, fmt.Errorf("scan system log row: %w", err)
		}
		l.Metadata = metaStr
		logs = append(logs, l)
	}
	return logs, rows.Err()
}
// CountLogs returns the total number of system log entries matching the given filters.
// Pass empty strings to count all entries.
// Used by /api/logs to return total count for client-side pagination.
func (r *LogRepository) CountLogs(severity types.LogSeverity, eventType string) (int, error) {
	query := `
		SELECT COUNT(*)
		FROM system_logs
		WHERE ($1 = '' OR severity = $1)
		  AND ($2 = '' OR event_type = $2)
	`
	var total int
	err := r.db.QueryRow(query, string(severity), eventType).Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("count system logs: %w", err)
	}
	return total, nil
}

// GetRecentLogs returns the most recent N log entries of a given severity (or all if empty).
// Convenience helper for dashboard summaries.
func (r *LogRepository) GetRecentLogs(severity types.LogSeverity, n int) ([]types.SystemLog, error) {
	return r.GetSystemLogs(severity, "", n, 0)
}

// GetLogsBetween returns all system log entries recorded between start and end (inclusive).
func (r *LogRepository) GetLogsBetween(start, end time.Time, limit int) ([]types.SystemLog, error) {
	if limit <= 0 {
		limit = 100
	}
	query := `
		SELECT log_id, event_type, timestamp, description, severity, metadata::text
		FROM system_logs
		WHERE timestamp >= $1 AND timestamp <= $2
		ORDER BY timestamp DESC
		LIMIT $3
	`
	rows, err := r.db.Query(query, start, end, limit)
	if err != nil {
		return nil, fmt.Errorf("get logs between %v and %v: %w", start, end, err)
	}
	defer rows.Close()

	var logs []types.SystemLog
	for rows.Next() {
		var l types.SystemLog
		var metaStr string
		if err := rows.Scan(&l.LogID, &l.EventType, &l.Timestamp, &l.Description, &l.Severity, &metaStr); err != nil {
			return nil, fmt.Errorf("scan system log row: %w", err)
		}
		l.Metadata = metaStr
		logs = append(logs, l)
	}
	return logs, rows.Err()
}
