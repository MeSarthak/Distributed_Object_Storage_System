package database

import (
	"encoding/json"
	"fmt"

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
