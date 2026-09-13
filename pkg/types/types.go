package types

import (
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// User Roles
type UserRole string

const (
	RoleUser  UserRole = "USER"
	RoleAdmin UserRole = "ADMIN"
)

// Node Statuses
type NodeStatus string

const (
	NodeStatusOnline   NodeStatus = "ONLINE"
	NodeStatusOffline  NodeStatus = "OFFLINE"
	NodeStatusDegraded NodeStatus = "DEGRADED"
)

// Replica Statuses
type ReplicaStatus string

const (
	ReplicaStatusHealthy    ReplicaStatus = "HEALTHY"
	ReplicaStatusRecovering ReplicaStatus = "RECOVERING"
	ReplicaStatusLost       ReplicaStatus = "LOST"
)

// System Log Severity
type LogSeverity string

const (
	SeverityDebug    LogSeverity = "DEBUG"
	SeverityInfo     LogSeverity = "INFO"
	SeverityWarn     LogSeverity = "WARN"
	SeverityError    LogSeverity = "ERROR"
	SeverityCritical LogSeverity = "CRITICAL"
)

// Standard Error Codes
const (
	ErrCodeAuthUnauthorized = "AUTH_401"
	ErrCodeAuthForbidden    = "AUTH_403"
	ErrCodeObjectNotFound   = "OBJ_404"
	ErrCodeNodeUnavailable  = "NODE_503"
	ErrCodeMetadataFailure  = "META_500"
	ErrCodeReplicaFailure   = "REPL_500"
	ErrCodeValidationFailed = "VAL_400"
)

// User represents an authenticated account
type User struct {
	UserID       uuid.UUID `json:"user_id" db:"user_id"`
	Name         string    `json:"name" db:"name"`
	Email        string    `json:"email" db:"email"`
	PasswordHash string    `json:"-" db:"password_hash"`
	Role         UserRole  `json:"role" db:"role"`
	CreatedAt    time.Time `json:"created_at" db:"created_at"`
}

// StorageNode represents an independent physical or virtual storage node
type StorageNode struct {
	NodeID        uuid.UUID  `json:"node_id" db:"node_id"`
	Hostname      string     `json:"hostname" db:"hostname"`
	IPAddress     string     `json:"ip_address" db:"ip_address"`
	TotalStorage  int64      `json:"total_storage" db:"total_storage"`
	UsedStorage   int64      `json:"used_storage" db:"used_storage"`
	CPUUsage      float64    `json:"cpu_usage" db:"cpu_usage"`
	MemoryUsage   float64    `json:"memory_usage" db:"memory_usage"`
	Latency       float64    `json:"latency" db:"latency"`
	Status        NodeStatus `json:"status" db:"status"`
	LastHeartbeat time.Time  `json:"last_heartbeat" db:"last_heartbeat"`
}

// Object represents a stored object's metadata
type Object struct {
	ObjectID          uuid.UUID `json:"object_id" db:"object_id"`
	OwnerID           uuid.UUID `json:"owner_id" db:"owner_id"`
	ObjectName        string    `json:"object_name" db:"object_name"`
	FileSize          int64     `json:"file_size" db:"file_size"`
	MimeType          string    `json:"mime_type" db:"mime_type"`
	Checksum          string    `json:"checksum" db:"checksum"`
	UploadTime        time.Time `json:"upload_time" db:"upload_time"`
	LastAccessed      time.Time `json:"last_accessed" db:"last_accessed"`
	ReplicationFactor int       `json:"replication_factor" db:"replication_factor"`
}

// Replica represents an object replica on a specific storage node
type Replica struct {
	ReplicaID uuid.UUID     `json:"replica_id" db:"replica_id"`
	ObjectID  uuid.UUID     `json:"object_id" db:"object_id"`
	NodeID    uuid.UUID     `json:"node_id" db:"node_id"`
	Status    ReplicaStatus `json:"status" db:"status"`
	CreatedAt time.Time     `json:"created_at" db:"created_at"`
}

// AccessLog tracks object access events
type AccessLog struct {
	AccessID     uuid.UUID  `json:"access_id" db:"access_id"`
	ObjectID     *uuid.UUID `json:"object_id,omitempty" db:"object_id"`
	UserID       *uuid.UUID `json:"user_id,omitempty" db:"user_id"`
	AccessTime   time.Time  `json:"access_time" db:"access_time"`
	ResponseTime int        `json:"response_time" db:"response_time"`
}

// SystemLog records operational cluster audit events
type SystemLog struct {
	LogID       uuid.UUID   `json:"log_id" db:"log_id"`
	EventType   string      `json:"event_type" db:"event_type"`
	Timestamp   time.Time   `json:"timestamp" db:"timestamp"`
	Description string      `json:"description" db:"description"`
	Severity    LogSeverity `json:"severity" db:"severity"`
	Metadata    string      `json:"metadata" db:"metadata"`
}

// HeartbeatPayload represents periodic telemetry sent by a storage node
type HeartbeatPayload struct {
	NodeID       uuid.UUID `json:"node_id"`
	Hostname     string    `json:"hostname"`
	IPAddress    string    `json:"ip_address"`
	Port         int       `json:"port"`
	TotalStorage int64     `json:"total_storage"`
	UsedStorage  int64     `json:"used_storage"`
	CPUUsage     float64   `json:"cpu_usage"`
	MemoryUsage  float64   `json:"memory_usage"`
	LatencyMs    float64   `json:"latency_ms"`
	Timestamp    time.Time `json:"timestamp"`
}

// StandardResponse wraps all API responses uniformly
type StandardResponse struct {
	Success   bool        `json:"success"`
	Message   string      `json:"message"`
	Data      interface{} `json:"data,omitempty"`
	ErrorCode string      `json:"error_code,omitempty"`
}

// ObjectMetadataWithReplicas includes replicas for detailed metadata queries
type ObjectMetadataWithReplicas struct {
	Object
	Replicas []ReplicaLocation `json:"replicas"`
	Tier     string            `json:"tier"` // HOT, WARM, COLD
}

// ReplicaLocation provides storage node address information for a replica
type ReplicaLocation struct {
	ReplicaID    uuid.UUID     `json:"replica_id"`
	NodeID       uuid.UUID     `json:"node_id"`
	Hostname     string        `json:"hostname"`
	IPAddress    string        `json:"ip_address"`
	Status       ReplicaStatus `json:"status"`
	NodeStatus   NodeStatus    `json:"node_status"`
	InternalURL  string        `json:"internal_url"`
}

// RegisterRequest represents the payload required to register a new user
type RegisterRequest struct {
	Name     string   `json:"name"`
	Email    string   `json:"email"`
	Password string   `json:"password"`
	Role     UserRole `json:"role,omitempty"`
}

// LoginRequest represents the payload for user authentication
type LoginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

// AuthResponse returns the JWT bearer token and user profile on successful authentication
type AuthResponse struct {
	Token string `json:"token"`
	User  User   `json:"user"`
}

// JWTClaims holds payload information encoded inside the JWT
type JWTClaims struct {
	UserID uuid.UUID `json:"user_id"`
	Email  string    `json:"email"`
	Role   UserRole  `json:"role"`
	jwt.RegisteredClaims
}

// VerifyResult represents object integrity verification telemetry
type VerifyResult struct {
	ObjectID         string `json:"object_id"`
	Checksum         string `json:"checksum"`
	FileSize         int64  `json:"file_size"`
	Valid            bool   `json:"valid"`
	ExpectedChecksum string `json:"expected_checksum,omitempty"`
}

