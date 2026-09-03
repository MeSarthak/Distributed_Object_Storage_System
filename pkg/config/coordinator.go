package config

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

// CoordinatorConfig holds strongly-typed configuration for the coordinator service
type CoordinatorConfig struct {
	Server      ServerConfig
	Database    DatabaseConfig
	Auth        AuthConfig
	Placement   PlacementConfig
	Replication ReplicationConfig
	Heartbeat   HeartbeatConfig
}

type ServerConfig struct {
	Port        int
	Host        string
	Environment string
	LogLevel    string
}

type DatabaseConfig struct {
	Host            string
	Port            int
	User            string
	Password        string
	Name            string
	SSLMode         string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

func (d DatabaseConfig) DSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		d.Host, d.Port, d.User, d.Password, d.Name, d.SSLMode)
}

type AuthConfig struct {
	JWTSecret       string
	ExpirationHours int
	BcryptCost      int
}

type PlacementConfig struct {
	WeightStorage       float64
	WeightCPU           float64
	WeightRAM           float64
	WeightLatency       float64
	WeightHealth        float64
	MaxCPUThreshold     float64
	MaxRAMThreshold     float64
	MinFreeStorageBytes int64
}

type ReplicationConfig struct {
	DefaultReplicationFactor int
	MinReplicationFactor     int
	MaxReplicationFactor     int
	HotAccessThreshold       int
	ColdAccessThreshold      int
}

type HeartbeatConfig struct {
	IntervalSeconds    time.Duration
	TimeoutSeconds     time.Duration
	SelfHealingSeconds time.Duration
}

// LoadCoordinatorConfig reads config from environment variables with defaults
func LoadCoordinatorConfig() (*CoordinatorConfig, error) {
	// Load .env if present (ignore error if not found)
	_ = godotenv.Load()

	cfg := &CoordinatorConfig{
		Server: ServerConfig{
			Port:        getEnvInt("COORDINATOR_PORT", 8080),
			Host:        getEnvStr("COORDINATOR_HOST", "0.0.0.0"),
			Environment: getEnvStr("ENVIRONMENT", "development"),
			LogLevel:    getEnvStr("LOG_LEVEL", "info"),
		},
		Database: DatabaseConfig{
			Host:            getEnvStr("DB_HOST", "localhost"),
			Port:            getEnvInt("DB_PORT", 5432),
			User:            getEnvStr("DB_USER", "storage_user"),
			Password:        getEnvStr("DB_PASSWORD", "storage_secret"),
			Name:            getEnvStr("DB_NAME", "storage_db"),
			SSLMode:         getEnvStr("DB_SSLMODE", "disable"),
			MaxOpenConns:    getEnvInt("DB_MAX_OPEN_CONNS", 25),
			MaxIdleConns:    getEnvInt("DB_MAX_IDLE_CONNS", 5),
			ConnMaxLifetime: time.Duration(getEnvInt("DB_CONN_MAX_LIFETIME_MINS", 5)) * time.Minute,
		},
		Auth: AuthConfig{
			JWTSecret:       getEnvStr("JWT_SECRET", "super-secret-jwt-key-change-in-production-min-32-chars"),
			ExpirationHours: getEnvInt("JWT_EXPIRATION_HOURS", 24),
			BcryptCost:      getEnvInt("BCRYPT_COST", 12),
		},
		Placement: PlacementConfig{
			WeightStorage:       getEnvFloat("WEIGHT_STORAGE", 0.35),
			WeightCPU:           getEnvFloat("WEIGHT_CPU", 0.20),
			WeightRAM:           getEnvFloat("WEIGHT_RAM", 0.15),
			WeightLatency:       getEnvFloat("WEIGHT_LATENCY", 0.15),
			WeightHealth:        getEnvFloat("WEIGHT_HEALTH", 0.15),
			MaxCPUThreshold:     getEnvFloat("MAX_CPU_THRESHOLD", 90.0),
			MaxRAMThreshold:     getEnvFloat("MAX_RAM_THRESHOLD", 90.0),
			MinFreeStorageBytes: getEnvInt64("MIN_FREE_STORAGE_BYTES", 104857600), // 100MB
		},
		Replication: ReplicationConfig{
			DefaultReplicationFactor: getEnvInt("DEFAULT_REPLICATION_FACTOR", 3),
			MinReplicationFactor:     getEnvInt("MIN_REPLICATION_FACTOR", 2),
			MaxReplicationFactor:     getEnvInt("MAX_REPLICATION_FACTOR", 5),
			HotAccessThreshold:       getEnvInt("HOT_ACCESS_THRESHOLD", 50),
			ColdAccessThreshold:      getEnvInt("COLD_ACCESS_THRESHOLD", 5),
		},
		Heartbeat: HeartbeatConfig{
			IntervalSeconds:    time.Duration(getEnvInt("HEARTBEAT_INTERVAL_SECONDS", 5)) * time.Second,
			TimeoutSeconds:     time.Duration(getEnvInt("HEARTBEAT_TIMEOUT_SECONDS", 15)) * time.Second,
			SelfHealingSeconds: time.Duration(getEnvInt("SELF_HEALING_INTERVAL_SECONDS", 10)) * time.Second,
		},
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// Validate ensures all configuration constraints are met
func (c *CoordinatorConfig) Validate() error {
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("invalid server port: %d", c.Server.Port)
	}

	if c.Database.Host == "" || c.Database.Name == "" || c.Database.User == "" {
		return fmt.Errorf("database host, name, and user must not be empty")
	}

	if len(c.Auth.JWTSecret) < 16 {
		return fmt.Errorf("JWT secret must be at least 16 characters long")
	}

	weightSum := c.Placement.WeightStorage + c.Placement.WeightCPU + c.Placement.WeightRAM +
		c.Placement.WeightLatency + c.Placement.WeightHealth
	if math.Abs(weightSum-1.0) > 0.01 {
		return fmt.Errorf("placement weights must sum to 1.0 (got %.2f)", weightSum)
	}

	if c.Replication.MinReplicationFactor > c.Replication.DefaultReplicationFactor {
		return fmt.Errorf("min replication factor cannot exceed default replication factor")
	}

	if c.Replication.DefaultReplicationFactor > c.Replication.MaxReplicationFactor {
		return fmt.Errorf("default replication factor cannot exceed max replication factor")
	}

	if c.Heartbeat.TimeoutSeconds <= c.Heartbeat.IntervalSeconds {
		return fmt.Errorf("heartbeat timeout (%v) must be greater than heartbeat interval (%v)",
			c.Heartbeat.TimeoutSeconds, c.Heartbeat.IntervalSeconds)
	}

	return nil
}

// Helper functions for reading typed environment variables
func getEnvStr(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return fallback
}

func getEnvInt64(key string, fallback int64) int64 {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.ParseInt(val, 10, 64); err == nil {
			return i
		}
	}
	return fallback
}

func getEnvFloat(key string, fallback float64) float64 {
	if val := os.Getenv(key); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
	}
	return fallback
}
