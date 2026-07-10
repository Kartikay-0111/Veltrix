// internal/config/config.go — environment-driven configuration for the sandbox manager.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config holds all runtime configuration parsed from environment variables.
type Config struct {
	// Server
	HealthPort int

	// Worker pool
	WorkerCount int

	// PostgreSQL
	PostgresHost     string
	PostgresPort     int
	PostgresUser     string
	PostgresPassword string
	PostgresDB       string

	// Redis
	RedisHost string
	RedisPort int

	// MinIO
	MinIOEndpoint  string
	MinIOAccessKey string
	MinIOSecretKey string
	MinIOBucket    string

	// Docker
	SandboxNetwork string

	// Benchmark defaults
	DefaultNumBots      int
	DefaultDurationSecs int

	// FleetPoolURLs is a list of bot-fleet base URLs (e.g. ["http://bot-fleet-1:7070"]).
	// Each URL is a separate bot-fleet instance on its own machine with exclusive
	// CPU cores. The fleet pool dispatches each benchmark to the least-loaded
	// instance, enabling true parallel benchmarking without core contention.
	// Populated from FLEET_POOL_URLS (comma-separated).
	FleetPoolURLs []string

	// Correctness phase (serialized golden-model differential replay). Runs
	// before the performance phase with a single writer and a fixed seed.
	CorrectnessSeed         int
	CorrectnessDurationSecs int

	// Archive safety limits
	MaxExtractSizeMB int
	MaxFileSizeMB    int
	MaxFileCount     int

	// Startup probe
	StartupTimeoutSecs int
}

// DSN returns the PostgreSQL libpq connection string.
func (c *Config) DSN() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s",
		c.PostgresUser, c.PostgresPassword,
		c.PostgresHost, c.PostgresPort, c.PostgresDB,
	)
}

// Load reads all configuration from environment variables with defaults.
func Load() *Config {
	return &Config{
		HealthPort:              envInt("HEALTH_PORT", 8081),
		WorkerCount:             envInt("CONFIG_WORKER_COUNT", 8),
		PostgresHost:            requireenv("POSTGRES_HOST"),
		PostgresPort:            envInt("POSTGRES_PORT", 5432),
		PostgresUser:            requireenv("POSTGRES_USER"),
		PostgresPassword:        requireenv("POSTGRES_PASSWORD"),
		PostgresDB:              requireenv("POSTGRES_DB"),
		RedisHost:               getenv("REDIS_HOST", "redis"),
		RedisPort:               envInt("REDIS_PORT", 6379),
		MinIOEndpoint:           fmt.Sprintf("%s:%s", requireenv("MINIO_HOST"), getenv("MINIO_PORT", "9000")),
		MinIOAccessKey:          requireenv("MINIO_ROOT_USER"),
		MinIOSecretKey:          requireenv("MINIO_ROOT_PASSWORD"),
		MinIOBucket:             getenv("MINIO_BUCKET", "submissions"),
		SandboxNetwork:          getenv("SANDBOX_NETWORK", "sandbox-net"),
		DefaultNumBots:          envInt("DEFAULT_NUM_BOTS", 100),
		DefaultDurationSecs:     envInt("DEFAULT_DURATION_SECS", 60),
		FleetPoolURLs:           envStringSlice("FLEET_POOL_URLS", []string{"http://bot-fleet:7070"}),
		CorrectnessSeed:         envInt("CORRECTNESS_SEED", 42),
		CorrectnessDurationSecs: envInt("CORRECTNESS_DURATION_SECS", 20),
		MaxExtractSizeMB:        envInt("MAX_EXTRACT_SIZE_MB", 200),
		MaxFileSizeMB:           envInt("MAX_FILE_SIZE_MB", 50),
		MaxFileCount:            envInt("MAX_FILE_COUNT", 500),
		StartupTimeoutSecs:      envInt("STARTUP_TIMEOUT_SECS", 15),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envStringSlice(key string, fallback []string) []string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	parts := strings.Split(v, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	if len(result) == 0 {
		return fallback
	}
	return result
}

func requireenv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic(fmt.Sprintf("required environment variable %q is not set", key))
	}
	return v
}

func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}
