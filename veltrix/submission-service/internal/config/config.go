// internal/config/config.go — environment-driven configuration for the submission service.
package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config holds all runtime configuration parsed from environment variables.
type Config struct {
	// HTTP
	Port string

	// PostgreSQL
	PostgresHost     string
	PostgresPort     int
	PostgresUser     string
	PostgresPassword string
	PostgresDB       string

	// Redis
	RedisHost string
	RedisPort int

	// MinIO (S3-compatible)
	MinIOEndpoint  string // host:port
	MinIOAccessKey string
	MinIOSecretKey string
	MinIOBucket    string
	MinIOUseSSL    bool
}

// DSN returns the PostgreSQL connection string.
func (c *Config) DSN() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s",
		c.PostgresUser, c.PostgresPassword,
		c.PostgresHost, c.PostgresPort, c.PostgresDB,
	)
}

// Load reads all configuration from environment variables, applying defaults.
func Load() (*Config, error) {
	pgPort, _ := strconv.Atoi(getenv("POSTGRES_PORT", "5432"))
	redisPort, _ := strconv.Atoi(getenv("REDIS_PORT", "6379"))
	minioPort := getenv("MINIO_PORT", "9000")

	cfg := &Config{
		Port:             getenv("PORT", "8080"),
		PostgresHost:     requireenv("POSTGRES_HOST"),
		PostgresPort:     pgPort,
		PostgresUser:     requireenv("POSTGRES_USER"),
		PostgresPassword: requireenv("POSTGRES_PASSWORD"),
		PostgresDB:       requireenv("POSTGRES_DB"),
		RedisHost:        getenv("REDIS_HOST", "redis"),
		RedisPort:        redisPort,
		MinIOEndpoint:    fmt.Sprintf("%s:%s", requireenv("MINIO_HOST"), minioPort),
		MinIOAccessKey:   requireenv("MINIO_ROOT_USER"),
		MinIOSecretKey:   requireenv("MINIO_ROOT_PASSWORD"),
		MinIOBucket:      getenv("MINIO_BUCKET", "submissions"),
		MinIOUseSSL:      false,
	}
	return cfg, nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func requireenv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic(fmt.Sprintf("required environment variable %q is not set", key))
	}
	return v
}
