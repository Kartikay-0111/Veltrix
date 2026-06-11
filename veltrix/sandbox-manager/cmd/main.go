// cmd/main.go — sandbox-manager entry point.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"veltrix/sandbox-manager/internal/config"
	"veltrix/sandbox-manager/internal/db"
	dockerpkg "veltrix/sandbox-manager/internal/docker"
	"veltrix/sandbox-manager/internal/storage"
	"veltrix/sandbox-manager/internal/worker"

	"github.com/redis/go-redis/v9"
)

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)

	cfg := config.Load()

	logger.Printf("╔══════════════════════════════════════════╗")
	logger.Printf("║   VELTRIX Sandbox Manager  (Go)         ║")
	logger.Printf("╚══════════════════════════════════════════╝")
	logger.Printf("  Workers       : %d", cfg.WorkerCount)
	logger.Printf("  Sandbox net   : %s", cfg.SandboxNetwork)
	logger.Printf("  Health port   : %d", cfg.HealthPort)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ── PostgreSQL ────────────────────────────────────────────────────────────
	pool, err := db.Connect(ctx, cfg.DSN())
	if err != nil {
		logger.Fatalf("[main] postgres: %v", err)
	}
	defer pool.Close()
	logger.Printf("[main] connected to postgres")

	// ── MinIO ─────────────────────────────────────────────────────────────────
	store, err := storage.New(cfg.MinIOEndpoint, cfg.MinIOAccessKey, cfg.MinIOSecretKey, cfg.MinIOBucket, false)
	if err != nil {
		logger.Fatalf("[main] minio: %v", err)
	}
	logger.Printf("[main] connected to minio at %s", cfg.MinIOEndpoint)

	// ── Redis ─────────────────────────────────────────────────────────────────
	rdb := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", cfg.RedisHost, cfg.RedisPort),
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		logger.Fatalf("[main] redis ping: %v", err)
	}
	defer rdb.Close()
	logger.Printf("[main] connected to redis at %s:%d", cfg.RedisHost, cfg.RedisPort)

	// ── Docker SDK ────────────────────────────────────────────────────────────
	dockerCli, err := dockerpkg.New(logger)
	if err != nil {
		logger.Fatalf("[main] docker: %v", err)
	}
	logger.Printf("[main] docker client ready")

	// ── Health server ─────────────────────────────────────────────────────────
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	healthSrv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.HealthPort),
		Handler: healthMux,
	}
	go func() {
		logger.Printf("[main] health server on :%d", cfg.HealthPort)
		if err := healthSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Printf("[main] health server error: %v", err)
		}
	}()

	// ── Bounded Worker Pool ───────────────────────────────────────────────────
	pool2 := worker.New(cfg, pool, store, dockerCli, rdb, logger)

	// Run blocks until ctx is cancelled (SIGTERM/SIGINT).
	pool2.Run(ctx)

	// Graceful HTTP shutdown.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = healthSrv.Shutdown(shutdownCtx)
	logger.Printf("[main] shutdown complete")
}
