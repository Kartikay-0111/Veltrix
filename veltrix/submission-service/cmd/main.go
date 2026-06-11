// cmd/main.go — submission-service entry point.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"veltrix/submission-service/internal/config"
	"veltrix/submission-service/internal/db"
	"veltrix/submission-service/internal/handler"
	"veltrix/submission-service/internal/queue"
	"veltrix/submission-service/internal/storage"
)

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)

	cfg, err := config.Load()
	if err != nil {
		logger.Fatalf("[main] config error: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// ── PostgreSQL ────────────────────────────────────────────────────────────
	pool, err := db.Connect(ctx, cfg.DSN())
	if err != nil {
		logger.Fatalf("[main] postgres connect error: %v", err)
	}
	defer pool.Close()
	logger.Printf("[main] connected to postgres at %s:%d", cfg.PostgresHost, cfg.PostgresPort)

	// ── MinIO ─────────────────────────────────────────────────────────────────
	store, err := storage.New(ctx, cfg.MinIOEndpoint, cfg.MinIOAccessKey, cfg.MinIOSecretKey, cfg.MinIOBucket, cfg.MinIOUseSSL)
	if err != nil {
		logger.Fatalf("[main] minio connect error: %v", err)
	}
	logger.Printf("[main] connected to minio at %s (bucket=%s)", cfg.MinIOEndpoint, cfg.MinIOBucket)

	// ── Redis ─────────────────────────────────────────────────────────────────
	q, err := queue.New(ctx, cfg.RedisHost, cfg.RedisPort)
	if err != nil {
		logger.Fatalf("[main] redis connect error: %v", err)
	}
	defer q.Close()
	logger.Printf("[main] connected to redis at %s:%d", cfg.RedisHost, cfg.RedisPort)

	// ── HTTP server ───────────────────────────────────────────────────────────
	mux := http.NewServeMux()
	h := handler.New(pool, store, q, logger)
	h.RegisterRoutes(mux)

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      mux,
		ReadTimeout:  60 * time.Second, // allow for large zip uploads
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Start serving in the background.
	go func() {
		logger.Printf("[main] submission-service listening on :%s", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("[main] http server error: %v", err)
		}
	}()

	// Block until SIGINT/SIGTERM.
	<-ctx.Done()
	logger.Printf("[main] shutdown signal received, draining connections…")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Printf("[main] graceful shutdown error: %v", err)
	}
	logger.Printf("[main] shutdown complete")
}
