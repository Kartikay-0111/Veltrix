package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip" // registers gzip decompressor

	"veltrix/telemetry-ingester/internal/grpcserver"
	pb "veltrix/telemetry-ingester/internal/pb"
	"veltrix/telemetry-ingester/internal/producer"
)

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	grpcPort := getenv("GRPC_PORT", "8091")
	httpPort := getenv("HTTP_PORT", "8090")
	brokers := splitCSV(getenv("REDPANDA_BROKERS", "redpanda:9092"))
	orderTopic := getenv("ORDER_EVENTS_TOPIC", "order_events")
	metricsTopic := getenv("ORDER_METRICS_TOPIC", "order_metrics")

	logger.Printf("╔══════════════════════════════════════════╗")
	logger.Printf("║   VELTRIX Telemetry Ingester  (Go)      ║")
	logger.Printf("╚══════════════════════════════════════════╝")
	logger.Printf("  gRPC port     : %s", grpcPort)
	logger.Printf("  HTTP port     : %s", httpPort)
	logger.Printf("  Brokers       : %s", strings.Join(brokers, ","))
	logger.Printf("  Order topic   : %s", orderTopic)
	logger.Printf("  Metrics topic : %s", metricsTopic)

	// ── Create the Redpanda producer ─────────────────────────────────────────
	prod, err := producer.New(producer.Config{
		Brokers:      brokers,
		OrderTopic:   orderTopic,
		MetricsTopic: metricsTopic,
		Logger:       logger,
	})
	if err != nil {
		logger.Fatalf("[main] producer init failed: %v", err)
	}
	defer prod.Close()

	// ── Create the gRPC server ───────────────────────────────────────────────
	telemetryServer := grpcserver.New(prod, logger)

	grpcSrv := grpc.NewServer(
		grpc.MaxRecvMsgSize(64*1024*1024), // 64 MB max message (large audit batches)
	)
	pb.RegisterTelemetryServiceServer(grpcSrv, telemetryServer)

	grpcLis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		logger.Fatalf("[main] gRPC listen failed: %v", err)
	}

	// ── Create the HTTP health server ────────────────────────────────────────
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"status":"ok"}`)
	})
	mux.HandleFunc("/metrics/latest", func(w http.ResponseWriter, r *http.Request) {
		latest := telemetryServer.LatestMetrics()
		if latest == nil {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"detail":"No metrics received yet"}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(latest)
	})

	httpSrv := &http.Server{
		Addr:    ":" + httpPort,
		Handler: mux,
	}

	// ── Launch servers ───────────────────────────────────────────────────────
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Printf("[main] gRPC server listening on :%s", grpcPort)
		if err := grpcSrv.Serve(grpcLis); err != nil {
			logger.Printf("[main] gRPC server error: %v", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Printf("[main] HTTP server listening on :%s", httpPort)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Printf("[main] HTTP server error: %v", err)
		}
	}()

	// ── Graceful shutdown ────────────────────────────────────────────────────
	<-ctx.Done()
	logger.Printf("[main] shutdown signal received")

	grpcSrv.GracefulStop()
	httpSrv.Shutdown(context.Background())
	wg.Wait()

	logger.Printf("[main] shutdown complete")
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}
