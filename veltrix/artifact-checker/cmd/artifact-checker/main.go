package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"veltrix/artifact-checker/internal/aggregator"
	"veltrix/artifact-checker/internal/consumer"
	"veltrix/artifact-checker/internal/models"
	"veltrix/artifact-checker/internal/replayengine"
	"veltrix/artifact-checker/internal/storage"
	"veltrix/artifact-checker/internal/watermark"
)

const (
	defaultOrderTopic   = "order_events"
	defaultMetricsTopic = "order_metrics"
	defaultGroup        = "artifact-checker"
	defaultChannelSize  = 65_536
)

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	config, err := loadConfig()
	if err != nil {
		logger.Fatalf("[main] config error: %v", err)
	}
	previousProcs := runtime.GOMAXPROCS(config.maxProcs)

	logger.Printf("╔══════════════════════════════════════════╗")
	logger.Printf("║   VELTRIX Artifact Checker  (Go)        ║")
	logger.Printf("╚══════════════════════════════════════════╝")
	logger.Printf("  Brokers      : %s", strings.Join(config.brokers, ","))
	logger.Printf("  Order topic  : %s", config.orderTopic)
	logger.Printf("  Metrics topic: %s", config.metricsTopic)
	logger.Printf("  Lateness     : %s", config.allowedLateness)
	logger.Printf("  GOMAXPROCS   : %d (was %d)", config.maxProcs, previousProcs)
	logger.Printf("  Health port  : %d", config.healthPort)

	healthSrv := startHealthServer(config.healthPort, logger)

	consumerService, err := consumer.New(consumer.Config{
		Brokers:       config.brokers,
		ConsumerGroup: config.consumerGroup,
		OrderTopic:    config.orderTopic,
		MetricsTopic:  config.metricsTopic,
		EventBuffer:   defaultChannelSize,
		MetricsBuffer: defaultChannelSize,
		Logger:        logger,
	})
	if err != nil {
		logger.Fatalf("[main] consumer init failed: %v", err)
	}

	publisher, err := storage.NewPublisher(ctx, storage.Config{
		PostgresURL: config.postgresURL,
		RedisAddr:   config.redisAddr,
		Logger:      logger,
	})
	if err != nil {
		logger.Fatalf("[main] storage init failed: %v", err)
	}
	defer publisher.Close()

	orderedEvents := make(chan models.OrderEvent, defaultChannelSize)
	correctnessUpdates := make(chan models.CorrectnessUpdate, 1_024)
	scores := make(chan models.Score, 1_024)

	router := watermark.NewRouter(int64(config.allowedLateness / time.Microsecond))
	router.Logger = logger

	replay := replayengine.New(logger)
	metricsAggregator := aggregator.New(10 * time.Second)
	metricsAggregator.Logger = logger

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 5)
	var wg sync.WaitGroup
	start := func(name string, fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(runCtx); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- fmt.Errorf("%s: %w", name, err)
			}
		}()
	}

	start("consumer", consumerService.Run)
	start("watermark-router", func(ctx context.Context) error {
		return router.Run(ctx, consumerService.Events(), orderedEvents)
	})
	start("replay-engine", func(ctx context.Context) error {
		return replay.Run(ctx, orderedEvents, correctnessUpdates)
	})
	start("aggregator", func(ctx context.Context) error {
		return metricsAggregator.Run(ctx, consumerService.Metrics(), correctnessUpdates, scores)
	})
	start("storage-publisher", func(ctx context.Context) error {
		return publisher.Run(ctx, scores)
	})

	exitCode := 0
	select {
	case <-ctx.Done():
		logger.Printf("[main] shutdown signal received")
	case err := <-errCh:
		logger.Printf("[main] service error: %v", err)
		exitCode = 1
	}

	cancel()
	if healthSrv != nil {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = healthSrv.Shutdown(shutdownCtx)
		shutdownCancel()
	}
	wg.Wait()

	if exitCode != 0 {
		os.Exit(exitCode)
	}
}

type appConfig struct {
	brokers         []string
	consumerGroup   string
	orderTopic      string
	metricsTopic    string
	allowedLateness time.Duration
	postgresURL     string
	redisAddr       string
	maxProcs        int
	healthPort      int
}

func loadConfig() (appConfig, error) {
	brokers := splitCSV(getenv("REDPANDA_BROKERS", "redpanda:9092"))
	if len(brokers) == 0 {
		return appConfig{}, fmt.Errorf("REDPANDA_BROKERS is required")
	}

	allowedLateness, err := parseDurationMS("ALLOWED_LATENESS_MS", watermark.DefaultAllowedLateness)
	if err != nil {
		return appConfig{}, err
	}
	maxProcs, err := parsePositiveInt("ARTIFACT_CHECKER_GOMAXPROCS", 1)
	if err != nil {
		return appConfig{}, err
	}
	healthPort, err := parsePositiveInt("HEALTH_PORT", 8092)
	if err != nil {
		return appConfig{}, err
	}

	return appConfig{
		brokers:         brokers,
		consumerGroup:   getenv("CONSUMER_GROUP", defaultGroup),
		orderTopic:      getenv("ORDER_EVENTS_TOPIC", defaultOrderTopic),
		metricsTopic:    getenv("METRICS_TOPIC", defaultMetricsTopic),
		allowedLateness: allowedLateness,
		postgresURL:     postgresURLFromEnv(),
		redisAddr:       redisAddrFromEnv(),
		maxProcs:        maxProcs,
		healthPort:      healthPort,
	}, nil
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}

func parseDurationMS(key string, fallback time.Duration) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}

	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer millisecond value: %w", key, err)
	}
	if value < 0 {
		return 0, fmt.Errorf("%s must be non-negative", key)
	}

	return time.Duration(value) * time.Millisecond, nil
}

func parsePositiveInt(key string, fallback int) (int, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}

	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	if value < 1 {
		return 0, fmt.Errorf("%s must be >= 1", key)
	}

	return value, nil
}

func postgresURLFromEnv() string {
	if raw := os.Getenv("POSTGRES_URL"); raw != "" {
		return raw
	}

	user := getenv("POSTGRES_USER", "iicpc")
	password := getenv("POSTGRES_PASSWORD", "iicpc_secret")
	host := getenv("POSTGRES_HOST", "postgres")
	port := getenv("POSTGRES_PORT", "5432")
	db := getenv("POSTGRES_DB", "iicpc_db")

	return "postgres://" +
		url.QueryEscape(user) + ":" +
		url.QueryEscape(password) + "@" +
		host + ":" + port + "/" +
		url.PathEscape(db) +
		"?sslmode=disable"
}

func redisAddrFromEnv() string {
	if raw := os.Getenv("REDIS_ADDR"); raw != "" {
		return raw
	}
	return getenv("REDIS_HOST", "redis") + ":" + getenv("REDIS_PORT", "6379")
}

func startHealthServer(port int, logger *log.Logger) *http.Server {
	if port <= 0 {
		return nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	go func() {
		logger.Printf("[health] listening on :%d", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Printf("[health] server error: %v", err)
		}
	}()

	return srv
}
