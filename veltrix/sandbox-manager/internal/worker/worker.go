// internal/worker/worker.go — Bounded Worker Pool for concurrent sandbox processing.
//
// Architecture:
//
//	┌──────────────┐      jobs chan      ┌──────────┐
//	│  Dispatcher  │ ─────────────────► │ Worker 1 │
//	│  (BLPOP)     │                    ├──────────┤
//	└──────────────┘                    │ Worker 2 │
//	                                    ├──────────┤
//	                                    │    ...   │
//	                                    └──────────┘
//
// The Dispatcher goroutine owns all Redis BLPOP calls and pushes submission IDs
// into a buffered channel. Each worker goroutine reads from that channel and
// processes the job end-to-end. The channel capacity = workerCount, so the pool
// has natural back-pressure: if all workers are busy, the dispatcher blocks
// until a slot opens — preventing unbounded goroutine growth.
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"veltrix/sandbox-manager/internal/archive"
	"veltrix/sandbox-manager/internal/config"
	"veltrix/sandbox-manager/internal/db"
	dockerpkg "veltrix/sandbox-manager/internal/docker"
	"veltrix/sandbox-manager/internal/storage"

	"github.com/redis/go-redis/v9"
)

const (
	submissionQueue    = "submission_queue"
	botFleetTrigger    = "bot_fleet_triggers" // Redis Pub/Sub channel
)

// Pool is the bounded worker pool.
type Pool struct {
	cfg     *config.Config
	db      *db.Pool
	storage *storage.Client
	docker  *dockerpkg.Client
	redis   *redis.Client
	jobs    chan string // buffered by workerCount — this is the back-pressure valve
	logger  *log.Logger
}

// New creates the pool. Call Run() to start dispatching.
func New(
	cfg *config.Config,
	db *db.Pool,
	store *storage.Client,
	dockerCli *dockerpkg.Client,
	rdb *redis.Client,
	logger *log.Logger,
) *Pool {
	return &Pool{
		cfg:     cfg,
		db:      db,
		storage: store,
		docker:  dockerCli,
		redis:   rdb,
		jobs:    make(chan string, cfg.WorkerCount),
		logger:  logger,
	}
}

// Run starts the dispatcher and all workers. It blocks until ctx is cancelled.
func (p *Pool) Run(ctx context.Context) {
	p.logger.Printf("[pool] starting %d workers", p.cfg.WorkerCount)

	// Launch the worker goroutines.
	for i := 0; i < p.cfg.WorkerCount; i++ {
		go p.worker(ctx, i)
	}

	// The dispatcher loop: BLPOP → push to jobs channel.
	// This is the only goroutine that touches Redis blocking calls.
	p.dispatch(ctx)
}

// dispatch blocks in a BLPOP loop, pushing submission IDs into the jobs channel.
func (p *Pool) dispatch(ctx context.Context) {
	p.logger.Printf("[dispatcher] waiting for jobs on queue %q", submissionQueue)
	for {
		select {
		case <-ctx.Done():
			p.logger.Printf("[dispatcher] context cancelled, shutting down")
			close(p.jobs)
			return
		default:
		}

		// BLPOP with a 5-second timeout so we can check ctx.Done() periodically.
		result, err := p.redis.BLPop(ctx, 5*time.Second, submissionQueue).Result()
		if err != nil {
			if err == redis.Nil || ctx.Err() != nil {
				continue
			}
			p.logger.Printf("[dispatcher] blpop error: %v", err)
			time.Sleep(time.Second) // brief back-off on transient Redis errors
			continue
		}

		submissionID := result[1] // result[0] = queue name, result[1] = value
		p.logger.Printf("[dispatcher] dequeued job: %s", submissionID)

		// Push into the jobs channel. This blocks if all workers are busy,
		// providing natural back-pressure without spinning.
		select {
		case p.jobs <- submissionID:
		case <-ctx.Done():
			return
		}
	}
}

// worker is the goroutine that reads from the jobs channel and processes each job.
func (p *Pool) worker(ctx context.Context, id int) {
	p.logger.Printf("[worker %d] started", id)
	for {
		submissionID, ok := <-p.jobs
		if !ok {
			p.logger.Printf("[worker %d] jobs channel closed, exiting", id)
			return
		}
		p.logger.Printf("[worker %d] processing %s", id, submissionID)
		p.process(ctx, submissionID)
	}
}

// process runs the full sandbox lifecycle for a single submission.
func (p *Pool) process(ctx context.Context, submissionID string) {
	// ── 1. Fetch submission record ────────────────────────────────────────────
	sub, err := p.db.GetSubmission(ctx, submissionID)
	if err != nil || sub == nil {
		p.logger.Printf("[process:%s] submission not found: %v", submissionID, err)
		return
	}

	p.logger.Printf("[process:%s] language=%s key=%s", submissionID, sub.Language, sub.StorageKey)

	if err := p.db.UpdateStatus(ctx, submissionID, "BUILDING", nil); err != nil {
		p.logger.Printf("[process:%s] status update BUILDING failed: %v", submissionID, err)
	}

	// ── 2. Download archive and build Docker image ────────────────────────────
	imageTag := "sandbox-" + submissionID
	buildDir, err := p.buildImage(ctx, sub, imageTag)
	defer os.RemoveAll(buildDir) // always clean up the temp dir

	if err != nil {
		p.logger.Printf("[process:%s] build failed: %v", submissionID, err)
		_ = p.db.UpdateStatus(ctx, submissionID, "FAILED_SYSTEM", map[string]any{
			"error_message": fmt.Sprintf("Build error: %v", err),
		})
		return
	}

	// ── 3. Start the sandbox container ───────────────────────────────────────
	result, err := p.docker.RunSandbox(ctx, imageTag, submissionID, p.cfg.SandboxNetwork)
	if err != nil {
		p.logger.Printf("[process:%s] run sandbox failed: %v", submissionID, err)
		failStatus, exitCode := classifyContainerError(err.Error())
		_ = p.db.UpdateStatus(ctx, submissionID, failStatus, map[string]any{
			"error_message": err.Error(),
			"exit_code":     exitCode,
		})
		p.docker.RemoveContainer(ctx, "sandbox-"+submissionID, imageTag)
		return
	}

	// ── 4. Mark READY and trigger the bot fleet via Redis Pub/Sub ────────────
	_ = p.db.UpdateStatus(ctx, submissionID, "READY", map[string]any{
		"container_id": result.ContainerID,
		"endpoint_url": result.EndpointURL,
	})
	p.logger.Printf("[process:%s] sandbox READY → %s", submissionID, result.EndpointURL)

	if err := p.triggerBotFleet(ctx, submissionID, result.TargetHost); err != nil {
		p.logger.Printf("[process:%s] bot fleet trigger failed: %v", submissionID, err)
	}

	_ = p.db.UpdateStatus(ctx, submissionID, "RUNNING", nil)

	// ── 5. Schedule cleanup after the benchmark window ────────────────────────
	cleanupDelay := time.Duration(p.cfg.DefaultDurationSecs+300) * time.Second
	go func() {
		p.logger.Printf("[process:%s] cleanup scheduled in %s", submissionID, cleanupDelay)
		time.Sleep(cleanupDelay)
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Only mark SUCCESS if still RUNNING (prevents overwriting a manual status).
		sub, err := p.db.GetSubmission(cleanupCtx, submissionID)
		if err == nil && sub != nil && sub.Status == "RUNNING" {
			_ = p.db.UpdateStatus(cleanupCtx, submissionID, "SUCCESS", nil)
		}

		p.docker.RemoveContainer(cleanupCtx, result.ContainerID, imageTag)
		p.logger.Printf("[process:%s] cleanup complete", submissionID)
	}()
}

// buildImage downloads the archive from MinIO, extracts it safely, and builds
// the Docker image. Returns the build context directory (caller must remove it).
func (p *Pool) buildImage(ctx context.Context, sub *db.Submission, imageTag string) (string, error) {
	tmpDir, err := os.MkdirTemp("", "veltrix-build-*")
	if err != nil {
		return "", fmt.Errorf("mktemp: %w", err)
	}

	// Download the archive.
	archivePath := filepath.Join(tmpDir, "submission.archive")
	reader, err := p.storage.DownloadObject(ctx, sub.StorageKey)
	if err != nil {
		return tmpDir, fmt.Errorf("download archive: %w", err)
	}
	defer reader.Close()

	out, err := os.Create(archivePath)
	if err != nil {
		return tmpDir, fmt.Errorf("create archive file: %w", err)
	}
	if _, err := out.ReadFrom(reader); err != nil {
		out.Close()
		return tmpDir, fmt.Errorf("write archive: %w", err)
	}
	out.Close()

	// Extract into src/ subdirectory.
	srcDir := filepath.Join(tmpDir, "src")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		return tmpDir, fmt.Errorf("mkdir src: %w", err)
	}

	limits := archive.Limits{
		MaxTotalBytes:    int64(p.cfg.MaxExtractSizeMB) * 1024 * 1024,
		MaxFileSizeBytes: int64(p.cfg.MaxFileSizeMB) * 1024 * 1024,
		MaxFileCount:     p.cfg.MaxFileCount,
	}
	if err := archive.Extract(archivePath, srcDir, limits); err != nil {
		return tmpDir, fmt.Errorf("extract archive: %w", err)
	}

	// Build the image (Dockerfile is written by BuildImage).
	if err := p.docker.BuildImage(ctx, tmpDir, imageTag, sub.Language); err != nil {
		return tmpDir, fmt.Errorf("docker build: %w", err)
	}

	return tmpDir, nil
}

// triggerBotFleet publishes a benchmark trigger message to the Redis Pub/Sub
// channel and sends an HTTP POST request to the C++ FleetCommander service
// listening on http://bot-fleet:7070/benchmark.
func (p *Pool) triggerBotFleet(ctx context.Context, submissionID, targetHost string) error {
	payload := map[string]any{
		"submission_id":  submissionID,
		"target_host":    targetHost,
		"target_port":    fmt.Sprintf("%d", dockerpkg.SandboxPort),
		"num_bots":       p.cfg.DefaultNumBots,
		"duration_secs":  p.cfg.DefaultDurationSecs,
		"protocol":       "rest",
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal trigger payload: %w", err)
	}

	// 1. Publish to Redis Pub/Sub (for decoupled architectures/subscribers)
	if err := p.redis.Publish(ctx, botFleetTrigger, string(data)).Err(); err != nil {
		p.logger.Printf("[trigger] Redis publish failed (continuing to HTTP trigger): %v", err)
	} else {
		p.logger.Printf("[trigger] published benchmark for %s to channel %q", submissionID, botFleetTrigger)
	}

	// 2. Call the C++ FleetCommander via HTTP POST (direct trigger)
	client := &http.Client{Timeout: 10 * time.Second}
	reqURL := "http://bot-fleet:7070/benchmark"
	
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create HTTP request to bot-fleet: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("send HTTP POST to bot-fleet: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("bot-fleet returned status %d: %s", resp.StatusCode, string(body))
	}

	p.logger.Printf("[trigger] HTTP request accepted by bot-fleet for submission %s", submissionID)
	return nil
}

// classifyContainerError maps container failure strings to our status codes.
func classifyContainerError(msg string) (string, int) {
	switch {
	case contains(msg, "137") || contains(msg, "OOM"):
		return "FAILED_RESOURCE", 137
	case contains(msg, "139"):
		return "FAILED_LOGIC", 139
	case contains(msg, "did not bind"):
		return "FAILED_STARTUP", 1
	default:
		return "FAILED_SYSTEM", 1
	}
}

func contains(s, sub string) bool {
	return len(sub) > 0 && len(s) >= len(sub) &&
		(s == sub || len(s) > 0 && stringContains(s, sub))
}

func stringContains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
