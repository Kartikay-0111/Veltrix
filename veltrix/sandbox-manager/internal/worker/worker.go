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
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"veltrix/sandbox-manager/internal/archive"
	"veltrix/sandbox-manager/internal/config"
	"veltrix/sandbox-manager/internal/db"
	dockerpkg "veltrix/sandbox-manager/internal/docker"
	"veltrix/sandbox-manager/internal/fleet"
	"veltrix/sandbox-manager/internal/storage"

	"github.com/redis/go-redis/v9"
)

const (
	submissionQueue = "submission_queue"
	botFleetTrigger = "bot_fleet_triggers" // Redis Pub/Sub channel

	// benchmarkPhaseGapSecs is the pause between the correctness phase ending and
	// the performance phase starting, leaving room for the run to stop, flush the
	// end-of-run marker, and let the checker finalize the verdict.
	benchmarkPhaseGapSecs = 15
)

// Pool is the bounded worker pool.
type Pool struct {
	cfg     *config.Config
	db      *db.Pool
	storage *storage.Client
	docker  *dockerpkg.Client
	redis   *redis.Client
	jobs    chan string // buffered by workerCount — this is the back-pressure valve
	// fleetPool dispatches benchmark runs to the least-loaded bot-fleet instance.
	// Multiple submissions can benchmark concurrently on different machines,
	// each with exclusive CPU cores, so there is zero core contention.
	fleetPool *fleet.Pool
	logger    *log.Logger
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
	fleetPool := fleet.NewPool(cfg.FleetPoolURLs, logger)
	logger.Printf("[pool] fleet pool initialised with %d instance(s): %v",
		len(cfg.FleetPoolURLs), cfg.FleetPoolURLs)

	return &Pool{
		cfg:       cfg,
		db:        db,
		storage:   store,
		docker:    dockerCli,
		redis:     rdb,
		jobs:      make(chan string, cfg.WorkerCount),
		fleetPool: fleetPool,
		logger:    logger,
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

	// perfStarted is closed by triggerBotFleet's performance goroutine exactly
	// when the performance dispatch is confirmed (202 received). The cleanup
	// goroutine waits on this so the container lifetime is anchored to when the
	// performance benchmark actually starts, not to when this function runs.
	perfStarted := make(chan struct{})

	if err := p.triggerBotFleet(ctx, submissionID, result.TargetHost, perfStarted); err != nil {
		p.logger.Printf("[process:%s] bot fleet trigger failed: %v", submissionID, err)
		close(perfStarted) // unblock cleanup goroutine on trigger failure
	}

	_ = p.db.UpdateStatus(ctx, submissionID, "RUNNING", nil)

	// ── 5. Schedule cleanup after the benchmark window ────────────────────────
	// Wait for the performance phase to actually start before counting down.
	// This prevents premature container removal when fleet dispatch was delayed.
	go func() {
		// Absolute safety net: if perfStarted is never closed, clean up after
		// the maximum possible window (correctness + gap + perf + 10min buffer).
		maxWait := time.Duration(
			p.cfg.CorrectnessDurationSecs+benchmarkPhaseGapSecs+p.cfg.DefaultDurationSecs+600) * time.Second

		select {
		case <-perfStarted:
			// Performance confirmed — wait for it to complete + 5 min grace.
			p.logger.Printf("[process:%s] perf started, cleanup in %ds",
				submissionID, p.cfg.DefaultDurationSecs+300)
			time.Sleep(time.Duration(p.cfg.DefaultDurationSecs+300) * time.Second)
		case <-time.After(maxWait):
			p.logger.Printf("[process:%s] cleanup safety net fired", submissionID)
		}

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

// triggerBotFleet dispatches both benchmark phases for a submission.
//
// Correctness phase: single bot, fixed seed, full audit.
// Performance phase: concurrent bots, metrics only.
//
// The two phases run sequentially for this submission (correctness must
// complete before performance so the checker finalises the verdict first).
// Different submissions are dispatched to different fleet instances in parallel
// — no serialisation between submissions.
//
// perfStarted is closed exactly when the performance dispatch is confirmed
// (202 received from the fleet). The caller uses this signal to anchor the
// container cleanup timer to the actual start of the performance benchmark.
func (p *Pool) triggerBotFleet(ctx context.Context, submissionID, targetHost string, perfStarted chan<- struct{}) error {
	targetPort := fmt.Sprintf("%d", dockerpkg.SandboxPort)

	// ── Phase 1: correctness (serialized golden-model differential replay) ────
	correctness := map[string]any{
		"submission_id": submissionID,
		"target_host":   targetHost,
		"target_port":   targetPort,
		"protocol":      "rest",
		"mode":          "correctness",
		"num_bots":      1,
		"seed":          p.cfg.CorrectnessSeed,
		"duration_secs": p.cfg.CorrectnessDurationSecs,
	}
	if err := p.fleetPool.Dispatch(ctx, submissionID, correctness); err != nil {
		return fmt.Errorf("correctness phase dispatch: %w", err)
	}
	p.logger.Printf("[trigger:%s] correctness phase dispatched (seed=%d, %ds)",
		submissionID, p.cfg.CorrectnessSeed, p.cfg.CorrectnessDurationSecs)

	// ── Phase 2: performance — wait for correctness to finish, then dispatch ──
	go func() {
		time.Sleep(time.Duration(p.cfg.CorrectnessDurationSecs+benchmarkPhaseGapSecs) * time.Second)

		perf := map[string]any{
			"submission_id": submissionID,
			"target_host":   targetHost,
			"target_port":   targetPort,
			"protocol":      "rest",
			"mode":          "performance",
			"num_bots":      p.cfg.DefaultNumBots,
			"duration_secs": p.cfg.DefaultDurationSecs,
		}

		// Hard deadline: if the fleet is still full after this window, abort.
		// Prevents an orphaned goroutine spinning forever on context.Background().
		// The deadline is generous: 10 minutes of retry time on top of the sleep above.
		dispatchCtx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()

		if err := p.fleetPool.Dispatch(dispatchCtx, submissionID, perf); err != nil {
			p.logger.Printf("[trigger:%s] performance phase dispatch failed: %v",
				submissionID, err)
			close(perfStarted) // unblock cleanup goroutine on failure path
			return
		}

		p.logger.Printf("[trigger:%s] performance phase dispatched (%d bots, %ds)",
			submissionID, p.cfg.DefaultNumBots, p.cfg.DefaultDurationSecs)

		// Signal that performance has started — cleanup timer starts from here.
		close(perfStarted)
	}()

	return nil
}

// postBenchmark publishes the trigger to Redis Pub/Sub.
// The actual HTTP dispatch is handled by fleet.Pool.Dispatch().
func (p *Pool) postBenchmark(ctx context.Context, submissionID string, payload map[string]any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal trigger payload: %w", err)
	}

	if err := p.redis.Publish(ctx, botFleetTrigger, string(data)).Err(); err != nil {
		p.logger.Printf("[trigger] Redis publish failed: %v", err)
	}
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
