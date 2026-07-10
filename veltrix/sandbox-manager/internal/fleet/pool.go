// internal/fleet/pool.go — Fleet pool for horizontal bot-fleet scaling.
//
// Instead of a single bot-fleet instance (and a serializing benchGate), the
// pool maintains N fleet instances and dispatches each benchmark to the
// least-loaded one that has a free slot. Instances run on separate machines,
// each owning their CPU cores exclusively, so there is zero core contention
// between concurrent benchmarks.
//
// Usage:
//
//	pool := fleet.NewPool([]string{
//	    "http://bot-fleet-1:7070",
//	    "http://bot-fleet-2:7070",
//	}, logger)
//	err := pool.Dispatch(ctx, payload)
package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// healthPollInterval is how often the pool refreshes each instance's capacity.
	healthPollInterval = 10 * time.Second

	// dispatchTimeout is the HTTP timeout for the /benchmark POST.
	dispatchTimeout = 10 * time.Second

	// healthTimeout is the HTTP timeout for /health checks.
	healthTimeout = 3 * time.Second
)

// healthResponse is the JSON shape returned by bot-fleet GET /health.
type healthResponse struct {
	Status string `json:"status"`
	Active int    `json:"active"`
	Max    int    `json:"max"`
}

// Instance represents one bot-fleet machine.
type Instance struct {
	URL string

	// active and max are refreshed by the background health poller.
	// They are read-only from outside the poller — written only by pollHealth.
	active atomic.Int32
	max    atomic.Int32 // 0 = unknown (treat as 1 slot safe default)
}

func (inst *Instance) hasCapacity() bool {
	max := inst.max.Load()
	if max <= 0 {
		max = 1 // conservative default before first health poll
	}
	return inst.active.Load() < max
}

// Pool dispatches benchmark jobs across N bot-fleet instances.
type Pool struct {
	instances []*Instance
	mu        sync.Mutex // guards dispatch selection
	logger    *log.Logger
	client    *http.Client
}

// NewPool creates a Pool from a list of bot-fleet base URLs.
// It immediately starts a background goroutine that polls each instance's
// /health endpoint every 10 seconds to keep active/max counts fresh.
func NewPool(urls []string, logger *log.Logger) *Pool {
	if logger == nil {
		logger = log.Default()
	}

	instances := make([]*Instance, 0, len(urls))
	for _, u := range urls {
		inst := &Instance{URL: u}
		inst.max.Store(4) // sane default until first health poll
		instances = append(instances, inst)
	}

	p := &Pool{
		instances: instances,
		logger:    logger,
		client:    &http.Client{Timeout: dispatchTimeout},
	}

	go p.healthPoller()
	return p
}

// Dispatch sends a benchmark payload to the least-loaded fleet instance
// that has capacity. It blocks until a slot is free or ctx is cancelled.
//
// The payload must be a JSON-serialisable map containing at minimum:
//
//	submission_id, target_host, target_port, mode, num_bots, duration_secs
func (p *Pool) Dispatch(ctx context.Context, submissionID string, payload map[string]any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("fleet.Dispatch: marshal payload: %w", err)
	}

	// Retry loop: keep trying until we find a slot or ctx is cancelled.
	// In practice the first attempt succeeds unless all instances are at max.
	backoff := 2 * time.Second
	for attempt := 1; ; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		inst := p.pickLeastLoaded()
		if inst == nil {
			p.logger.Printf("[fleet] all instances at capacity (attempt %d), waiting %s…",
				attempt, backoff)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
				if backoff < 30*time.Second {
					backoff *= 2
				}
				continue
			}
		}

		if err := p.post(ctx, inst, data); err != nil {
			p.logger.Printf("[fleet] instance %s failed (attempt %d): %v — trying another",
				inst.URL, attempt, err)
			// Don't count as active — the request failed
			continue
		}

		p.logger.Printf("[fleet] dispatched submission=%s → %s (active=%d)",
			submissionID, inst.URL, inst.active.Load())
		return nil
	}
}

// pickLeastLoaded returns the instance with the smallest active/max ratio
// that still has a free slot. Returns nil if all instances are full.
func (p *Pool) pickLeastLoaded() *Instance {
	p.mu.Lock()
	defer p.mu.Unlock()

	var best *Instance
	bestLoad := float64(2) // > 1.0 means "no candidate found"

	for _, inst := range p.instances {
		max := inst.max.Load()
		if max <= 0 {
			max = 1
		}
		active := inst.active.Load()
		if active >= max {
			continue // full
		}
		load := float64(active) / float64(max)
		if best == nil || load < bestLoad {
			best = inst
			bestLoad = load
		}
	}

	return best
}

// post sends the /benchmark POST to a specific instance.
// It increments active before the call and decrements it on failure
// (on success, the health poller will update active from /health).
func (p *Pool) post(ctx context.Context, inst *Instance, data []byte) error {
	inst.active.Add(1)

	reqCtx, cancel := context.WithTimeout(ctx, dispatchTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost,
		inst.URL+"/benchmark", bytes.NewReader(data))
	if err != nil {
		inst.active.Add(-1)
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		inst.active.Add(-1)
		return fmt.Errorf("http post: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		inst.active.Add(-1)
		return fmt.Errorf("non-2xx response %d: %s", resp.StatusCode, string(body))
	}

	// 202 accepted — the benchmark is now running on this instance.
	// active will be corrected by the next health poll.
	return nil
}

// healthPoller runs forever in a goroutine, polling each instance's /health
// endpoint every healthPollInterval to keep active/max counts accurate.
func (p *Pool) healthPoller() {
	ticker := time.NewTicker(healthPollInterval)
	defer ticker.Stop()

	// Also poll immediately on startup.
	p.pollAll()

	for range ticker.C {
		p.pollAll()
	}
}

func (p *Pool) pollAll() {
	for _, inst := range p.instances {
		p.pollHealth(inst)
	}
}

func (p *Pool) pollHealth(inst *Instance) {
	ctx, cancel := context.WithTimeout(context.Background(), healthTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, inst.URL+"/health", nil)
	if err != nil {
		return
	}

	resp, err := p.client.Do(req)
	if err != nil {
		p.logger.Printf("[fleet:health] instance %s unreachable: %v", inst.URL, err)
		return
	}
	defer resp.Body.Close()

	var h healthResponse
	if err := json.NewDecoder(resp.Body).Decode(&h); err != nil {
		p.logger.Printf("[fleet:health] instance %s bad response: %v", inst.URL, err)
		return
	}

	inst.active.Store(int32(h.Active))
	if h.Max > 0 {
		inst.max.Store(int32(h.Max))
	}
}
