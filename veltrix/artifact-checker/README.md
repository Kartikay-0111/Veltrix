# Artifact Checker (Go)

## Service Overview

- Purpose: reorder event streams, validate correctness, aggregate metrics, and publish leaderboard state.
- Business responsibility: authoritative correctness and performance scoring.
- Key workflows: consume Redpanda topics, reorder by watermark, validate orderbook, compute p50/p90/p99, publish to Redis and Postgres.
- Dependencies: Redpanda, Postgres, Redis.

## Architecture

- Internal architecture:
  - Consumer: franz-go client consuming `order_events` and `order_metrics`.
  - Watermark router: per-submission event-time ordering.
  - Replay engine (`replayengine`): golden-model differential replay — replays each
    submission's `seq`-ordered stream through a trusted price-time reference and
    diffs it against the contestant's reported fills. Buffering stays single-goroutine
    (lock-free) while the CPU-heavy replay is offloaded onto worker goroutines
    (bounded by `GOMAXPROCS`), so a burst of finalizations never stalls the intake.
  - Aggregator: merges metrics batches into composite scores.
  - Publisher: Redis pubsub + Postgres inserts.
- Event flow: Redpanda -> consumer -> watermark -> replay engine -> correctness updates -> aggregator -> publisher.
- Verdict is tri-state (`correct` / `incorrect` / `unverified`); `unverified` is the
  fail-safe default for any run that cannot be conclusively judged (missing end-of-run
  marker, a `seq` gap, an `UNKNOWN` order outcome, or an unmappable fill). See
  [`docs/matching-spec.md`](../docs/matching-spec.md) and
  [`docs/architecture.md`](../docs/architecture.md).
- Error handling: validation failures are emitted as correctness updates; infra failures log and surface errors.

## Folder Structure

```
artifact-checker/
├── cmd/artifact-checker/main.go
└── internal/
    ├── aggregator/
    ├── consumer/
    ├── models/
    ├── replayengine/
    ├── storage/
    └── watermark/
```

## API Documentation

### GET /health

- Purpose: liveness check for the checker process.
- Response 200: `{ "status": "ok" }`.

## Configuration

- `REDPANDA_BROKERS` (required)
- `ORDER_EVENTS_TOPIC` (required)
- `METRICS_TOPIC` (required)
- `CONSUMER_GROUP` (optional)
- `ALLOWED_LATENESS_MS` (optional)
- `ARTIFACT_CHECKER_GOMAXPROCS` (optional, default = number of CPUs; also bounds the
  count of concurrent replay workers)
- `POSTGRES_URL` (optional)
- `REDIS_ADDR` (optional)
- `HEALTH_PORT` (optional, default 8092)

## Running Locally

```bash
go run ./cmd/artifact-checker
```