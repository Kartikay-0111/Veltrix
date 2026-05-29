# Artifact Checker (Go)

## Service Overview

- Purpose: reorder event streams, validate correctness, aggregate metrics, and publish leaderboard state.
- Business responsibility: authoritative correctness and performance scoring.
- Key workflows: consume Redpanda topics, reorder by watermark, validate orderbook, compute p50/p90/p99, publish to Redis and TimescaleDB.
- Dependencies: Redpanda, Postgres/TimescaleDB, Redis.

## Architecture

- Internal architecture:
  - Consumer: franz-go client consuming `order_events` and `order_metrics`.
  - Watermark router: per-submission event-time ordering.
  - Shadow engine: validates price-time correctness.
  - Aggregator: merges metrics batches into composite scores.
  - Publisher: Redis pubsub + TimescaleDB inserts.
- Event flow: Redpanda -> consumer -> watermark -> shadow engine -> correctness updates -> aggregator -> publisher.
- Error handling: validation failures are emitted as correctness updates; infra failures log and surface errors.

## Folder Structure

```
artifact-checker-go/
├── cmd/artifact-checker/main.go
└── internal/
    ├── aggregator/
    ├── consumer/
    ├── models/
    ├── shadowengine/
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
- `ARTIFACT_CHECKER_GOMAXPROCS` (optional)
- `POSTGRES_URL` (optional)
- `REDIS_ADDR` (optional)
- `HEALTH_PORT` (optional, default 8092)

## Running Locally

```bash
go run ./cmd/artifact-checker
```