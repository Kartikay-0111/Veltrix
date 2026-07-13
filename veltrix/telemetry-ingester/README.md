# Telemetry Ingester

## Service Overview

- Purpose: accept telemetry over gRPC, publish order events and metrics to Redpanda.
- Business responsibility: bridge bot-fleet telemetry into the event bus.
- Key workflows: stream `AuditBatch`, split into order events and metrics, publish to topics.
- Dependencies: Redpanda.
- External integrations: gRPC client streams from bot-fleet.

## Architecture

- Internal architecture:
	- gRPC `StreamTelemetry` handler for `AuditBatch` messages.
	- Redpanda producer (franz-go) that publishes JSON records.
	- HTTP server for health and latest metrics.
- Event flow:
	1. Bot fleet sends `AuditBatch` over gRPC.
	2. Ingester publishes `order_events` and `order_metrics` records.
	3. Artifact checker consumes those topics.
- Records are keyed by `submission_id` (so one submission's events stay in one
	partition, in order) and carry a JSON value. Intents pass through the per-attempt
	`outcome` tag (proto enum `0=OK / 1=REJECTED / 2=UNKNOWN`, mapped to the strings
	`"" / "REJECTED" / "UNKNOWN"`) so the checker can classify lost or rejected orders.

## Folder Structure

```
telemetry-ingester/
├── Dockerfile
├── main.go
└── internal/
		├── grpcserver/
		├── pb/
		└── producer/
```

## API Documentation

### gRPC: TelemetryService/StreamTelemetry

- Purpose: receive streaming `AuditBatch` messages from bot-fleet.
- Request: stream of `AuditBatch` messages (protobuf).
- Response: `StreamTelemetryResponse` with `batches_received` and `status`.

### GET /health

- Purpose: liveness check.
- Response 200: `{ "status": "ok" }`.

### GET /metrics/latest

- Purpose: return the latest metrics batch seen by the ingester.
- Response 200: JSON metrics batch.
- Response 404: `{ "detail": "No metrics received yet" }`.

## Configuration

- `REDPANDA_BROKERS` (required)
- `ORDER_EVENTS_TOPIC` (required)
- `ORDER_METRICS_TOPIC` (required)
- `GRPC_PORT` (optional, default 8091)
- `HTTP_PORT` (optional, default 8090)

## Running Locally

```bash
go run ./...
```