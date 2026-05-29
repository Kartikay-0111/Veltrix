# Service Dependency Map

## Overview

All services are deployed in a single docker-compose network for local development.

## Dependency Table

| Service | Depends On | Protocols | Data Stores |
| --- | --- | --- | --- |
| Submission Service | Postgres, Redis, MinIO | HTTP | Postgres, Redis, MinIO |
| Sandbox Manager | Postgres, Redis, MinIO, Bot Fleet | HTTP, Docker Engine | Postgres, Redis, MinIO |
| Bot Fleet | Telemetry Ingester | HTTP, gRPC | Redpanda via ingester |
| Telemetry Ingester | Redpanda | gRPC, HTTP | Redpanda |
| Artifact Checker | Redpanda, Postgres, Redis | Kafka, Redis | Postgres, Redis |
| Leaderboard Service | Postgres, Redis | HTTP, WebSocket | Postgres, Redis |

## Data and Event Contracts

- `order_events` topic: JSON order/trade events (from bot-fleet via telemetry ingester).
- `order_metrics` topic: JSON metrics batches (from bot-fleet via telemetry ingester).
- `leaderboard_updates` channel: JSON payload from artifact checker to leaderboard service.

## Shared Infrastructure

- Postgres: submissions and leaderboard metrics.
- Redis: submission queue and leaderboard state/pubsub.
- MinIO: submission archives.
- Redpanda: telemetry event bus.
