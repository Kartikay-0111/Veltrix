# Service Dependency Map

## Overview

All services are deployed in a single docker-compose network for local development.

## Dependency Table

| Service | Language | Depends On | Protocols | Data Stores |
| --- | --- | --- | --- | --- |
| Submission Service | Go | Postgres, Redis, MinIO | HTTP | Postgres, Redis, MinIO |
| Sandbox Manager | Go | Postgres, Redis, MinIO, Docker | Docker Engine, Redis Pub/Sub | Postgres, Redis, MinIO |
| Bot Fleet | C++ | Redis, Telemetry Ingester | Redis Sub, gRPC | Redpanda via ingester |
| Telemetry Ingester | Go | Redpanda | gRPC, HTTP | Redpanda |
| Artifact Checker | Go | Redpanda, Postgres, Redis | Kafka, Redis | Postgres, Redis |
| Leaderboard Service | Go | Postgres, Redis | HTTP, WebSocket | Postgres, Redis |

## Data and Event Contracts

- `submission_queue` (Redis list): submission IDs queued by Submission Service, consumed by Sandbox Manager.
- `bot_fleet_triggers` (Redis Pub/Sub): JSON benchmark trigger published by Sandbox Manager, subscribed by Bot Fleet.
- `order_events` topic: JSON order/trade events (bot-fleet → telemetry ingester → Redpanda).
- `order_metrics` topic: JSON metrics batches (bot-fleet → telemetry ingester → Redpanda).
- `leaderboard_updates` (Redis Pub/Sub): JSON payload from artifact checker to leaderboard service.

## Shared Infrastructure

- **PostgreSQL**: all metadata — submissions, teams, benchmark jobs, leaderboard metrics.
- **Redis**: submission queue (list), bot fleet trigger (pub/sub), leaderboard state (hash + pub/sub).
- **MinIO**: submission zip archives.
- **Redpanda**: telemetry event bus.
