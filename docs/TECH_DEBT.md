# Technical Debt Report

This report lists known risks and improvement areas based on the current codebase.

## Risks and Concerns

- Bot fleet JSON parsing is regex-based and assumes well-formed payloads. This is lightweight but fragile.
- Submission service and sandbox manager have no rate limiting or request throttling per team.
- Telemetry ingester and artifact checker lack distributed tracing and structured JSON logs.
- The artifact checker uses in-memory state only; a crash resets correctness state until rebuilt from Redpanda.

## Scalability Concerns

- Bot fleet is CPU-core bound and uses one process; horizontal scaling would require multiple fleet instances.
- Redis pubsub is used for leaderboard fanout; at high scale this can become a bottleneck.
- Redpanda topic partitions are fixed at 6; this may be insufficient for very high bot counts.

## Resolved Debt

| Item | Resolution |
|---|---|
| **Sequential Python Sandbox Manager** | Replaced with Go Bounded Worker Pool (`CONFIG_WORKER_COUNT`, default 10). Workers run concurrently via goroutines + buffered channel. Back-pressure is built-in: dispatcher blocks when all workers are busy. No goroutine explosion. |
| **Database Sprawl (Postgres + TimescaleDB)** | TimescaleDB removed entirely. `postgres:16-alpine` is the single database engine. `leaderboard_metrics` is a plain Postgres table with a composite index on `(team_id, time DESC)`. |
| **HTTP Fleet Trigger coupling** | Sandbox Manager no longer HTTP-POSTs to bot-fleet. It now publishes a JSON payload to Redis channel `bot_fleet_triggers`. The C++ FleetCommander subscribes to this channel, removing the direct service dependency. |

## Security Concerns

- Submissions are accepted with an API key but no rotation/expiry strategy is enforced.
- Sandbox containers have strong CPU/memory/PID limits but share the host kernel.
- Artifact checker and telemetry ingester accept internal traffic without auth/TLS.

## Refactor Recommendations

- Replace regex JSON parsing in bot-fleet with a small JSON library (e.g. `nlohmann/json`).
- Add a health/ready endpoint for every service — all Go services already have one; bot-fleet needs it.
- Introduce structured logging (zerolog or zap) and a `submission_id` correlation field across services.
- Add per-team rate limiting to the submission service (e.g. token bucket in Redis).

## Performance Bottlenecks

- Postgres writes in the artifact checker are single-row inserts; batching could reduce write amplification.

## Legacy/Experimental Areas

- `veltrix/architecture_part*.md` have been superseded by `ARCHITECTURE.md` and can be removed.
