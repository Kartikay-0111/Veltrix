# Developer Onboarding

Welcome to Veltrix. This guide provides the minimum steps to be productive on day one.

## Prerequisites

- Docker + docker-compose
- Go 1.21+
- C++20 toolchain (for bot-fleet or sample submissions)

## Quick Start

1. Clone the repo and create your env file:

```bash
cp veltrix/.env.example veltrix/.env
```

2. Run the full stack:

```bash
cd veltrix
docker compose up -d --build
```

3. Open the leaderboard:

```bash
open http://localhost:8085
```

## Key Entry Points

- Submission Service: [veltrix/submission-service/cmd/main.go](veltrix/submission-service/cmd/main.go)
- Sandbox Manager: [veltrix/sandbox-manager/cmd/main.go](veltrix/sandbox-manager/cmd/main.go)
- Bot Fleet: [veltrix/bot-fleet/src/fleet_commander.cpp](veltrix/bot-fleet/src/fleet_commander.cpp)
- Telemetry Ingester: [veltrix/telemetry-ingester/main.go](veltrix/telemetry-ingester/main.go)
- Artifact Checker: [veltrix/artifact-checker/cmd/artifact-checker/main.go](veltrix/artifact-checker/cmd/artifact-checker/main.go)
- Leaderboard Service: [veltrix/leaderboard-service/main.go](veltrix/leaderboard-service/main.go)

## Debugging Tips

- Use `docker compose logs -f <service>` for realtime logs.
- Inspect Postgres state via `docker compose exec postgres psql`.
- Check Redis queue and leaderboard state via `docker compose exec redis redis-cli`.
- Validate Redpanda topics via `rpk topic list`.

## Coding Standards

- Keep services small and focused with clear module boundaries.
- Prefer explicit error paths and log with context (submission_id, team_id).
- Avoid new shared state in the hot paths (bot-fleet, artifact-checker).
- Preserve backward compatibility with existing message schemas.

## Branching Strategy

- Use short-lived feature branches off `main`.
- Keep changes scoped to one service when possible.
- Rebase or merge from `main` frequently to avoid drift.

## Deployment Workflow (Local Only)

This repo documents local docker-compose usage only. Production deployment is out of scope in local docs.
