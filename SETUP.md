# Local Setup

This guide covers local setup only. It does not include production deployments.

## Supported OS

- Linux (native)
- macOS (Docker Desktop)
- Windows (WSL2 + Docker Desktop)

## Required Tools and Runtimes

Minimum versions recommended for local development:

- Docker Engine 24+ and docker-compose v2
- Python 3.12 (for running services outside Docker)
- Go 1.22+ (for running Go services outside Docker)
- C++20 toolchain (for building the bot-fleet or contestant submissions locally)
- PostgreSQL client tooling (optional, for debugging)

## Repository Layout

All runtime services live under [veltrix/](veltrix/). The root documentation applies to the entire repo.

## Installation Steps

1. Clone the repository:

```bash
git clone <repo-url>
cd iicpc
```

2. Create a local environment file for docker-compose:

```bash
cp .env.example veltrix/.env
```

3. Start infrastructure and services (build once):

```bash
cd veltrix
docker compose up -d --build
```

4. Verify basic health endpoints:

```bash
curl http://localhost:8080/health
curl http://localhost:8090/health
curl http://localhost:8085/health
curl http://localhost:7070/health
```

## Environment Variables

This project uses a single `.env` file loaded by docker-compose. The example below is in `.env.example`.

### Core Infrastructure

- `POSTGRES_USER` (required): Database user.
- `POSTGRES_PASSWORD` (required): Database password.
- `POSTGRES_DB` (required): Database name.
- `POSTGRES_HOST` (required): Hostname for Postgres (default: `postgres`).
- `POSTGRES_PORT` (optional): Postgres port (default: `5432`).

- `REDIS_HOST` (required): Redis host (default: `redis`).
- `REDIS_PORT` (optional): Redis port (default: `6379`).

- `MINIO_ROOT_USER` (required): MinIO root user.
- `MINIO_ROOT_PASSWORD` (required): MinIO root password.
- `MINIO_HOST` (required): MinIO host (default: `minio`).
- `MINIO_PORT` (optional): MinIO port (default: `9000`).
- `MINIO_BUCKET` (required): Bucket used for submission artifacts.

### Sandbox Manager

- `SANDBOX_NETWORK` (required): Docker network for contestant containers.
- `FLEET_COMMANDER_URL` (required): Bot fleet trigger endpoint.
- `DEFAULT_NUM_BOTS` (optional): Default bot count per run.
- `DEFAULT_DURATION_SECS` (optional): Default benchmark duration.
- `SANDBOX_MANAGER_HEALTH_PORT` (optional): Health server port (default: `8081`).

### Telemetry Ingester

- `REDPANDA_BROKERS` (required): Comma-separated brokers.
- `ORDER_EVENTS_TOPIC` (required): Topic for order events.
- `ORDER_METRICS_TOPIC` (required): Topic for metrics batches.
- `GRPC_PORT` (optional): gRPC port (default: `8091`).
- `HTTP_PORT` (optional): HTTP port (default: `8090`).

### Artifact Checker

- `CONSUMER_GROUP` (optional): Redpanda consumer group.
- `ALLOWED_LATENESS_MS` (optional): Watermark lateness (ms).
- `ARTIFACT_CHECKER_GOMAXPROCS` (optional): GOMAXPROCS override.
- `HEALTH_PORT` (optional): Health server port (default: `8092`).
- `POSTGRES_URL` (optional): Full Postgres URL; overrides host/user/db.
- `REDIS_ADDR` (optional): Full Redis address; overrides host/port.

## Local Development Setup (Without Docker)

Run services outside Docker if needed.

```bash
# Submission service
cd veltrix/submission-service
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8080

# Sandbox manager
cd veltrix/sandbox-manager
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py

# Telemetry ingester
cd veltrix/telemetry-ingester
go run ./...

# Artifact checker
cd veltrix/artifact-checker-go
go run ./cmd/artifact-checker

# Leaderboard
cd veltrix/leaderboard-service
go run .
```

The Docker-based stack remains the recommended local setup because it wires Redis, Postgres, MinIO, and Redpanda automatically.
