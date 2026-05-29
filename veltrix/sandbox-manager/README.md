# Sandbox Manager

## Service Overview

- Purpose: build contestant sandboxes, enforce resource isolation, and trigger benchmarks.
- Business responsibility: translate submissions into runnable containers and update lifecycle state.
- Key workflows: dequeue job, build image, run container, health probe, trigger bot fleet, cleanup.
- Dependencies: Postgres, Redis, MinIO, Docker Engine, Bot Fleet.
- External integrations: Docker Engine API, HTTP call to bot-fleet.

## Architecture

- Internal architecture:
        - Redis job poller (`submission_queue`).
        - Safe archive extraction (zip/tar validation, size limits, no symlinks).
        - Docker build and run with strict resource limits.
        - Port readiness probe (TCP connect).
        - Fleet trigger via `POST /benchmark`.
- Request flow:
        1. Read submission ID from Redis.
        2. Download archive from MinIO.
        3. Build image and run container on `sandbox-net`.
        4. Poll port 9999 for readiness.
        5. Update Postgres and trigger bot fleet.
- Database interaction: updates `submissions` status and endpoint_url.
- Error handling: maps Docker exit codes to `FAILED_*` statuses.

## Folder Structure

```
sandbox-manager/
├── Dockerfile
├── main.py          # Job poller, build/run logic, cleanup
└── requirements.txt
```

## API Documentation

### GET /health

- Purpose: liveness check for the worker process.
- Response 200: `{ "status": "ok" }`.

## Configuration

Environment variables (loaded via `.env`):

- `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_HOST`, `POSTGRES_PORT`
- `REDIS_HOST`, `REDIS_PORT`
- `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `MINIO_HOST`, `MINIO_PORT`, `MINIO_BUCKET`
- `SANDBOX_NETWORK`
- `FLEET_COMMANDER_URL`
- `DEFAULT_NUM_BOTS`, `DEFAULT_DURATION_SECS`
- `SANDBOX_MANAGER_HEALTH_PORT` (default 8081)

## Running Locally

```bash
python main.py
```

## Security and Isolation Notes

- Containers are capped to 1 CPU core and 512MB memory.
- Linux capabilities are dropped and `no-new-privileges` is enforced.
- Archive extraction blocks absolute paths, traversal, and symlinks.