# Running Locally

This guide explains how to run the full stack and each service locally.

## Running the Full Stack

From [veltrix/](veltrix/):

```bash
docker compose up -d --build
```

To rebuild a single service:

```bash
docker compose up -d --build submission-service
```

To stop everything:

```bash
docker compose down
```

## Ports and Endpoints

- Submission Service: `http://localhost:8080`
- Bot Fleet: `http://localhost:7070`
- Telemetry Ingester (HTTP): `http://localhost:8090`
- Telemetry Ingester (gRPC): `localhost:8091`
- Artifact Checker health: `http://localhost:8092/health`
- Leaderboard Service: `http://localhost:8085`
- MinIO console: `http://localhost:9001`
- Postgres: `localhost:5432`
- Redis: `localhost:6379`
- Redpanda Kafka API: `localhost:9092`

## Running Individual Services

### Submission Service (FastAPI)

```bash
cd veltrix/submission-service
uvicorn main:app --host 0.0.0.0 --port 8080
```

### Sandbox Manager

```bash
cd veltrix/sandbox-manager
python main.py
```

### Bot Fleet

```bash
cd veltrix
./bot-fleet/build/bot_fleet
```

### Telemetry Ingester

```bash
cd veltrix/telemetry-ingester
go run ./...
```

### Artifact Checker

```bash
cd veltrix/artifact-checker-go
go run ./cmd/artifact-checker
```

### Leaderboard Service

```bash
cd veltrix/leaderboard-service
go run .
```

## Viewing Logs Locally

### Docker Compose Logs

```bash
docker compose logs -f
docker compose logs -f submission-service
docker compose logs -f bot-fleet
docker compose logs -f telemetry-ingester
```

### Non-Docker Logs

Services log to stdout/stderr. When run locally, logs appear in the terminal where the process is started.

## Restarting Services

- Restart one service:

```bash
docker compose restart submission-service
```

- Restart all services:

```bash
docker compose restart
```

- Stop all services:

```bash
docker compose down
```
