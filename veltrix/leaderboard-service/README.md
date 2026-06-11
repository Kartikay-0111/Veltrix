# Leaderboard Service

## Service Overview

- Purpose: broadcast live leaderboard updates and serve the UI.
- Business responsibility: read state from Redis/Postgres and stream updates to clients.
- Key workflows: render initial leaderboard, subscribe to Redis pubsub, broadcast updates.
- Dependencies: Postgres, Redis.

## Architecture

- Internal architecture:
  - Hub: WebSocket registry with backpressure handling.
  - Redis subscriber: renders HTML rows and pushes to the hub.
  - HTTP handlers: index, leaderboard fragment, WebSocket, health.
- Request flow:
  1. Client loads `/` and connects to `/ws/leaderboard`.
  2. Service fetches latest metrics from Postgres.
  3. Redis updates are pushed as HTML fragments over WebSocket.

## Folder Structure

```
leaderboard-service/
├── Dockerfile
├── main.go
├── db.go
├── hub.go
├── subscriber.go
└── templates/
```

## API Documentation

### GET /

- Purpose: serves the leaderboard UI.

### GET /leaderboard

- Purpose: returns the table fragment used by HTMX.

### GET /ws/leaderboard

- Purpose: WebSocket stream of leaderboard updates.

### GET /health

- Purpose: liveness check.
- Response 200: `{ "status": "ok" }`.

## Configuration

- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
- `REDIS_HOST`, `REDIS_PORT`
- `PORT` (optional, default 8085)

## Running Locally

```bash
go run .
```
