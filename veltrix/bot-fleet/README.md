# Bot Fleet (C++20)

## Service Overview

- Purpose: generate high-concurrency traffic against contestant sandboxes.
- Business responsibility: drive load, capture intent and execution telemetry.
- Key workflows: receive benchmark request, spawn per-core workers, stream telemetry.
- Dependencies: telemetry-ingester (gRPC).
-
## Architecture

- Internal architecture:
  - FleetCommander: HTTP server for `/benchmark` and `/health`.
  - ThreadWorker: one OS thread per CPU core, io_uring event loop.
  - RestBot: order generator and request builder.
  - gRPC client: streams `AuditBatch` to telemetry ingester.
- Request flow:
  1. Sandbox manager calls `/benchmark`.
  2. FleetCommander splits bots across cores.
  3. Workers emit order requests and collect telemetry.
  4. Telemetry is sent over gRPC to the ingester.

## Folder Structure

```
bot-fleet/
├── CMakeLists.txt
├── Dockerfile
├── include/
└── src/
```

## API Documentation

### POST /benchmark

- Purpose: start a benchmark run.
- Request JSON:

```json
{
  "submission_id": "uuid",
  "target_host": "sandbox-<id>",
  "target_port": "9999",
  "num_bots": 1000,
  "duration_secs": 60,
  "protocol": "rest"
}
```

- Response 202:

```json
{ "status": "benchmark_started", "submission_id": "uuid" }
```

### GET /health

- Purpose: liveness check.
- Response 200: `{ "status": "ok" }`.

## Configuration

- `FLEET_LISTEN_PORT` (optional, default 7070)
- `TELEMETRY_GRPC_TARGET` (required)

## Running Locally

```bash
cmake -S . -B build
cmake --build build
./build/bot_fleet
```
