# Submission Service

## Service Overview

- Purpose: ingest contestant submissions, store artifacts, and enqueue sandbox jobs.
- Business responsibility: entrypoint for contestant uploads and status polling.
- Key workflows: upload archive, create submission row, push to Redis queue.
- Dependencies: Postgres, Redis, MinIO.
- External integrations: MinIO S3-compatible API.

## Architecture

- Internal architecture: FastAPI app with asyncpg, Redis client, and boto3 S3 client.
- Request flow:
     1. `POST /submit` streams multipart upload to MinIO.
     2. Insert submission metadata into Postgres.
     3. Enqueue submission ID in Redis `submission_queue`.
     4. Sandbox manager consumes the queue and builds containers.
- Database interaction: `teams` and `submissions` tables in Postgres.
- Queue usage: Redis list `submission_queue`.
- Authentication: `x-api-key` header mapped to `teams.api_key`.
- Error handling: validates API keys and language, propagates storage errors as 5xx.

## Folder Structure

```
submission-service/
├── Dockerfile
├── main.py          # FastAPI app, storage, and queueing
└── requirements.txt
```

## API Documentation

### POST /submit

- Purpose: upload a submission archive for sandboxing.
- Auth: `x-api-key` header required.
- Query params: `language` (cpp|rust|go), default `cpp`.
- Request: multipart form with field `file`.
- Response 200:

```json
{
     "submission_id": "uuid",
     "status": "PENDING",
     "message": "Submission received. Container will be ready shortly."
}
```

- Errors:
     - 400: invalid language.
     - 401: invalid API key.

### GET /submission/{submission_id}

- Purpose: poll submission status and endpoint.
- Auth: `x-api-key` header required.
- Response 200:

```json
{
     "submission_id": "uuid",
     "status": "READY",
     "endpoint_url": "http://sandbox-<id>:9999",
     "error": null
}
```

- Errors:
     - 404: submission not found for the API key.

### GET /health

- Purpose: basic liveness check.
- Response 200: `{ "status": "ok" }`.

## Configuration

Environment variables (loaded via `.env`):

- `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_HOST`, `POSTGRES_PORT`
- `REDIS_HOST`, `REDIS_PORT`
- `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `MINIO_HOST`, `MINIO_PORT`, `MINIO_BUCKET`

## Running Locally

```bash
uvicorn main:app --host 0.0.0.0 --port 8080
```

FastAPI docs are available at `/docs` when running locally.