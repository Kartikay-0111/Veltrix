# Local Troubleshooting

This guide focuses on common local development issues and their fixes.

## Docker Compose Fails to Start

- Symptoms: containers exit immediately or never become healthy.
- Root cause: missing env file or invalid environment variables.
- Fix:
  - Ensure `.env` exists in [veltrix/](veltrix/).
  - Validate values with `docker compose config`.

## Port Conflicts

- Symptoms: "bind: address already in use" when starting a service.
- Root cause: another process using the same port.
- Fix:
  - Find the process: `lsof -i :8080` or `ss -ltnp`.
  - Stop it or update ports in docker-compose.

## Submission Service Returns 401

- Symptoms: `/submit` returns `Invalid API key`.
- Root cause: missing or incorrect `x-api-key` header.
- Fix:
  - Use the seeded API key from Postgres: `test-api-key-1234`.

## Sandbox Fails to Start

- Symptoms: submissions stuck in `FAILED_STARTUP` or `FAILED_SYSTEM`.
- Root cause: invalid archive, build failure, or missing `server` binary.
- Fix:
  - Confirm submission matches requirements in [veltrix/test/README.md](veltrix/test/README.md).
  - Inspect sandbox-manager logs: `docker compose logs -f sandbox-manager`.

## Redis or Postgres Connection Errors

- Symptoms: services log connection failures on startup.
- Root cause: infrastructure containers not healthy.
- Fix:
  - `docker compose ps` and wait for `healthy` status.
  - Restart infra services: `docker compose restart postgres redis`.

## No Leaderboard Updates

- Symptoms: leaderboard UI loads but never updates.
- Root cause: no telemetry reaching Redis.
- Fix:
  - Confirm bot-fleet can reach telemetry-ingester (`telemetry-ingester:8091`).
  - Verify Redpanda topics exist: `rpk topic list -X brokers=localhost:9092`.
  - Check artifact-checker logs for consumer errors.

## Redpanda Topic Errors

- Symptoms: `unknown topic` or `partition` errors.
- Root cause: topics not created.
- Fix:
  - Recreate topics: `docker compose up -d redpanda-init`.

## MinIO Upload Failures

- Symptoms: `/submit` fails with storage errors.
- Root cause: bucket missing or MinIO credentials mismatch.
- Fix:
  - Verify MinIO health: `curl http://localhost:9000/minio/health/ready`.
  - Ensure `MINIO_*` env vars match the compose file.

## Debug Commands

```bash
# Show container status
cd veltrix
docker compose ps

# Inspect recent logs
cd veltrix
docker compose logs --tail=200 submission-service

# Verify Postgres tables
docker compose exec postgres psql -U iicpc -d iicpc_db -c "\\dt"
```
