# Technical Debt Report

This report lists known risks and improvement areas based on the current codebase.

## Risks and Concerns

- Bot fleet JSON parsing is regex-based and assumes well-formed payloads. This is lightweight but fragile.
- Submission service and sandbox manager have no rate limiting or request throttling.
- Sandbox manager has no backoff or concurrency control beyond Redis queue polling.
- Telemetry ingester and artifact checker lack distributed tracing and structured JSON logs.
- The artifact checker uses in-memory state only; a crash resets correctness state until rebuilt.

## Scalability Concerns

- One sandbox manager processes jobs sequentially; scale-out requires sharding or distributed workers.
- Bot fleet is CPU-core bound and uses one process; horizontal scaling would require multiple fleet instances.
- Redis pubsub is used for leaderboard fanout; at high scale, this can become a bottleneck.

## Security Concerns

- Submissions are accepted with an API key but no rotation/expiry strategy is enforced.
- Sandbox containers have strong CPU/memory limits but are still sharing the host kernel.
- Artifact checker and telemetry ingester accept internal traffic without auth/TLS.

## Refactor Recommendations

- Replace regex JSON parsing in bot-fleet with a small JSON library.
- Add a bounded worker pool for sandbox manager to limit concurrent builds.
- Add a health/ready endpoint for every service and wire compose health checks.
- Introduce structured logging and a correlation ID for submission_id across services.

## Performance Bottlenecks

- Redpanda topic partitions are fixed at 6; this may be insufficient for high bot counts.
- Postgres writes in the artifact checker are single-row inserts; batching could reduce load.

## Legacy/Experimental Areas

- `veltrix/architecture_part*.md` have been superseded by root documentation and can be removed.
