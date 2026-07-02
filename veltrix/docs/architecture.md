# Veltrix Architecture

Veltrix grades contestant matching engines on two axes — **correctness** (does it
match orders exactly like a standard price-time engine?) and **performance**
(latency/throughput under load) — and publishes a live leaderboard.

The overriding design constraint is **soundness**: code that is correct must never
be marked incorrect. Where the system cannot be sure, it says so (`unverified`)
rather than guessing.

## Services and data flow

```
                submission-service ──► MinIO (artifact)  +  Postgres (record)
                                          │  enqueue submission_id (Redis list)
                                          ▼
   ┌──────────────────────── sandbox-manager (bounded worker pool) ───────────────────────┐
   │  BLPOP submission_queue → build image → run sandbox-<id>:9999 → trigger bot-fleet     │
   └──────────────────────────────────────────────────────────────────────────────────────┘
                                          │ POST /benchmark  (correctness, then performance)
                                          ▼
   bot-fleet (C++)  ──gRPC stream──►  telemetry-ingester  ──►  Redpanda
     • correctness: 1 bot, fixed seed, full audit + END          order_events   (audit)
     • performance: N bots, metrics only                         order_metrics  (latency)
                                          │
                                          ▼
   ┌──────────────────────────── artifact-checker (Go) ──────────────────────────────┐
   │  consumer → watermark router → replayengine  (order_events → correctness verdict)│
   │           → aggregator                        (order_metrics → tps/percentiles)  │
   │  replayengine verdict + aggregator metrics → Score → storage publisher           │
   └──────────────────────────────────────────────────────────────────────────────────┘
                                          │
                                          ▼
                     Redis  (leaderboard_state, live updates)  +  Postgres (leaderboard_metrics)
```

Stack: Go services (submission-service, sandbox-manager, telemetry-ingester,
artifact-checker, leaderboard-service) + a C++20 Boost.Asio bot-fleet, over
Redpanda, Postgres, Redis, and MinIO, orchestrated by docker-compose. Contestant
sandboxes are built and run via Docker-in-Docker.

## Two-phase grading

Each submission is graded in two time-separated phases against its own sandbox
(`sandbox-<submission_id>:9999`):

1. **Correctness phase** — `mode=correctness`, **one** bot, a **fixed RNG seed**,
   serialized single-writer order stream. Every order carries a monotonic `seq`
   and the run ends with an **end-of-run marker**. Full audit (orders + trades)
   is streamed to `order_events`. Because there is exactly one writer, send-order
   == the contestant's process-order == the `seq` the golden model replays.

2. **Performance phase** — `mode=performance`, N concurrent bots, **metrics only**
   (no per-order audit), streamed to `order_metrics`. Drives latency/throughput.

sandbox-manager runs correctness first, waits a gap, then performance, so the two
never hit the contestant concurrently (which would break serialized ordering).

## Correctness: golden-model differential replay

`artifact-checker/internal/replayengine` buffers a submission's `order_events` and,
on the end-of-run marker, replays them in `seq` order through a trusted in-process
reference engine (a standard price-time model), diffing per aggressor order:

- **counterparty** (which resting order matched, by price-time priority) and
  **per-fill quantity / order outcome**: compared **exactly**;
- **execution price**: **tolerant** within the crossing band (accepts maker /
  aggressor / mid conventions).

The contract this enforces is [`matching-spec.md`](matching-spec.md).

### Three-state verdict (fail-safe)

The verdict is one of `correct | incorrect | unverified`. **`unverified` is the
default and the point of the third state**: any path that cannot conclusively
judge a submission resolves here, never to a silent pass. Sources of `unverified`:

- **No end-of-run marker** when the stream is finalized (`finalize` in
  `engine.go`) — the stream was truncated (e.g. a consumer-group rebalance) and a
  verdict on a partial replay is untrustworthy.
- **A fill with no captured intent**, or **a fill against a maker id never
  submitted** (`Replay` / `diffAggressor`) — lost telemetry or a response shape
  the grader could not parse. Returning `incorrect` here would risk failing correct
  code, so the sound answer is `unverified`.

The aggregator's per-submission default is `unverified` (never `correct`), so a
submission whose verdict is lost or never produced does not fail open to a pass.

### Verdict propagation

The verdict is emitted the moment it is decided (`aggregator.emitVerdict`),
decoupled from the metrics flush and carrying the last-known latency numbers.
This closes a prior gap where the verdict rode only on a metrics flush and would
be stranded whenever a submission had no new metrics in that window.

### Storage

`Score` carries the tri-state `Verdict`. The storage publisher writes the full
`status` string to Redis `leaderboard_state` (so consumers can distinguish
`unverified` from `incorrect`) plus a legacy `is_correct` / `correct` boolean
(`true` only when `status == correct`) for backward compatibility. Postgres
`leaderboard_metrics.is_correct` is likewise `true` only for a verified pass.

## bot-fleet threading model

- The FleetCommander runs one HTTP server (Boost.Asio, single `io_context`). A
  `POST /benchmark` replies `202` immediately and launches the run in a detached
  thread.
- `launch_benchmark` divides `num_bots` across `hardware_concurrency()` cores and
  creates **one `ThreadWorker` per core**.
- Each `ThreadWorker` runs on its own OS thread **pinned to a CPU core**
  (`pthread_setaffinity_np`) with its **own `io_context` (io_uring), lock-free
  counters, and audit log** — shared-nothing, zero mutexes.
- Within a worker, each bot is a **coroutine** multiplexed over io_uring async I/O
  (C10K style), not a thread. So 1000 bots on 8 cores ≈ 8 pinned threads ×
  ~125 coroutine-bots.

### Shared-fleet serialization

The bot-fleet is a **single shared service** that pins one worker per core, so two
overlapping runs contend for the same cores and pollute each other's latency
numbers. sandbox-manager therefore holds a **benchmark gate** (`benchGate`,
capacity `MAX_CONCURRENT_BENCHMARKS`, default **1**) for a run's whole span
(correctness + gap + performance), serializing runs for fair, reproducible
measurement. Increasing the capacity trades measurement fairness for throughput;
the sandbox for a queued submission may sit idle briefly while it waits for a slot.

## Reliability & idempotency

- `order_events` are deduped per submission by `seq`, so at-least-once redelivery
  cannot double-count into a false over-fill.
- The end-of-run marker gives a deterministic single finalization point (and its
  absence yields `unverified`, per above).
- telemetry-ingester's Redpanda producer uses a **background context** for async
  publishes, so the final correctness batch (orders + END marker) is never
  cancelled by the gRPC stream's EOF.

## Known limitations / roadmap

These are understood gaps, not silent ones; the fail-safe verdict keeps them from
turning into false passes/fails, but they are worth closing:

- **Transport-truncation detection needs an expected-count in the END marker.**
  Today a truncated stream is caught only when it drops the END marker or orphans
  a fill. A robust check is to carry the number of audit events the writer emitted
  on the END sentinel (proto `event_count` + C++ writer + Go compare) and mark
  `unverified` when fewer distinct events arrive. Deferred because it spans
  proto/C++/Go and needs a full rebuild + multi-consumer e2e to verify.
- **Consumer offset is `AtEnd`.** A consumer-group rebalance can skip in-flight
  correctness events; combined with the above, the result is `unverified` rather
  than a false pass, but manual commit-after-process (and a non-`AtEnd` reset for
  the order topic) would let a rebalanced consumer resume without loss.
- **Fixed grading seed = coverage ceiling.** Every contestant is graded on the same
  seed-42 stream. Broaden it (longer correctness window, a scripted deterministic
  stream, and/or multiple seeds) so grading exercises every check dimension —
  notably same-price FIFO counterparty selection, which the current seed may not
  hit.
- **Cold contestant build (~5 min)** dominates per-submission latency; a base image
  with build dependencies pre-installed would cut it substantially.
