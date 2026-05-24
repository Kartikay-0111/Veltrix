# Veltrix - Part 3: Artifact Checker and Metrics Pipeline

## Overview
This document describes Part 3 of Veltrix: the Artifact Checker and the metrics pipeline. The service consumes telemetry from Redpanda, aggregates per-submission latency histograms, runs periodic correctness checks, and writes the leaderboard rows into TimescaleDB.

---

## 1. Core Components
- **Redpanda (Kafka API):** `order_metrics` topic delivers bot-fleet telemetry.
- **Artifact Checker (C++20):** spin-poll consumer pinned to a dedicated CPU core.
- **HdrHistogram:** computes p50/p90/p99 from merged buckets.
- **Correctness Engine:** injects a watermark order and verifies the orderbook snapshot.
- **TimescaleDB (Postgres):** stores both leaderboard rows and raw telemetry.

---

## 2. Execution Flow

```text
Redpanda (order_metrics topic)
    -> spin-poll consume(0) pinned to CPU 7
    -> parse_and_accumulate()
    -> merge histograms across all threads per submission
    -> every 1s: compute p50/p90/p99 and TPS
    -> every 10s: CorrectnessEngine.run_check()
         - POST /order (0.01 watermark)
         - wait 200ms
         - GET /book/AAPL
         - ShadowOrderbook.verify_snapshot()
    -> TimescaleWriter.write() (one row per second)
    -> leaderboard_metrics table in TimescaleDB
```

---

## 3. Directory Structure

```text
veltrix/artifact-checker/
├── include/
│   ├── hdr_histogram.hpp       # HDR histogram for p50/p90/p99
│   ├── shadow_orderbook.hpp     # std::multiset based shadow orderbook
│   ├── correctness_engine.hpp   # watermark injection + GET /book verification
│   ├── timescale_writer.hpp     # libpqxx TimescaleDB writer
│   └── artifact_checker.hpp     # main orchestrator
├── src/
│   ├── hdr_histogram.cpp
│   ├── shadow_orderbook.cpp
│   ├── correctness_engine.cpp
│   ├── timescale_writer.cpp
│   ├── artifact_checker.cpp
│   └── main.cpp
├── CMakeLists.txt
└── Dockerfile
```

---

## 4. Data Model (TimescaleDB)

### leaderboard_metrics (per second, per submission)
- `time` (TIMESTAMPTZ)
- `team_id`
- `tps`
- `p50_latency_ms`
- `p90_latency_ms`
- `p99_latency_ms`
- `is_correct`

### telemetrics (raw telemetry, per flush)
- `time` (TIMESTAMPTZ)
- `ts_epoch_ms`
- `submission_id`
- `thread_id`
- `http_200`, `http_4xx`, `http_5xx`
- `timeout`, `econnref`, `other_err`
- `avg_latency_ms`, `samples`, `hist`

---

## 5. Operational Notes
- **CPU isolation:** the Artifact Checker is pinned to core 7 to avoid contention with the bot fleet.
- **Spin-poll design:** `consume(0)` never blocks, guaranteeing microsecond reaction time.
- **Write cadence:** one TimescaleDB insert per second, per submission.
- **Correctness cadence:** every 10 seconds, using a watermark order and snapshot verification.
