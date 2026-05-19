# Veltrix — Benchmarking at Scale

## Overview
**Veltrix** is a highly concurrent, distributed benchmarking and hosting platform built for the IICPC Summer Hackathon 2026. Designed for hardcore systems engineering, Veltrix evaluates contestant-submitted, high-frequency trading infrastructure (e.g., simulated orderbooks, matching engines). 

Drawing architectural inspiration from massive open-source testing suites like DataStax's *Fallout*, Veltrix securely hosts untrusted code and bombards it with a distributed fleet of trading bots. It accurately captures granular telemetry without skewing target performance, ensuring a fair, precise, and highly scalable competition.

## Key Objectives
* **Simulate Peak Market Volatility:** Generate massive, concurrent order traffic (FIX, REST, WebSockets) via a distributed load generator.
* **Precision Telemetry:** Accurately measure p50, p90, and p99 latencies using High Dynamic Range (HDR) Histograms, alongside maximum throughput (TPS).
* **Correctness Validation:** Verify price-time priority and trade fill accuracy without data loss.
* **Strict Security & Fairness:** Run untrusted contestant code in strictly isolated environments with identical CPU/Memory constraints.

---

## System Architecture: The 4 Core Components

Veltrix is heavily decoupled into four specialized sub-systems to ensure microsecond-level precision and horizontal scalability.

### Part 1: Submission & Sandboxing Engine
*The secure ingestion and orchestration pipeline.*
* **Tech Stack:** Python (FastAPI), Docker SDK / Kubernetes, MinIO (S3), PostgreSQL, Redis.
* **Function:** Contestants upload their code. The FastAPI backend securely chunks the upload to MinIO. The Sandbox Manager dynamically provisions highly isolated containers/Pods, enforces strict resource limits, executes the code, and utilizes active TCP health checks to verify readiness.

### Part 2: Distributed Bot Fleet
*The highly concurrent load generators.*
* **Tech Stack:** C++20, gRPC.
* **Function:** Once a sandbox is `READY`, a vast fleet of stateless bots awakens. Written in C++ to avoid Garbage Collection (GC) pauses, these bots bombard the contestant's predefined endpoints based on the modular test configuration.

### Part 3: Telemetry & Correctness Ingester
*The low-latency tracking pipeline.*
* **Tech Stack:** C++20, Redpanda (Kafka-compatible), TimescaleDB, HdrHistogram.
* **Function:** Bots record latencies locally using HDR Histograms and stream logs via Redpanda into a dedicated Artifact Checker. The checker merges logs to calculate true p99 latencies and validates raw transaction logs for price-time priority accuracy.

### Part 4: Real-Time Leaderboard
*The live analytics interface.*
* **Tech Stack:** Next.js, Redis (Sorted Sets), WebSockets.
* **Function:** Consumes aggregated telemetry from the TimescaleDB/Redpanda pipeline and pushes live metrics via WebSockets to a Next.js dashboard, utilizing Redis Sorted Sets for $O(\log n)$ dynamic rankings.

---

## Deployment & IaC
Veltrix is designed from the ground up for cloud deployment:
* **Local Development:** Orchestrated via `docker-compose`.
* **Cloud Infrastructure:** Kubernetes (K8s) for distributed container orchestration.
* **Infrastructure as Code (IaC):** Terraform scripts to automate the provisioning of VPCs, Subnets, Kubernetes clusters, and storage buckets.