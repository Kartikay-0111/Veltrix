# Veltrix — Part 1: Submission & Sandboxing Engine Architecture

## Overview
This document outlines the detailed architecture for **Part 1** of **Veltrix**, a high-performance distributed benchmarking platform. The Submission & Sandboxing Engine is responsible for the secure ingestion, storage, orchestration, and isolated execution of untrusted contestant-submitted trading engines (e.g., C++, Rust, Go). 

This document serves as a complete blueprint for implementing the Part 1 pipeline from scratch.

---

## 1. Core Technologies
* **API Gateway & Web Server:** Python (FastAPI) - Used for asynchronous file chunking and orchestration.
* **Object Storage:** MinIO (Local) / AWS S3 (Cloud) - Fast, flat, stateless storage for contestant binaries.
* **Relational Database:** PostgreSQL - State machine tracking and metadata storage.
* **Message Broker / State Queue:** Redis - Prevents race conditions during port allocation and event triggering.
* **Container Orchestration:** Docker Engine SDK (Local) transitioning to Kubernetes Python Client (Cloud).
* **Resource Management:** Linux cgroups and network namespaces via container runtime configurations.

---

## 2. Directory Structure
To ensure separation of concerns between the API, orchestration, and infrastructure, the repository is structured as follows:

```text
/veltrix
├── /db
│   └── init.sql              # Database schema initialization
├── /infra                    # Future Terraform/K8s manifests
├── /sandbox-manager          # Docker/K8s Orchestration Service
│   ├── Dockerfile
│   ├── main.py
│   └── requirements.txt
├── /submission-service       # FastAPI Ingestion & Storage Service
│   ├── Dockerfile
│   ├── main.py
│   └── requirements.txt
├── .env                      # Environment variables
└── docker-compose.yml        # Local infrastructure (PostgreSQL, MinIO, Redis)
```

## 3. The Execution Pipeline (Step-by-Step Flow)

### Phase 1: Ingestion & Storage (submission-service)
- **Upload Initiation:** A contestant uploads a ZIP file via the frontend.
- **Secure Streaming:** The frontend sends the file to the FastAPI /submission-service.
- **Async Chunking:** FastAPI streams the upload chunk-by-chunk directly into MinIO via the boto3 library to prevent memory overflow (RAM crashes) on the API server.
- **State Initialization:** Upon successful MinIO storage, the service writes a row to PostgreSQL using SQLAlchemy (Status: PENDING).
- **Event Trigger:** The service pushes the submission ID to a Redis queue to wake up the Sandbox Manager safely.

### Phase 2: Orchestration & Isolation (sandbox-manager)
- **Job Retrieval:** The Python Sandbox Manager pulls the PENDING job ID and retrieves the corresponding ZIP from MinIO.
- **Secure Extraction:** The manager extracts the ZIP, enforcing strict limits on file sizes and sanitizing paths to prevent directory traversal attacks.
- **Dynamic Orchestration:** Using the Docker SDK, the manager provisions a container with strict security constraints:
	- **Memory Limit (`mem_limit`):** e.g., 512MB (Linux cgroups).
	- **CPU Pinning (`nano_cpus`):** Limited to exactly 1 CPU core.
	- **Network Namespace:** Disabled or restricted to internal VPC routing (`network_mode="none"`) to prevent outbound internet access.
	- **Networking Strategy:** Utilize Docker Ephemeral Ports (mapping the container's internal port to a random high host port).

### Phase 3: Validation & Readiness
- **Active TCP Health Check:** The Sandbox Manager executes a polling loop, attempting a direct TCP socket connection to the assigned host port.
- **Timeout Cap:** The loop polls every 0.5s with a hard cap of 10 seconds.
- **Success:** Connection established. Database status updates to READY.
- **Failure:** 10 seconds expire. Database status updates to FAILED_STARTUP.

### Phase 4: Teardown & State Resolution
Once the external Bot Fleet finishes the stress test, the Sandbox Manager parses the container's exit codes to provide precise feedback (e.g., SUCCESS, FAILED_LOGIC for exit code 1, FAILED_RESOURCE for OOM exit code 137). The container is then forcefully killed and removed to reclaim resources.

## 4. Cloud Translation (K8s Readiness)
The architecture natively translates to Kubernetes:

- MinIO swaps seamlessly to AWS S3.
- PostgreSQL shifts to a managed RDS instance.
- Docker SDK scripts are replaced with the Kubernetes Python Client to spin up isolated Pods instead of local containers.