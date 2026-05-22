## Part 2: The Distributed Bot Fleet & Concurrency Engine

### 1. System Overview

The **Distributed Bot Fleet** is a high-performance C++20 engine designed to stress-test untrusted trading algorithms (orderbooks/matching engines) submitted by contestants. It operates with microsecond precision, simulating severe market volatility by generating tens of thousands of concurrent network connections without garbage collection pauses or thread-locking overhead.

---

### 2. The Execution Flow (Trigger Pipeline)

The system operates sequentially to ensure contestant code is safely sandboxed and fully booted before the load test begins.

```text
[Team submits code via POST /submit]
          ↓
[Submission Service] → Saves to MinIO + Updates Postgres State (PENDING)
          ↓
[Sandbox Manager (Python)] → Pulls job via Task Queue → Builds Image → docker run
          ↓
[Health Check] → Waits for container port to open → Marks state READY
          ↓
[Command & Control] → POST http://bot-fleet:7070/benchmark (The Trigger)
          ↓
[C++ Fleet Commander] → Detects N CPU Cores
          ↓
[Concurrency Engine] → Divides 10,000 bots evenly across N threads (e.g., 1,250 bots per core)
          ↓
[The Hot Path] → Each thread runs an io_uring event loop + C++20 coroutines
          ↓
[Telemetry Flush] → Every 500ms, asynchronously flushes metrics to Redpanda topic: `order_metrics`

```

---

### 3. Directory Structure

```text
veltrix/
├── bot-fleet/
│   ├── include/
│   │   ├── bot_payload.hpp      ← Abstract base class (the contract/interface)
│   │   ├── rest_bot.hpp         ← Concrete implementation for REST order generation
│   │   ├── telemetry.hpp        ← Lock-free counters + Redpanda producer logic
│   │   ├── thread_worker.hpp    ← One per CPU core, owns the private io_uring context
│   │   └── fleet_commander.hpp  ← HTTP server, receives the external trigger
│   ├── src/
│   │   ├── main.cpp             ← Entry point
│   │   ├── rest_bot.cpp
│   │   ├── telemetry.cpp
│   │   ├── thread_worker.cpp    ← The hot path (io_uring + coroutines)
│   │   └── fleet_commander.cpp
│   ├── CMakeLists.txt
│   └── Dockerfile               ← 2-stage build (compile + minimal runtime)
├── sandbox-manager/main.py      ← Triggers the fleet commander
└── docker-compose.yml           ← Provisions the infrastructure (Redpanda, Postgres, Bot Fleet)

```

---

### 4. Core Architectural Decisions (The "Why")

#### A. Concurrency Model: Thread-per-Core (Shared-Nothing)

* **Decision:** We rejected traditional Thread Pools and Mutex locks. If the server has 8 cores, we spawn exactly 8 threads.
* **Rationale:** Context switching between threads and waiting on locks destroys microsecond latency. By pinning exactly one thread per core and giving it a private array of bots, the CPU never stops calculating.
* **Network I/O:** Uses **`io_uring`** via Boost.Asio and C++20 Coroutines (`co_await`). The thread asks the Linux kernel to send data and immediately yields to process the next bot, returning only when the kernel signals the network response has arrived.

#### B. Protocol Abstraction: The Modularity Engine

* **Decision:** Bot mechanics are abstracted using OOP principles.
* **Rationale:** Allows the engine to swap between testing REST, WebSockets, or FIX protocols without rewriting the core loop.
* **Implementation:** * `BotPayload` (Abstract Base Class) with a `virtual ~BotPayload() = default;` to prevent memory leaks.
* A pure virtual function `virtual void generate_request() = 0;` forcing child classes (like `RestBot`) to implement the specific network payload building.



#### C. Telemetry: Lock-Free Aggregation

* **Decision:** Recording latencies and errors natively without blocking.
* **Rationale:** Writing to disk or sending network packets on every trade ruins throughput.
* **Implementation:** Each thread owns a private, contiguous `std::array<int, 6>`. Events are mapped to an `enum` (e.g., `HTTP_200 = 0`, `TIMEOUT = 1`). Updating a metric (`counters[TIMEOUT]++`) takes a single, lock-free CPU instruction.

---

### 5. Resilience & Edge Cases (The "Doomsday" Scenarios)

To guarantee fairness and absolute stability during the hackathon, the architecture proactively defends against three critical failure modes:

#### Scenario 1: The Thundering Herd (Queue Collapse)

* **The Threat:** 5,000 users submit code right before the deadline. Attempting to run 5,000 Docker containers simultaneously will crash the host OS (OOM / File Descriptor exhaustion).
* **The Solution (Worker Pool / Task Queue):** FastAPI simply logs submissions as `PENDING` in PostgreSQL. Python Background Workers pull a strictly controlled batch of jobs (e.g., 10 at a time) using the `SELECT ... FOR UPDATE SKIP LOCKED` SQL command to ensure workers never collide, protecting the Docker Daemon from overload.

#### Scenario 2: The Noisy Neighbor (Resource Exhaustion)

* **The Threat:** A contestant submits malicious code (e.g., a fork bomb) or a massive memory leak. Because it runs on the same physical server as the C++ bots, it hogs 100% of the CPU, starving the benchmark engine and ruining latency scores.
* **The Solution (Linux Cgroups):** The Python Sandbox Manager strictly limits container allocation using Docker SDK flags (`mem_limit="512m"`, `nano_cpus=1000000000`). If a contestant exceeds 1 Core or 512MB RAM, the Linux OOM-killer violently terminates their container, logging a `FAILED` state while leaving the C++ Bot Fleet entirely unaffected.

#### Scenario 3: The Telemetry Avalanche (Pipeline Saturation)

* **The Threat:** Every 500ms, the fast C++ `io_uring` threads attempt to flush telemetry arrays to Redpanda. If the network experiences a brief 100ms lag, the fast threads will block on the TCP socket. The bots stop sending orders, artificially spiking the $p99$ latency measurements.
* **The Solution (Producer-Consumer + Pub/Sub):**
* **In-Memory (Ring Buffer):** The fast `io_uring` thread (Producer) drops the telemetry JSON into a Lock-Free Ring Buffer in RAM and immediately returns to processing bots. A separate, dedicated Network Thread (Consumer) reads from the buffer and manages the slow network upload.
* **Over the Network (Message Broker):** We use a Pub/Sub pattern via Redpanda. Bots only publish messages to the Broker. Downstream services (Leaderboard, Validation Engine) subscribe to the Broker. This completely decouples the ultra-fast execution engine from slow or crashing downstream services.