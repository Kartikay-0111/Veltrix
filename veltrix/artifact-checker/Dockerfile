# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM ubuntu:24.04 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    pkg-config \
    librdkafka-dev \
    libpqxx-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY CMakeLists.txt .
COPY include/ include/
COPY src/ src/

RUN cmake -B build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build --parallel $(nproc)

# ── Stage 2: Runtime ─────────────────────────────────────────────────────────
FROM ubuntu:24.04 AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka++1 \
    libpqxx-7.8t64 \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /build/build/artifact_checker .

# ── Environment defaults (overridden by docker-compose) ───────────────────────
ENV REDPANDA_BROKERS=redpanda:9092
ENV METRICS_TOPIC=order_metrics
ENV CONSUMER_GROUP=artifact-checker
ENV POSTGRES_HOST=postgres
ENV POSTGRES_PORT=5432
ENV POSTGRES_DB=iicpc_db
ENV POSTGRES_USER=iicpc
ENV POSTGRES_PASSWORD=iicpc_secret
ENV SANDBOX_HOST=localhost
ENV SANDBOX_PORT=9999

# CPU core 7 is pinned via Docker cpuset in docker-compose, not here
CMD ["./artifact_checker"]