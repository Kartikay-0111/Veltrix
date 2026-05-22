# ── Stage 1: Build ───────────────────────────────────────────────────────────
FROM ubuntu:24.04 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    libboost-all-dev \
    liburing-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY CMakeLists.txt .
COPY include/ include/
COPY src/ src/

RUN cmake -B build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build --parallel $(nproc)

# ── Stage 2: Runtime (minimal image) ─────────────────────────────────────────
FROM ubuntu:24.04 AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    libboost-system1.83.0 \
    liburing2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /build/build/bot_fleet .

# Fleet Commander listens here for trigger from sandbox manager
EXPOSE 7070

ENV FLEET_LISTEN_PORT=7070
ENV TELEMETRY_INGESTER_HOST=telemetry-ingester
ENV TELEMETRY_INGESTER_PORT=8090

CMD ["./bot_fleet"]
