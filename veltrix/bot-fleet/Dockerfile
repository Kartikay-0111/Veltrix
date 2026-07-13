# ── Stage 1: Build ───────────────────────────────────────────────────────────
FROM ubuntu:24.04 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    pkg-config \
    libboost-all-dev \
    liburing-dev \
    libgrpc++-dev \
    libprotobuf-dev \
    protobuf-compiler-grpc \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy the shared proto schema
COPY proto/ /proto/

# Copy the bot-fleet source (context is veltrix root)
COPY bot-fleet/CMakeLists.txt .
COPY bot-fleet/include/ include/
COPY bot-fleet/src/ src/

# The CMakeLists references PROTO_DIR as "../proto" relative to CMAKE_SOURCE_DIR.
# Override it to point to /proto where we copied the shared schema.
RUN cmake -B build -DCMAKE_BUILD_TYPE=Release \
        -DPROTO_DIR=/proto \
    && cmake --build build --parallel $(nproc)

# ── Stage 2: Runtime (minimal image) ─────────────────────────────────────────
FROM ubuntu:24.04 AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    libboost-system1.83.0 \
    libboost-json1.83.0 \
    liburing2 \
    libgrpc++1.51t64 \
    libprotobuf32 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /build/build/bot_fleet .

# Fleet Commander listens here for trigger from sandbox manager
EXPOSE 7070

ENV FLEET_LISTEN_PORT=7070
ENV TELEMETRY_GRPC_TARGET=telemetry-ingester:8091

CMD ["./bot_fleet"]
