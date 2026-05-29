# ── Stage 1: Build the Go telemetry ingester ─────────────────────────────────
# Uses protoc to generate Go code from the shared proto schema, then compiles.
FROM golang:1.25-bookworm AS builder

# Install protoc and Go plugins for code generation
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.2 \
    && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.4.0

WORKDIR /src

# Copy the shared proto schema
COPY proto/ /proto/

# Copy Go module files first for layer caching
COPY telemetry-ingester/go.mod telemetry-ingester/go.sum ./
RUN go mod download

# Copy source
COPY telemetry-ingester/main.go .
COPY telemetry-ingester/internal/ internal/

# Generate protobuf Go code (replace hand-written stubs with real codegen)
RUN rm -rf internal/pb/*.go && \
    mkdir -p internal/pb && \
    protoc --proto_path=/proto \
      --go_out=internal/pb --go_opt=paths=source_relative \
      --go-grpc_out=internal/pb --go-grpc_opt=paths=source_relative \
      /proto/telemetry.proto

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/telemetry-ingester \
    .

# ── Stage 2: Small runtime image ─────────────────────────────────────────────
FROM alpine:3.20

RUN apk add --no-cache wget  # for healthcheck
RUN adduser -D -H -u 10001 ingester
USER ingester

WORKDIR /app
COPY --from=builder /out/telemetry-ingester /app/telemetry-ingester

EXPOSE 8090 8091

ENV GRPC_PORT=8091
ENV HTTP_PORT=8090
ENV REDPANDA_BROKERS=redpanda:9092
ENV ORDER_EVENTS_TOPIC=order_events
ENV ORDER_METRICS_TOPIC=order_metrics

CMD ["/app/telemetry-ingester"]
