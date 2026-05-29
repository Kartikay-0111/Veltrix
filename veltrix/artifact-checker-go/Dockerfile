# ── Stage 1: Build the Go artifact checker ──────────────────────────────────
FROM golang:1.22-bookworm AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ cmd/
COPY internal/ internal/

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/artifact-checker \
    ./cmd/artifact-checker

# ── Stage 2: Small runtime image ─────────────────────────────────────────────
FROM alpine:3.20

RUN adduser -D -H -u 10001 artifact
USER artifact

WORKDIR /app
COPY --from=builder /out/artifact-checker /app/artifact-checker

ENV REDPANDA_BROKERS=redpanda:9092
ENV ORDER_EVENTS_TOPIC=order_events
ENV METRICS_TOPIC=order_metrics
ENV CONSUMER_GROUP=artifact-checker
ENV ALLOWED_LATENESS_MS=2000
ENV POSTGRES_HOST=postgres
ENV POSTGRES_PORT=5432
ENV POSTGRES_DB=iicpc_db
ENV POSTGRES_USER=iicpc
ENV POSTGRES_PASSWORD=iicpc_secret
ENV REDIS_HOST=redis
ENV REDIS_PORT=6379

CMD ["/app/artifact-checker"]
