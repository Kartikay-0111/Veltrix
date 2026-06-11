# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/sandbox-manager \
    ./cmd/main.go

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
# Note: sandbox-manager must run as root to access /var/run/docker.sock.
# The socket volume mount in docker-compose grants Docker daemon access.
FROM alpine:3.20

RUN apk add --no-cache ca-certificates

WORKDIR /app
COPY --from=builder /out/sandbox-manager .

EXPOSE 8081

ENV HEALTH_PORT=8081 \
    CONFIG_WORKER_COUNT=10

CMD ["/app/sandbox-manager"]