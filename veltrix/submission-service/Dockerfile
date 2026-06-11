# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

# Install git for go mod download of VCS-pinned deps
RUN apk add --no-cache git

WORKDIR /src

# Layer-cache go.mod before copying source so re-builds only re-fetch
# dependencies when go.mod/go.sum actually change.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/submission-service \
    ./cmd/main.go

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM alpine:3.20

# ca-certificates is required for TLS connections to MinIO/Postgres.
RUN apk add --no-cache ca-certificates

RUN adduser -D -H -u 10001 appuser
USER appuser

WORKDIR /app
COPY --from=builder /out/submission-service .

EXPOSE 8080

ENV PORT=8080

CMD ["/app/submission-service"]