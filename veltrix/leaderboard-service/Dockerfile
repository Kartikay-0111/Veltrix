# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM golang:1.22-bookworm AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
COPY templates/ templates/
RUN CGO_ENABLED=0 GOOS=linux go build -o leaderboard .

# ── Stage 2: Runtime (minimal) ────────────────────────────────────────────────
FROM gcr.io/distroless/static-debian12

WORKDIR /app
COPY --from=builder /app/leaderboard .
COPY --from=builder /app/templates templates/

EXPOSE 8085

ENV PORT=8085
ENV POSTGRES_HOST=postgres
ENV POSTGRES_PORT=5432
ENV POSTGRES_DB=iicpc_db
ENV POSTGRES_USER=iicpc
ENV POSTGRES_PASSWORD=iicpc_secret
ENV REDIS_HOST=redis
ENV REDIS_PORT=6379

CMD ["/app/leaderboard"]
