-- db/leaderboard_metrics.sql
-- Plain PostgreSQL table — TimescaleDB hypertable removed.
-- All metadata and leaderboard time-series now live in vanilla Postgres.
CREATE TABLE IF NOT EXISTS leaderboard_metrics (
    time            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    team_id         VARCHAR(50)  NOT NULL,
    tps             INTEGER,
    p50_latency_ms  REAL,
    p90_latency_ms  REAL,
    p99_latency_ms  REAL,
    is_correct      BOOLEAN
);

-- BRIN index is efficient for append-heavy time-series on plain Postgres.
CREATE INDEX IF NOT EXISTS idx_metrics_team_time
    ON leaderboard_metrics(team_id, time DESC);