-- ─────────────────────────────────────────────────────────────────────────────
--  Part 3 Addition: Leaderboard Metrics (TimescaleDB hypertable)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS leaderboard_metrics (
    time            TIMESTAMPTZ NOT NULL,
    team_id         VARCHAR(50)  NOT NULL,
    tps             INTEGER,
    p50_latency_ms  REAL,
    p90_latency_ms  REAL,
    p99_latency_ms  REAL,
    is_correct      BOOLEAN
);

SELECT create_hypertable(
    'leaderboard_metrics', 'time',
    chunk_time_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_metrics_team_time
    ON leaderboard_metrics(team_id, time DESC);
