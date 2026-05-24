#pragma once
#include <pqxx/pqxx>
#include <optional>
#include <string>

// ─────────────────────────────────────────────────────────────────────────────
// TimescaleWriter — Writes one row per second to leaderboard_metrics
//
// Why one row per second?
//   The artifact checker aggregates ALL bot data from ALL threads over a
//   1-second window. One INSERT per second is 3,600 rows per hour —
//   trivial for TimescaleDB. The Leaderboard service (Part 4) reads the
//   latest row per team to display live rankings.
//
// Why libpqxx?
//   - Official C++ wrapper for PostgreSQL/TimescaleDB
//   - No linker issues (unlike rdkafka)
//   - Prepared statements prevent SQL injection
//   - Connection pooling handled internally
// ─────────────────────────────────────────────────────────────────────────────

struct MetricRow {
    std::string team_id;        // submission_id acts as team identifier
    int         tps;            // successful orders per second
    double      p50_latency_ms;
    double      p90_latency_ms;
    double      p99_latency_ms;
    bool        is_correct;
};

class TimescaleWriter {
public:
    explicit TimescaleWriter(const std::string& connection_string);

    // Insert one row into leaderboard_metrics.
    // Called once per second by the artifact checker main loop.
    void write(const MetricRow& row);

    // Verify connection is alive, reconnect if needed
    bool ping();

    // Lookup sandbox endpoint URL for a submission id.
    std::optional<std::string> get_endpoint_url(const std::string& submission_id);

private:
    std::string                         conn_str_;
    std::unique_ptr<pqxx::connection>   conn_;

    void ensure_connected();
    void prepare_statement();
};
