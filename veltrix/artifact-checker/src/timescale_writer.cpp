#include "timescale_writer.hpp"
#include <iostream>
#include <stdexcept>

TimescaleWriter::TimescaleWriter(const std::string &connection_string)
    : conn_str_(connection_string)
{
    ensure_connected();
    std::cout << "[TimescaleWriter] Connected to TimescaleDB\n";
}

void TimescaleWriter::write(const MetricRow &row)
{
    ensure_connected();

    try
    {
        pqxx::work txn(*conn_);

        // NOW() in TimescaleDB = current timestamp → partitioned into the
        // correct hypertable chunk automatically
        txn.exec_params(
            R"(
                INSERT INTO leaderboard_metrics
                    (time, team_id, tps, p50_latency_ms, p90_latency_ms,
                     p99_latency_ms, is_correct)
                VALUES
                    (NOW(), $1, $2, $3, $4, $5, $6)
            )",
            row.team_id,
            row.tps,
            row.p50_latency_ms,
            row.p90_latency_ms,
            row.p99_latency_ms,
            row.is_correct);

        txn.commit();
    }
    catch (const pqxx::broken_connection &e)
    {
        std::cerr << "[TimescaleWriter] Connection broken, reconnecting: "
                  << e.what() << "\n";
        conn_.reset(); // force reconnect on next call
        ensure_connected();
    }
    catch (const std::exception &e)
    {
        std::cerr << "[TimescaleWriter] Write failed: " << e.what() << "\n";
    }
}

bool TimescaleWriter::ping()
{
    try
    {
        if (!conn_ || conn_->is_open() == false)
            return false;
        pqxx::work txn(*conn_);
        txn.exec("SELECT 1");
        txn.commit();
        return true;
    }
    catch (...)
    {
        return false;
    }
}

std::optional<std::string> TimescaleWriter::get_endpoint_url(const std::string &submission_id)
{
    ensure_connected();

    try
    {
        pqxx::work txn(*conn_);
        auto res = txn.exec_params(
            "SELECT endpoint_url FROM submissions WHERE id = $1",
            submission_id);
        txn.commit();

        if (res.empty() || res[0][0].is_null())
            return std::nullopt;

        return res[0][0].as<std::string>();
    }
    catch (const std::exception &e)
    {
        std::cerr << "[TimescaleWriter] Endpoint lookup failed: " << e.what() << "\n";
        return std::nullopt;
    }
}

void TimescaleWriter::ensure_connected()
{
    if (conn_ && conn_->is_open())
        return;

    try
    {
        conn_ = std::make_unique<pqxx::connection>(conn_str_);
    }
    catch (const std::exception &e)
    {
        throw std::runtime_error(
            std::string("[TimescaleWriter] Cannot connect: ") + e.what());
    }
}
