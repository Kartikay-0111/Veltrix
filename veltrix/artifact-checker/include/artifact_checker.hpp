#pragma once
#include "hdr_histogram.hpp"
#include "correctness_engine.hpp"
#include "timescale_writer.hpp"

#include <librdkafka/rdkafkacpp.h>
#include <string>
#include <unordered_map>
#include <atomic>
#include <memory>
#include <chrono>
#include <optional>
#include <utility>

// ─────────────────────────────────────────────────────────────────────────────
// ArtifactChecker — Main orchestrator for Part 3
//
// Single-threaded design (intentional):
//   - One dedicated CPU core (pinned via Docker cpuset)
//   - Spin-poll loop: consume(0) = non-blocking, pegs CPU to 100%
//   - This guarantees microsecond reaction time when a new log arrives
//   - No context switching, no blocking, no waiting
//
// Every 500ms window from the bot fleet arrives as one Redpanda message.
// We receive messages from all 8 threads (8 messages per 500ms).
// We accumulate for 1 second (2 windows × 8 threads = 16 messages),
// then write one row to TimescaleDB.
//
// Per-submission state is tracked in SubmissionState.
// This allows multiple contests to run simultaneously.
// ─────────────────────────────────────────────────────────────────────────────

struct SubmissionState {
    HdrHistogram histogram;             // merged across all threads this second
    int64_t      successful_orders = 0; // http_200 count this second
    int64_t      total_orders      = 0; // all orders this second
    std::string  submission_id;
    std::string  sandbox_host;
    std::string  sandbox_port;
    bool         last_correct = true;
    bool         endpoint_missing_logged = false;
};

class ArtifactChecker {
public:
    ArtifactChecker(const std::string& redpanda_brokers,
                    const std::string& topic,
                    const std::string& consumer_group,
                    const std::string& db_conn_str,
                    const std::string& sandbox_host,
                    const std::string& sandbox_port);

    // Blocking — runs forever. Pin this to a dedicated CPU core.
    void run();

    // Signal graceful shutdown
    void stop() { running_ = false; }

private:
    std::atomic<bool>   running_{true};

    // ── Redpanda consumer ─────────────────────────────────────────────────────
    std::unique_ptr<RdKafka::KafkaConsumer> consumer_;
    std::string topic_;

    // ── Per-submission aggregation state ─────────────────────────────────────
    // Key: submission_id
    std::unordered_map<std::string, SubmissionState> states_;

    // ── Output sinks ──────────────────────────────────────────────────────────
    TimescaleWriter                                  db_writer_;
    std::string                                      default_sandbox_host_;
    std::string                                      default_sandbox_port_;

    // ── Timing ────────────────────────────────────────────────────────────────
    std::chrono::steady_clock::time_point last_db_write_;
    std::chrono::steady_clock::time_point last_correctness_check_;

    // ── Internal methods ──────────────────────────────────────────────────────

    // Process one Redpanda message
    void process_message(RdKafka::Message* msg);

    // Parse JSON payload from bot fleet / telemetry-ingester
    void parse_and_accumulate(const std::string& json);

    // Write all current states to TimescaleDB and reset
    void flush_to_db();

    // Resolve endpoint for a submission (prefers DB endpoint, falls back to container name)
    std::optional<std::pair<std::string, std::string>> resolve_endpoint(SubmissionState& state);

    // Run correctness check for a single submission
    void run_correctness_for_submission(SubmissionState& state);

    // Parse endpoint_url into host/port
    std::optional<std::pair<std::string, std::string>> parse_endpoint_url(const std::string& url) const;

    // Helper: extract JSON number/string fields
    double      extract_double(const std::string& json, const std::string& key);
    int64_t     extract_int64(const std::string& json,  const std::string& key);
    std::string extract_string(const std::string& json, const std::string& key);
    std::array<int64_t, 5> extract_hist(const std::string& json);
};
