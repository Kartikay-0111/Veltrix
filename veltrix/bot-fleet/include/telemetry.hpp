#pragma once
#include <array>
#include <atomic>
#include <string>
#include <cstdint>
#include <chrono>
#include <librdkafka/rdkafka.h>

// ─────────────────────────────────────────────────────────────────────────────
// Telemetry — Lock-Free Per-Thread Metrics
//
// Each thread owns ONE TelemetryCounters instance. No thread ever touches
// another thread's counters. This is why there are zero locks.
//
// The enum maps events to array indices — O(1) increment, no hashing,
// no heap allocation, fits entirely in a single cache line (6 * 8 bytes = 48b)
// ─────────────────────────────────────────────────────────────────────────────

// ── Event index mapping ───────────────────────────────────────────────────────
enum TelemetryEvent : int {
    HTTP_200   = 0,   // successful order acknowledgment
    HTTP_4XX   = 1,   // client error (bad request from bot — our bug)
    HTTP_5XX   = 2,   // server error — contestant's code crashed
    TIMEOUT    = 3,   // socket timed out — contestant too slow
    ECONNREF   = 4,   // connection refused — sandbox not listening
    OTHER_ERR  = 5,   // anything else
    EVENT_COUNT = 6   // always last — array size sentinel
};

// ── Per-thread counters (lives on the stack, zero heap) ───────────────────────
struct TelemetryCounters {
    std::array<int64_t, EVENT_COUNT> counts = {};   // zero-initialized
    double total_latency_ms = 0.0;
    int64_t latency_samples = 0;

    // p99 approximation via histogram buckets (0-1ms, 1-5ms, 5-10ms, 10-50ms, 50ms+)
    std::array<int64_t, 5> latency_buckets = {};

    void record_latency(double ms) {
        total_latency_ms += ms;
        ++latency_samples;
        if      (ms < 1.0)  ++latency_buckets[0];
        else if (ms < 5.0)  ++latency_buckets[1];
        else if (ms < 10.0) ++latency_buckets[2];
        else if (ms < 50.0) ++latency_buckets[3];
        else                ++latency_buckets[4];
    }

    void reset() {
        counts.fill(0);
        latency_buckets.fill(0);
        total_latency_ms = 0.0;
        latency_samples  = 0;
    }
};

// ── Redpanda producer (one per process, thread-safe) ─────────────────────────
class TelemetryProducer {
public:
    explicit TelemetryProducer(const std::string& brokers,
                                const std::string& topic);
    ~TelemetryProducer();

    // Called every 500ms by the async timer in each thread worker.
    // Serialises counters to JSON and produces to Redpanda.
    void flush(const TelemetryCounters& counters,
               const std::string&       submission_id,
               int                      thread_id);

    // Non-copyable — one producer per process
    TelemetryProducer(const TelemetryProducer&)            = delete;
    TelemetryProducer& operator=(const TelemetryProducer&) = delete;

private:
    rd_kafka_t*       rk_;    // Kafka producer handle
    rd_kafka_topic_t* rkt_;   // Topic handle
    std::string       topic_;
};
