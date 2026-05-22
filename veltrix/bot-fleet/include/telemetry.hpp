#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <boost/asio.hpp>

// ─────────────────────────────────────────────────────────────────────────────
// Telemetry — Lock-Free Per-Thread Metrics
//
// Each thread owns one TelemetryCounters instance. No thread ever touches
// another thread's counters. This is why there are zero locks.
// ─────────────────────────────────────────────────────────────────────────────

enum TelemetryEvent : int
{
    HTTP_200 = 0,
    HTTP_4XX = 1,
    HTTP_5XX = 2,
    TIMEOUT = 3,
    ECONNREF = 4,
    OTHER_ERR = 5,
    EVENT_COUNT = 6
};

struct TelemetryCounters
{
    std::array<int64_t, EVENT_COUNT> counts = {};
    double total_latency_ms = 0.0;
    int64_t latency_samples = 0;
    std::array<int64_t, 5> latency_buckets = {};

    void record_latency(double ms)
    {
        total_latency_ms += ms;
        ++latency_samples;
        if (ms < 1.0)
            ++latency_buckets[0];
        else if (ms < 5.0)
            ++latency_buckets[1];
        else if (ms < 10.0)
            ++latency_buckets[2];
        else if (ms < 50.0)
            ++latency_buckets[3];
        else
            ++latency_buckets[4];
    }

    void reset()
    {
        counts.fill(0);
        latency_buckets.fill(0);
        total_latency_ms = 0.0;
        latency_samples = 0;
    }
};

namespace asio = boost::asio;
using tcp = asio::ip::tcp;

class TelemetryProducer
{
public:
    TelemetryProducer(std::string host, std::string port);
    ~TelemetryProducer() = default;

    void flush(const TelemetryCounters &counters,
               const std::string &submission_id,
               int thread_id);

    TelemetryProducer(const TelemetryProducer &) = delete;
    TelemetryProducer &operator=(const TelemetryProducer &) = delete;

private:
    std::string host_;
    std::string port_;
};
