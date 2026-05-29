#pragma once

#include <array>
#include <cmath>
#include <cstdint>
#include <string>
#include <chrono>

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

// ─────────────────────────────────────────────────────────────────────────────
// HdrLatencyHistogram — merge-friendly latency histogram
//
// The old 5 buckets were too coarse for p99 under HFT-ish loads: a response at
// 51ms and a timeout at 1000ms both landed in the same "50+" bucket. This
// histogram keeps a compact fixed-size array, but uses denser buckets near the
// sub-millisecond / low-millisecond range where good engines compete.
//
// The telemetry ingester accepts `hist: list[int]`, so this remains wire-format
// compatible with the existing POST /metrics endpoint while giving the Go
// artifact checker many more percentile cut points to merge.
// ─────────────────────────────────────────────────────────────────────────────
struct HdrLatencyHistogram
{
    static constexpr std::array<double, 18> UPPER_BOUNDS_MS = {
        0.050, 0.100, 0.250, 0.500,
        0.750, 1.000, 2.000, 3.000,
        5.000, 7.500, 10.000, 15.000,
        25.000, 50.000, 100.000, 250.000,
        500.000, 1000.000
    };

    std::array<int64_t, UPPER_BOUNDS_MS.size()> buckets = {};
    int64_t total_samples = 0;

    void record(double ms)
    {
        if (std::isnan(ms) || ms < 0.0)
            ms = 0.0;
        if (std::isinf(ms))
            ms = UPPER_BOUNDS_MS.back();

        for (std::size_t i = 0; i < UPPER_BOUNDS_MS.size(); ++i)
        {
            if (ms <= UPPER_BOUNDS_MS[i])
            {
                ++buckets[i];
                ++total_samples;
                return;
            }
        }

        ++buckets.back();
        ++total_samples;
    }

    void reset()
    {
        buckets.fill(0);
        total_samples = 0;
    }
};

// OrderEvent is no longer defined here — replaced by protobuf-generated types
// in telemetry.pb.h (OrderSubmitted + TradeExecuted).
// The audit log capture uses AuditLog (see audit_log.hpp).

struct TelemetryCounters
{
    std::array<int64_t, EVENT_COUNT> counts = {};
    double total_latency_ms = 0.0;
    int64_t latency_samples = 0;
    HdrLatencyHistogram histogram;

    void record_latency(double ms)
    {
        if (std::isnan(ms) || ms < 0.0)
            ms = 0.0;
        if (std::isinf(ms))
            ms = HdrLatencyHistogram::UPPER_BOUNDS_MS.back();

        total_latency_ms += ms;
        ++latency_samples;
        histogram.record(ms);
    }

    void reset()
    {
        counts.fill(0);
        histogram.reset();
        total_latency_ms = 0.0;
        latency_samples = 0;
    }
};

