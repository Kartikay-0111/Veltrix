#pragma once
#include <array>
#include <cstdint>
#include <cmath>

// ─────────────────────────────────────────────────────────────────────────────
// HdrHistogram — Merge-able latency histogram
//
// The bot fleet sends 5-bucket histogram data per 500ms flush window.
// The artifact checker receives messages from ALL 8 threads and merges them.
//
// Why merge-able?
//   Thread 0 saw: [200, 180, 50, 20, 3]  (453 samples)
//   Thread 1 saw: [190, 175, 60, 18, 2]  (445 samples)
//   Merged:       [390, 355, 110, 38, 5] (898 samples)
//
//   φ(a ∪ b) = φ(a) + φ(b)  — this property is why HDR histograms are used
//   in distributed systems. A simple average of averages would be WRONG.
//
// Bucket boundaries (ms):
//   [0]  0 – 1 ms     (ultra-fast responses)
//   [1]  1 – 5 ms     (fast)
//   [2]  5 – 10 ms    (acceptable)
//   [3]  10 – 50 ms   (slow)
//   [4]  50+ ms       (failing / timeout territory)
// ─────────────────────────────────────────────────────────────────────────────

struct HdrHistogram {
    static constexpr int    NUM_BUCKETS = 5;
    static constexpr double BOUNDARIES[NUM_BUCKETS + 1] = {
        0.0, 1.0, 5.0, 10.0, 50.0, 1000.0
    };

    std::array<int64_t, NUM_BUCKETS> buckets = {};
    int64_t total_samples = 0;

    // ── Merge another histogram in (element-wise add) ─────────────────────────
    void merge(const HdrHistogram& other) {
        for (int i = 0; i < NUM_BUCKETS; ++i)
            buckets[i] += other.buckets[i];
        total_samples += other.total_samples;
    }

    // ── Load from the 5-element array the bot fleet sends ────────────────────
    void load(const std::array<int64_t, 5>& bot_buckets, int64_t samples) {
        for (int i = 0; i < NUM_BUCKETS; ++i)
            buckets[i] += bot_buckets[i];
        total_samples += samples;
    }

    // ── Percentile via linear interpolation within bucket ─────────────────────
    // Returns the estimated latency in ms at the given percentile (0-100).
    double percentile(double pct) const {
        if (total_samples == 0) return 0.0;

        double target = (pct / 100.0) * static_cast<double>(total_samples);
        int64_t cumulative = 0;

        for (int i = 0; i < NUM_BUCKETS; ++i) {
            int64_t prev = cumulative;
            cumulative += buckets[i];

            if (cumulative >= static_cast<int64_t>(target)) {
                // Target percentile falls in this bucket — interpolate linearly
                double bucket_start = BOUNDARIES[i];
                double bucket_end   = BOUNDARIES[i + 1];
                double bucket_range = bucket_end - bucket_start;

                // Avoid division by zero for the overflow bucket
                if (buckets[i] == 0) return bucket_start;

                // How far into this bucket does the percentile fall?
                double fraction = (target - static_cast<double>(prev))
                                / static_cast<double>(buckets[i]);

                return bucket_start + fraction * bucket_range;
            }
        }

        // All samples are in the last bucket — return its midpoint
        return BOUNDARIES[NUM_BUCKETS - 1];
    }

    double p50() const { return percentile(50.0); }
    double p90() const { return percentile(90.0); }
    double p99() const { return percentile(99.0); }

    void reset() {
        buckets.fill(0);
        total_samples = 0;
    }
};
