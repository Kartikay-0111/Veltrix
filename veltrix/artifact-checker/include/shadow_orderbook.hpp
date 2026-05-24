#pragma once
#include <string>
#include <vector>
#include <optional>
#include <cstdint>

// ─────────────────────────────────────────────────────────────────────────────
// ShadowOrderbook — Internal orderbook replica for correctness verification
//
// The artifact checker maintains this internally and periodically compares
// it against the contestant's GET /book/{ticker} snapshot.
//
// Design:
//   Bids → std::map<price, qty, std::greater<double>>
//           highest price first (best bid at top)
//   Asks → std::map<price, qty, std::less<double>>
//           lowest price first (best ask at top)
//
// We only track orders the artifact checker itself injects (watermark orders).
// We don't try to track all 10,000 bot orders — that would require the bots
// to log every individual order to Redpanda (too much data).
//
// Correctness strategy:
//   1. Inject a LIMIT BUY at $0.01 (always the worst bid — goes to bottom)
//   2. Wait ~200ms for it to be processed
//   3. GET /book/{ticker}
//   4. Check that $0.01 appears in the bids at the bottom
//   5. If yes → price-time priority is working → is_correct = true
// ─────────────────────────────────────────────────────────────────────────────

struct BookLevel {
    double price;
    int    quantity;
};

class ShadowOrderbook {
public:
    struct WatermarkEntry {
        std::string ticker;
        double      price;
        int         qty;
        uint64_t    order_id;
        int64_t     injected_at_ms;
        int         baseline_qty = 0;
        bool        verified = false;
    };

    void record_watermark(const std::string& ticker,
                          double              price,
                          int                 qty,
                          uint64_t            order_id,
                          int                 baseline_qty);

    bool verify_snapshot(const std::string& ticker,
                         const std::string& snapshot_json) const;

    void expire_watermarks(int64_t now_ms, int64_t max_age_ms = 30000);

    const std::vector<WatermarkEntry>& watermarks() const { return watermarks_; }

private:
    std::vector<WatermarkEntry> watermarks_;

    std::vector<BookLevel> parse_bids(const std::string& json) const;

    std::optional<BookLevel> find_level(const std::vector<BookLevel>& levels,
                                        double price) const;
};