#pragma once
#include "shadow_orderbook.hpp"
#include <string>
#include <atomic>
#include <cstdint>

// ─────────────────────────────────────────────────────────────────────────────
// CorrectnessEngine — Periodically proves the contestant's engine is correct
//
// Strategy (from blueprint):
//   1. Inject a LIMIT BUY at $0.01 — the lowest possible bid, always goes
//      to the bottom of the bid book. If the engine has price-time priority
//      working correctly, this order MUST appear at the bottom of the bids.
//
//   2. Wait 200ms for the engine to process it.
//
//   3. GET /book/{ticker} — fetch the current book snapshot.
//
//   4. Verify that $0.01 appears in the bids. If yes → price-time priority
//      is functioning → is_correct = true.
//
// This is a "black-box" correctness check: we don't need to see every order
// the bots sent. We only need to verify our known-state watermark appears
// where we expect it.
//
// Why $0.01? It's below any real market price, so it will never match
// against an ask. It sits in the book forever until we cancel it.
// This makes it a reliable probe.
// ─────────────────────────────────────────────────────────────────────────────

class CorrectnessEngine {
public:
    CorrectnessEngine(const std::string& sandbox_host,
                      const std::string& sandbox_port);

    // Run one full correctness check cycle.
    // Returns true if the engine is behaving correctly.
    bool run_check(const std::string& ticker = "AAPL");

    // Last known correctness result (used by artifact_checker for DB writes)
    bool last_result() const { return last_result_.load(); }

private:
    std::string         sandbox_host_;
    std::string         sandbox_port_;
    ShadowOrderbook     shadow_book_;
    uint64_t            next_order_id_ = 9000000; // high ID to avoid collision
    std::atomic<bool>   last_result_{true};

    // POST /order → returns order_id from response, or 0 on failure
    uint64_t inject_watermark(const std::string& ticker,
                               int price, int qty);

    // GET /book/{ticker} → returns raw JSON body
    std::string fetch_book_snapshot(const std::string& ticker);

    // DELETE /order/{id} → cleanup watermark after verification
    void cancel_watermark(uint64_t order_id);

    // Minimal synchronous HTTP using BSD sockets (no Boost dependency here)
    std::string http_post(const std::string& path, const std::string& body);
    std::string http_get(const std::string& path);
    void        http_delete(const std::string& path);

    // Parse order_id from POST /order response JSON
    uint64_t parse_order_id(const std::string& response_json);

    // Snapshot helpers
    std::vector<BookLevel> parse_levels(const std::string& json, const std::string& key) const;
    std::optional<BookLevel> find_level(const std::vector<BookLevel>& levels, int price) const;
    int choose_watermark_price(const std::vector<BookLevel>& bids,
                               const std::vector<BookLevel>& asks) const;
};
