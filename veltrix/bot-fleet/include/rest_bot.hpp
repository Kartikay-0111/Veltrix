#pragma once
#include "bot_payload.hpp"
#include <random>
#include <array>
#include <deque>
#include <string>

// ─────────────────────────────────────────────────────────────────────────────
// RestBot — Concrete HTTP/REST order generator
//
// Generates JSON order payloads for Limit, Market, and Cancel orders.
// Each bot has its own RNG so threads never share state (shared-nothing).
// ─────────────────────────────────────────────────────────────────────────────

// Tickers the bots trade — simulates a real multi-symbol exchange
static constexpr std::array<const char *, 5> TICKERS = {
    "AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"};

class RestBot : public BotPayload
{
public:
    explicit RestBot(const std::string &host,
                     const std::string &port,
                     uint64_t bot_id);

    // Implements BotPayload interface
    std::string generate_request() override;
    OrderType order_type() const override { return current_order_type_; }

    // Build the full raw HTTP/1.1 request string (headers + body)
    // ready to be written directly to a socket
    std::string build_http_request(const std::string &body) const;

    // Called by ThreadWorker after a successful 200 response to track
    // server-assigned order IDs for the cancel flow
    void record_accepted_order(uint64_t order_id);

private:
    uint64_t bot_id_;
    OrderType current_order_type_;
    uint64_t order_counter_ = 0;

    // Each bot has its own RNG — zero contention, zero locking
    std::mt19937 rng_;
    std::uniform_int_distribution<int> price_dist_;  // 90–110 price range
    std::uniform_int_distribution<int> qty_dist_;    // 1–100 quantity
    std::uniform_int_distribution<int> ticker_dist_; // which symbol
    std::uniform_int_distribution<int> type_dist_;   // which order type

    // Ring buffer of server-assigned order IDs for realistic cancel flow
    static constexpr std::size_t MAX_TRACKED_ORDERS = 200;
    std::deque<uint64_t> accepted_orders_;

    std::string make_limit_order(const char *type = "LIMIT");
    std::string make_market_order();
    std::string make_cancel_order();
};
