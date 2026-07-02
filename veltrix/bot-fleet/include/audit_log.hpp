#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <chrono>

// ─────────────────────────────────────────────────────────────────────────────
// AuditLog — Per-Thread Event Capture (Shared-Nothing)
//
// Each ThreadWorker owns exactly one AuditLog. No locks. No sharing.
// Events are appended during the hot path and flushed every 500ms via gRPC.
//
// Two event types:
//   1. OrderSubmittedEntry — Intent: captured BEFORE sending HTTP request
//   2. TradeExecutedEntry  — Observation: parsed from the HTTP response
// ─────────────────────────────────────────────────────────────────────────────

struct OrderSubmittedEntry
{
    int64_t timestamp_us = 0;     // Bot's local clock, epoch microseconds
    std::string submission_id;
    int32_t bot_id = 0;
    std::string order_id;         // Bot-generated ID (e.g. "24-1")
    std::string action;           // LIMIT | MARKET | CANCEL | FOK | FAK | GFD
    std::string side;             // BUY | SELL (empty for CANCEL)
    std::string ticker;
    double price = 0.0;
    int32_t quantity = 0;
    uint64_t cancel_target_id = 0;    // For CANCEL: server-assigned ID being cancelled
    uint64_t seq = 0;                 // Monotonic per-submission sequence (replay order + dedup)
    uint64_t contestant_order_id = 0; // Server-assigned ID for this order (captured post-response)
    bool end_of_run = false;          // Sentinel: serialized correctness run complete
};

struct TradeExecutedEntry
{
    int64_t timestamp_us = 0;          // Bot's clock upon receiving response
    std::string submission_id;
    int32_t bot_id = 0;
    uint64_t contestant_order_id = 0;  // Server-assigned order ID for the new order
    uint64_t matched_order_id = 0;     // The resting order that was matched against
    double execution_price = 0.0;
    int32_t execution_quantity = 0;
    std::string ticker;
    std::string aggressor_order_id;    // Bot-generated order_id of the aggressing order
                                       // (join key back to the OrderSubmitted intent)
    uint64_t seq = 0;                  // Monotonic per-submission sequence (replay order + dedup)
};

class AuditLog
{
public:
    AuditLog()
    {
        // Pre-allocate for a typical 500ms window
        orders_.reserve(8192);
        trades_.reserve(4096);
    }

    void record_order(const std::string &submission_id,
                      int32_t bot_id,
                      const std::string &order_id,
                      const std::string &action,
                      const std::string &side,
                      const std::string &ticker,
                      double price,
                      int32_t quantity,
                      uint64_t cancel_target_id,
                      uint64_t seq,
                      uint64_t contestant_order_id)
    {
        auto now_us = std::chrono::duration_cast<std::chrono::microseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();

        orders_.push_back({now_us, submission_id, bot_id, order_id,
                           action, side, ticker, price, quantity, cancel_target_id,
                           seq, contestant_order_id, false});
    }

    // Sentinel marking the end of a serialized correctness run so the checker can
    // finalize the verdict. Emitted once, after the order stream is complete.
    void record_end_of_run(const std::string &submission_id, uint64_t seq)
    {
        auto now_us = std::chrono::duration_cast<std::chrono::microseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();

        OrderSubmittedEntry entry;
        entry.timestamp_us = now_us;
        entry.submission_id = submission_id;
        entry.seq = seq;
        entry.end_of_run = true;
        orders_.push_back(entry);
    }

    void record_trade(const std::string &submission_id,
                      int32_t bot_id,
                      const std::string &ticker,
                      uint64_t contestant_order_id,
                      uint64_t matched_order_id,
                      double execution_price,
                      int32_t execution_quantity,
                      const std::string &aggressor_order_id,
                      uint64_t seq)
    {
        auto now_us = std::chrono::duration_cast<std::chrono::microseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();

        trades_.push_back({now_us, submission_id, bot_id,
                           contestant_order_id, matched_order_id,
                           execution_price, execution_quantity, ticker,
                           aggressor_order_id, seq});
    }

    const std::vector<OrderSubmittedEntry> &orders() const { return orders_; }
    const std::vector<TradeExecutedEntry> &trades() const { return trades_; }

    bool empty() const { return orders_.empty() && trades_.empty(); }

    void clear()
    {
        orders_.clear();
        trades_.clear();
    }

private:
    std::vector<OrderSubmittedEntry> orders_;
    std::vector<TradeExecutedEntry> trades_;
};
