#pragma once
#include <string>
#include <chrono>

// ─────────────────────────────────────────────────────────────────────────────
// BotPayload — Abstract Base Class
//
// Every bot type (REST, WebSocket, FIX) must implement this contract.
// The pure virtual generate_request() is the only thing that changes
// between protocols — the thread worker calls it without knowing or
// caring which protocol is underneath.
//
// The virtual destructor is NON-OPTIONAL. Without it, deleting a
// RestBot* through a BotPayload* would only call ~BotPayload(),
// leaking the child's resources. This is the most common C++ memory
// leak in polymorphic hierarchies.
// ─────────────────────────────────────────────────────────────────────────────

enum class OrderType { LIMIT, MARKET, CANCEL };

struct BotPayload {
    virtual ~BotPayload() = default;

    // Must produce a ready-to-send request string (HTTP body or WS frame)
    virtual std::string generate_request() = 0;

    // Which order type this bot is currently sending
    virtual OrderType order_type() const = 0;

    // Target endpoint this bot hammers
    std::string target_host;
    std::string target_port;
    std::string target_path = "/order";  // REST endpoint on contestant's server
};
