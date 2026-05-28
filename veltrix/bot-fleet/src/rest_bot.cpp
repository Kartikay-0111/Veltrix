#include "rest_bot.hpp"
#include <sstream>
#include <iomanip>
#include <chrono>

RestBot::RestBot(const std::string &host,
                 const std::string &port,
                 uint64_t bot_id)
    : bot_id_(bot_id), current_order_type_(OrderType::LIMIT), rng_(std::random_device{}() ^ (bot_id * 2654435761ULL)) // unique seed per bot
      ,
      price_dist_(90, 110), qty_dist_(1, 100), ticker_dist_(0, static_cast<int>(TICKERS.size()) - 1), type_dist_(0, 5) // 0=LIMIT, 1=MARKET, 2=CANCEL, 3=FOK, 4=FAK, 5=GFD
{
    target_host = host;
    target_port = port;
}

std::string RestBot::generate_request()
{
    // Rotate order types for a realistic mix of behaviors.
    int roll = type_dist_(rng_);
    std::string body;

    switch (roll)
    {
    case 0:
        current_order_type_ = OrderType::LIMIT;
        body = make_limit_order("LIMIT");
        break;
    case 1:
        current_order_type_ = OrderType::MARKET;
        body = make_market_order();
        break;
    case 2:
        current_order_type_ = OrderType::CANCEL;
        body = make_cancel_order();
        break;
    case 3:
        current_order_type_ = OrderType::LIMIT;
        body = make_limit_order("FOK");
        break;
    case 4:
        current_order_type_ = OrderType::LIMIT;
        body = make_limit_order("FAK");
        break;
    default:
        current_order_type_ = OrderType::LIMIT;
        body = make_limit_order("GFD");
        break;
    }

    return  build_http_request(body);
}

std::string RestBot::make_limit_order(const char *type)
{
    auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();

    // Generate globally unique ID (e.g., "Bot5-Order12")
    order_counter_++;
    
    std::ostringstream oss;
    oss << "{"
        << "\"order_id\":\"" << bot_id_ << "-" << order_counter_ << "\","
        << "\"type\":\"" << type << "\","
        << "\"ticker\":\"" << TICKERS[ticker_dist_(rng_)] << "\","
        << "\"side\":\"" << (rng_() % 2 == 0 ? "BUY" : "SELL") << "\","
        << "\"price\":" << price_dist_(rng_) << ".00,"
        << "\"quantity\":" << qty_dist_(rng_) << ","
        << "\"bot_id\":" << bot_id_ << ","
        << "\"timestamp\":" << now 
        << "}";
    return oss.str();
}

std::string RestBot::make_market_order()
{
    auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();

    // Generate globally unique ID (e.g., "Bot5-Order12")
    order_counter_++;

    std::ostringstream oss;
    oss << "{"
        << "\"order_id\":\"" << bot_id_ << "-" << order_counter_ << "\","
        << "\"type\":\"MARKET\","
        << "\"ticker\":\"" << TICKERS[ticker_dist_(rng_)] << "\","
        << "\"side\":\"" << (rng_() % 2 == 0 ? "BUY" : "SELL") << "\","
        << "\"quantity\":" << qty_dist_(rng_) << ","
        << "\"bot_id\":" << bot_id_ << ","
        << "\"timestamp\":" << now 
        << "}";
    return oss.str();
}

std::string RestBot::make_cancel_order()
{
    // If we haven't received any accepted order IDs from the server yet,
    // fall back to a LIMIT order (can't cancel what we don't know)
    if (accepted_orders_.empty())
        return make_limit_order("LIMIT");

    // Pick a random order from our tracked list of server-assigned IDs
    std::uniform_int_distribution<std::size_t> dist(0, accepted_orders_.size() - 1);
    std::size_t idx = dist(rng_);
    uint64_t target_id = accepted_orders_[idx];

    // Remove it — can't cancel the same order twice
    accepted_orders_.erase(accepted_orders_.begin() + static_cast<std::ptrdiff_t>(idx));

    auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();

    // Server expects: {"type":"cancel", "order_id": <integer>}
    std::ostringstream oss;
    oss << "{"
        << "\"type\":\"cancel\","
        << "\"order_id\":" << target_id << ","
        << "\"bot_id\":" << bot_id_ << ","
        << "\"timestamp\":" << now
        << "}";
    return build_http_request(oss.str());
}

void RestBot::record_accepted_order(uint64_t order_id)
{
    accepted_orders_.push_back(order_id);
    if (accepted_orders_.size() > MAX_TRACKED_ORDERS)
        accepted_orders_.pop_front();
}

std::string RestBot::build_http_request(const std::string &body) const
{
    // Build a complete raw HTTP/1.1 POST request
    // Written directly to the socket — no HTTP library overhead on hot path
    std::ostringstream req;
    req << "POST " << target_path << " HTTP/1.1\r\n"
        << "Host: " << target_host << ":" << target_port << "\r\n"
        << "Content-Type: application/json\r\n"
        << "Content-Length: " << body.size() << "\r\n"
        << "Connection: keep-alive\r\n"
        << "\r\n"
        << body;
    return req.str();
}
