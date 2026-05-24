#include "rest_bot.hpp"
#include <sstream>
#include <iomanip>

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

    return build_http_request(body);
}

std::string RestBot::make_limit_order(const char *type)
{
    std::ostringstream oss;
    oss << "{"
        << "\"type\":\"" << type << "\","
        << "\"ticker\":\"" << TICKERS[ticker_dist_(rng_)] << "\","
        << "\"side\":\"" << (rng_() % 2 == 0 ? "BUY" : "SELL") << "\","
        << "\"price\":" << price_dist_(rng_) << ".00,"
        << "\"quantity\":" << qty_dist_(rng_) << ","
        << "\"bot_id\":" << bot_id_
        << "}";
    return oss.str();
}

std::string RestBot::make_market_order()
{
    std::ostringstream oss;
    oss << "{"
        << "\"type\":\"MARKET\","
        << "\"ticker\":\"" << TICKERS[ticker_dist_(rng_)] << "\","
        << "\"side\":\"" << (rng_() % 2 == 0 ? "BUY" : "SELL") << "\","
        << "\"quantity\":" << qty_dist_(rng_) << ","
        << "\"bot_id\":" << bot_id_
        << "}";
    return oss.str();
}

std::string RestBot::make_cancel_order()
{
    // Cancel a random order ID (1–10000 range — contestant must handle gracefully)
    std::ostringstream oss;
    oss << "{"
        << "\"type\":\"CANCEL\","
        << "\"order_id\":" << (rng_() % 10000 + 1) << ","
        << "\"bot_id\":" << bot_id_
        << "}";
    return oss.str();
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
