#include <algorithm>
#include <array>
#include <atomic>
#include <arpa/inet.h>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <deque>
#include <functional>
#include <iomanip>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <netinet/in.h>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <sys/types.h>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <unistd.h>

using Price = std::int32_t;
using Quantity = std::uint32_t;
using OrderId = std::uint64_t;
using OrderIds = std::vector<OrderId>;

enum class OrderType
{
    GoodTillCancel,
    FillAndKill,
    FillOrKill,
    GoodForDay,
    Market,
};

enum class Side
{
    Buy,
    Sell
};

struct Constants
{
    static constexpr Price InvalidPrice = 0;
};

struct TradeInfo
{
    OrderId orderId_{};
    Price price_{};
    Quantity quantity_{};
};

class Trade
{
public:
    Trade(const TradeInfo& bidTrade, const TradeInfo& askTrade)
        : bidTrade_{ bidTrade }
        , askTrade_{ askTrade }
    {
    }

    const TradeInfo& GetBidTrade() const { return bidTrade_; }
    const TradeInfo& GetAskTrade() const { return askTrade_; }

private:
    TradeInfo bidTrade_{};
    TradeInfo askTrade_{};
};

using Trades = std::vector<Trade>;

struct LevelInfo
{
    Price price_{};
    Quantity quantity_{};
};

using LevelInfos = std::vector<LevelInfo>;

class OrderbookLevelInfos
{
public:
    OrderbookLevelInfos(const LevelInfos& bids, const LevelInfos& asks)
        : bids_{ bids }
        , asks_{ asks }
    {
    }

    const LevelInfos& GetBids() const { return bids_; }
    const LevelInfos& GetAsks() const { return asks_; }

private:
    LevelInfos bids_{};
    LevelInfos asks_{};
};

class Order
{
public:
    Order(OrderType orderType, OrderId orderId, Side side, Price price, Quantity quantity)
        : orderType_{ orderType }
        , orderId_{ orderId }
        , side_{ side }
        , price_{ price }
        , initialQuantity_{ quantity }
        , remainingQuantity_{ quantity }
    {
    }

    Order(OrderId orderId, Side side, Quantity quantity)
        : Order(OrderType::Market, orderId, side, Constants::InvalidPrice, quantity)
    {
    }

    OrderId GetOrderId() const { return orderId_; }
    Side GetSide() const { return side_; }
    Price GetPrice() const { return price_; }
    OrderType GetOrderType() const { return orderType_; }
    Quantity GetInitialQuantity() const { return initialQuantity_; }
    Quantity GetRemainingQuantity() const { return remainingQuantity_; }
    Quantity GetFilledQuantity() const { return GetInitialQuantity() - GetRemainingQuantity(); }
    bool IsFilled() const { return GetRemainingQuantity() == 0; }

    void Fill(Quantity quantity)
    {
        if (quantity > GetRemainingQuantity())
            throw std::logic_error("Attempted to overfill order");

        remainingQuantity_ -= quantity;
    }

    void ToGoodTillCancel(Price price)
    {
        if (GetOrderType() != OrderType::Market)
            throw std::logic_error("Only market orders can be converted to good-till-cancel");

        price_ = price;
        orderType_ = OrderType::GoodTillCancel;
    }

private:
    OrderType orderType_;
    OrderId orderId_;
    Side side_;
    Price price_;
    Quantity initialQuantity_;
    Quantity remainingQuantity_;
};

using OrderPointer = std::shared_ptr<Order>;
using OrderPointers = std::list<OrderPointer>;

class OrderModify
{
public:
    OrderModify(OrderId orderId, Side side, Price price, Quantity quantity)
        : orderId_{ orderId }
        , price_{ price }
        , side_{ side }
        , quantity_{ quantity }
    {
    }

    OrderId GetOrderId() const { return orderId_; }
    Price GetPrice() const { return price_; }
    Side GetSide() const { return side_; }
    Quantity GetQuantity() const { return quantity_; }

    OrderPointer ToOrderPointer(OrderType type) const
    {
        return std::make_shared<Order>(type, GetOrderId(), GetSide(), GetPrice(), GetQuantity());
    }

private:
    OrderId orderId_;
    Price price_;
    Side side_;
    Quantity quantity_;
};

class Orderbook
{
private:
    struct OrderEntry
    {
        OrderPointer order_{ nullptr };
        OrderPointers::iterator location_;
    };

    struct LevelData
    {
        Quantity quantity_{ 0 };
        std::int64_t count_{ 0 };

        enum class Action
        {
            Add,
            Remove,
            Match,
        };
    };

    std::unordered_map<Price, LevelData> data_;
    std::map<Price, OrderPointers, std::greater<Price>> bids_;
    std::map<Price, OrderPointers, std::less<Price>> asks_;
    std::unordered_map<OrderId, OrderEntry> orders_;
    mutable std::mutex ordersMutex_;
    std::thread ordersPruneThread_;
    std::condition_variable shutdownConditionVariable_;
    std::atomic<bool> shutdown_{ false };

    void PruneGoodForDayOrders()
    {
        using namespace std::chrono;
        const auto end = hours(16);

        while (true)
        {
            const auto now = system_clock::now();
            const auto now_c = system_clock::to_time_t(now);
            std::tm now_parts{};
            localtime_r(&now_c, &now_parts);

            if (now_parts.tm_hour >= end.count())
                now_parts.tm_mday += 1;

            now_parts.tm_hour = static_cast<int>(end.count());
            now_parts.tm_min = 0;
            now_parts.tm_sec = 0;

            auto next = system_clock::from_time_t(std::mktime(&now_parts));
            auto till = next - now + milliseconds(100);

            {
                std::unique_lock ordersLock{ ordersMutex_ };

                if (shutdown_.load(std::memory_order_acquire) ||
                    shutdownConditionVariable_.wait_for(ordersLock, till) == std::cv_status::no_timeout)
                    return;
            }

            OrderIds orderIds;

            {
                std::scoped_lock ordersLock{ ordersMutex_ };

                for (const auto& [_, entry] : orders_)
                {
                    const auto& order = entry.order_;
                    if (order->GetOrderType() != OrderType::GoodForDay)
                        continue;

                    orderIds.push_back(order->GetOrderId());
                }
            }

            CancelOrders(orderIds);
        }
    }

    void CancelOrders(OrderIds orderIds)
    {
        std::scoped_lock ordersLock{ ordersMutex_ };

        for (const auto& orderId : orderIds)
            CancelOrderInternal(orderId);
    }

    void CancelOrderInternal(OrderId orderId)
    {
        auto orderIt = orders_.find(orderId);
        if (orderIt == orders_.end())
            return;

        const auto& [order, iterator] = orderIt->second;
        orders_.erase(orderIt);

        if (order->GetSide() == Side::Sell)
        {
            auto price = order->GetPrice();
            auto levelIt = asks_.find(price);
            if (levelIt != asks_.end())
            {
                auto& orders = levelIt->second;
                orders.erase(iterator);
                if (orders.empty())
                    asks_.erase(levelIt);
            }
        }
        else
        {
            auto price = order->GetPrice();
            auto levelIt = bids_.find(price);
            if (levelIt != bids_.end())
            {
                auto& orders = levelIt->second;
                orders.erase(iterator);
                if (orders.empty())
                    bids_.erase(levelIt);
            }
        }

        OnOrderCancelled(order);
    }

    void OnOrderCancelled(OrderPointer order)
    {
        UpdateLevelData(order->GetPrice(), order->GetRemainingQuantity(), LevelData::Action::Remove);
    }

    void OnOrderAdded(OrderPointer order)
    {
        UpdateLevelData(order->GetPrice(), order->GetInitialQuantity(), LevelData::Action::Add);
    }

    void OnOrderMatched(Price price, Quantity quantity, bool isFullyFilled)
    {
        UpdateLevelData(price, quantity, isFullyFilled ? LevelData::Action::Remove : LevelData::Action::Match);
    }

    void UpdateLevelData(Price price, Quantity quantity, LevelData::Action action)
    {
        auto& data = data_[price];

        data.count_ += action == LevelData::Action::Remove ? -1 : action == LevelData::Action::Add ? 1 : 0;
        if (action == LevelData::Action::Remove || action == LevelData::Action::Match)
            data.quantity_ -= quantity;
        else
            data.quantity_ += quantity;

        if (data.count_ == 0)
            data_.erase(price);
    }

    bool CanFullyFill(Side side, Price price, Quantity quantity) const
    {
        Quantity available = 0;

        if (side == Side::Buy)
        {
            for (const auto& [askPrice, orders] : asks_)
            {
                if (askPrice > price)
                    break;

                for (const auto& order : orders)
                {
                    available += order->GetRemainingQuantity();
                    if (available >= quantity)
                        return true;
                }
            }
        }
        else
        {
            for (const auto& [bidPrice, orders] : bids_)
            {
                if (bidPrice < price)
                    break;

                for (const auto& order : orders)
                {
                    available += order->GetRemainingQuantity();
                    if (available >= quantity)
                        return true;
                }
            }
        }

        return false;
    }

    bool CanMatch(Side side, Price price) const
    {
        if (side == Side::Buy)
        {
            if (asks_.empty())
                return false;

            const auto& [bestAsk, _] = *asks_.begin();
            return price >= bestAsk;
        }

        if (bids_.empty())
            return false;

        const auto& [bestBid, _] = *bids_.begin();
        return price <= bestBid;
    }

    Trades MatchOrders()
    {
        Trades trades;
        trades.reserve(orders_.size());

        while (true)
        {
            if (bids_.empty() || asks_.empty())
                break;

            auto& [bidPrice, bids] = *bids_.begin();
            auto& [askPrice, asks] = *asks_.begin();

            if (bidPrice < askPrice)
                break;

            while (!bids.empty() && !asks.empty())
            {
                auto bid = bids.front();
                auto ask = asks.front();

                Quantity quantity = std::min(bid->GetRemainingQuantity(), ask->GetRemainingQuantity());

                bid->Fill(quantity);
                ask->Fill(quantity);

                if (bid->IsFilled())
                {
                    bids.pop_front();
                    orders_.erase(bid->GetOrderId());
                }

                if (ask->IsFilled())
                {
                    asks.pop_front();
                    orders_.erase(ask->GetOrderId());
                }

                trades.push_back(Trade{
                    TradeInfo{ bid->GetOrderId(), bid->GetPrice(), quantity },
                    TradeInfo{ ask->GetOrderId(), ask->GetPrice(), quantity }
                });

                OnOrderMatched(bid->GetPrice(), quantity, bid->IsFilled());
                OnOrderMatched(ask->GetPrice(), quantity, ask->IsFilled());
            }

            if (bids.empty())
            {
                bids_.erase(bidPrice);
                data_.erase(bidPrice);
            }

            if (asks.empty())
            {
                asks_.erase(askPrice);
                data_.erase(askPrice);
            }
        }

        if (!bids_.empty())
        {
            auto& [_, bids] = *bids_.begin();
            auto& order = bids.front();
            if (order->GetOrderType() == OrderType::FillAndKill)
                CancelOrder(order->GetOrderId());
        }

        if (!asks_.empty())
        {
            auto& [_, asks] = *asks_.begin();
            auto& order = asks.front();
            if (order->GetOrderType() == OrderType::FillAndKill)
                CancelOrder(order->GetOrderId());
        }

        return trades;
    }

public:
    Orderbook()
        : ordersPruneThread_{ [this] { PruneGoodForDayOrders(); } }
    {
    }

    Orderbook(const Orderbook&) = delete;
    void operator=(const Orderbook&) = delete;
    Orderbook(Orderbook&&) = delete;
    void operator=(Orderbook&&) = delete;

    ~Orderbook()
    {
        shutdown_.store(true, std::memory_order_release);
        shutdownConditionVariable_.notify_one();
        if (ordersPruneThread_.joinable())
            ordersPruneThread_.join();
    }

    Trades AddOrder(OrderPointer order)
    {
        std::scoped_lock ordersLock{ ordersMutex_ };

        if (orders_.find(order->GetOrderId()) != orders_.end())
            return {};

        if (order->GetOrderType() == OrderType::Market)
        {
            if (order->GetSide() == Side::Buy && !asks_.empty())
            {
                const auto& [worstAsk, _] = *asks_.rbegin();
                order->ToGoodTillCancel(worstAsk);
            }
            else if (order->GetSide() == Side::Sell && !bids_.empty())
            {
                const auto& [worstBid, _] = *bids_.rbegin();
                order->ToGoodTillCancel(worstBid);
            }
            else
            {
                return {};
            }
        }

        if (order->GetOrderType() == OrderType::FillAndKill && !CanMatch(order->GetSide(), order->GetPrice()))
            return {};

        if (order->GetOrderType() == OrderType::FillOrKill && !CanFullyFill(order->GetSide(), order->GetPrice(), order->GetInitialQuantity()))
            return {};

        OrderPointers::iterator iterator;

        if (order->GetSide() == Side::Buy)
        {
            auto& orders = bids_[order->GetPrice()];
            orders.push_back(order);
            iterator = std::prev(orders.end());
        }
        else
        {
            auto& orders = asks_[order->GetPrice()];
            orders.push_back(order);
            iterator = std::prev(orders.end());
        }

        orders_.insert({ order->GetOrderId(), OrderEntry{ order, iterator } });

        OnOrderAdded(order);

        return MatchOrders();
    }

    void CancelOrder(OrderId orderId)
    {
        std::scoped_lock ordersLock{ ordersMutex_ };
        CancelOrderInternal(orderId);
    }

    Trades ModifyOrder(OrderModify order)
    {
        OrderType orderType;

        {
            std::scoped_lock ordersLock{ ordersMutex_ };

            if (orders_.find(order.GetOrderId()) == orders_.end())
                return {};

            const auto& [existingOrder, _] = orders_.at(order.GetOrderId());
            orderType = existingOrder->GetOrderType();
        }

        CancelOrder(order.GetOrderId());
        return AddOrder(order.ToOrderPointer(orderType));
    }

    std::size_t Size() const
    {
        std::scoped_lock ordersLock{ ordersMutex_ };
        return orders_.size();
    }

    bool HasOrder(OrderId orderId) const
    {
        std::scoped_lock ordersLock{ ordersMutex_ };
        return orders_.find(orderId) != orders_.end();
    }

    OrderbookLevelInfos GetOrderInfos() const
    {
        LevelInfos bidInfos, askInfos;
        bidInfos.reserve(bids_.size());
        askInfos.reserve(asks_.size());

        auto createLevelInfo = [](Price price, const OrderPointers& orders)
        {
            return LevelInfo{ price, std::accumulate(orders.begin(), orders.end(), static_cast<Quantity>(0),
                [](Quantity runningSum, const OrderPointer& order)
                { return runningSum + order->GetRemainingQuantity(); }) };
        };

        for (const auto& [price, orders] : bids_)
            bidInfos.push_back(createLevelInfo(price, orders));

        for (const auto& [price, orders] : asks_)
            askInfos.push_back(createLevelInfo(price, orders));

        return OrderbookLevelInfos{ bidInfos, askInfos };
    }
};

struct HttpRequest
{
    std::string method;
    std::string target;
    std::string path;
    std::unordered_map<std::string, std::string> headers;
    std::string body;
};

static std::string to_lower(std::string value)
{
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return value;
}

static std::string trim(std::string value)
{
    auto is_space = [](unsigned char c) { return std::isspace(c) != 0; };
    while (!value.empty() && is_space(value.front()))
        value.erase(value.begin());
    while (!value.empty() && is_space(value.back()))
        value.pop_back();
    return value;
}

static bool send_all(int fd, const std::string& data)
{
    std::size_t sent = 0;
    while (sent < data.size())
    {
        const auto n = ::send(fd, data.data() + sent, data.size() - sent, 0);
        if (n <= 0)
            return false;
        sent += static_cast<std::size_t>(n);
    }
    return true;
}

static bool recv_into_buffer(int fd, std::string& buffer)
{
    char temp[4096];
    const auto n = ::recv(fd, temp, sizeof(temp), 0);
    if (n <= 0)
        return false;
    buffer.append(temp, static_cast<std::size_t>(n));
    return true;
}

static std::optional<HttpRequest> read_http_request(int fd)
{
    std::string buffer;
    buffer.reserve(8192);

    while (buffer.find("\r\n\r\n") == std::string::npos)
    {
        if (!recv_into_buffer(fd, buffer))
            return std::nullopt;
        if (buffer.size() > 64 * 1024)
            return std::nullopt;
    }

    const auto header_end = buffer.find("\r\n\r\n");
    const auto header_text = buffer.substr(0, header_end);
    auto body = buffer.substr(header_end + 4);

    std::istringstream header_stream(header_text);
    std::string request_line;
    if (!std::getline(header_stream, request_line))
        return std::nullopt;

    if (!request_line.empty() && request_line.back() == '\r')
        request_line.pop_back();

    std::istringstream request_line_stream(request_line);
    HttpRequest request;
    std::string http_version;
    request_line_stream >> request.method >> request.target >> http_version;
    if (request.method.empty() || request.target.empty())
        return std::nullopt;

    std::string header_line;
    while (std::getline(header_stream, header_line))
    {
        if (!header_line.empty() && header_line.back() == '\r')
            header_line.pop_back();

        const auto colon = header_line.find(':');
        if (colon == std::string::npos)
            continue;

        auto key = to_lower(trim(header_line.substr(0, colon)));
        auto value = trim(header_line.substr(colon + 1));
        request.headers.emplace(std::move(key), std::move(value));
    }

    const auto content_length_it = request.headers.find("content-length");
    if (content_length_it != request.headers.end())
    {
        const auto content_length = static_cast<std::size_t>(std::stoul(content_length_it->second));
        while (body.size() < content_length)
        {
            if (!recv_into_buffer(fd, buffer))
                return std::nullopt;
            body.append(buffer);
            buffer.clear();
        }
        body.resize(content_length);
    }

    request.body = std::move(body);
    const auto query_pos = request.target.find('?');
    request.path = query_pos == std::string::npos ? request.target : request.target.substr(0, query_pos);
    return request;
}

static std::string json_escape(std::string value)
{
    std::string out;
    out.reserve(value.size() + 8);
    for (char c : value)
    {
        switch (c)
        {
        case '\\': out += "\\\\"; break;
        case '"': out += "\\\""; break;
        case '\b': out += "\\b"; break;
        case '\f': out += "\\f"; break;
        case '\n': out += "\\n"; break;
        case '\r': out += "\\r"; break;
        case '\t': out += "\\t"; break;
        default:
            out.push_back(c);
            break;
        }
    }
    return out;
}

static std::optional<std::string> extract_json_string(const std::string& body, const std::string& key)
{
    const auto key_pos = body.find('"' + key + '"');
    if (key_pos == std::string::npos)
        return std::nullopt;

    const auto colon = body.find(':', key_pos);
    if (colon == std::string::npos)
        return std::nullopt;

    const auto first_quote = body.find('"', colon + 1);
    if (first_quote == std::string::npos)
        return std::nullopt;

    const auto second_quote = body.find('"', first_quote + 1);
    if (second_quote == std::string::npos)
        return std::nullopt;

    return body.substr(first_quote + 1, second_quote - first_quote - 1);
}

static std::optional<long long> extract_json_integer(const std::string& body, const std::string& key)
{
    const auto key_pos = body.find('"' + key + '"');
    if (key_pos == std::string::npos)
        return std::nullopt;

    const auto colon = body.find(':', key_pos);
    if (colon == std::string::npos)
        return std::nullopt;

    auto start = body.find_first_of("-0123456789", colon + 1);
    if (start == std::string::npos)
        return std::nullopt;

    auto end = body.find_first_not_of("-0123456789", start);
    try
    {
        return std::stoll(body.substr(start, end - start));
    }
    catch (...)
    {
        return std::nullopt;
    }
}

static std::string side_to_string(Side side)
{
    return side == Side::Buy ? "buy" : "sell";
}

static std::optional<Side> parse_side(const std::string& value)
{
    const auto lower = to_lower(value);
    if (lower == "buy")
        return Side::Buy;
    if (lower == "sell")
        return Side::Sell;
    return std::nullopt;
}

static std::vector<std::string> split_path(const std::string& path)
{
    std::vector<std::string> parts;
    std::istringstream stream(path);
    std::string part;
    while (std::getline(stream, part, '/'))
    {
        if (!part.empty())
            parts.push_back(part);
    }
    return parts;
}

static std::string build_http_response(int code, const std::string& status, const std::string& body, const std::string& content_type = "application/json")
{
    std::ostringstream response;
    response << "HTTP/1.1 " << code << ' ' << status << "\r\n";
    response << "Content-Type: " << content_type << "\r\n";
    response << "Content-Length: " << body.size() << "\r\n";
    response << "Connection: close\r\n\r\n";
    response << body;
    return response.str();
}

static std::string make_book_json(const std::string& ticker, const OrderbookLevelInfos& infos)
{
    auto serialize_levels = [](const LevelInfos& levels)
    {
        std::ostringstream out;
        out << '[';
        for (std::size_t i = 0; i < levels.size(); ++i)
        {
            if (i > 0)
                out << ',';
            out << '{' << "\"price\":" << levels[i].price_ << ',' << "\"qty\":" << levels[i].quantity_ << '}';
        }
        out << ']';
        return out.str();
    };

    std::ostringstream out;
    out << '{'
        << "\"ticker\":\"" << json_escape(ticker) << "\",";
    out << "\"bids\":" << serialize_levels(LevelInfos{ infos.GetBids().begin(), infos.GetBids().begin() + std::min<std::size_t>(10, infos.GetBids().size()) }) << ',';
    out << "\"asks\":" << serialize_levels(LevelInfos{ infos.GetAsks().begin(), infos.GetAsks().begin() + std::min<std::size_t>(10, infos.GetAsks().size()) })
        << '}';
    return out.str();
}

static std::string make_trades_json(const Trades& trades, const std::string& ticker)
{
    std::ostringstream out;
    out << '{' << "\"ticker\":\"" << json_escape(ticker) << "\"," << "\"trades\":";
    out << '[';
    for (std::size_t i = 0; i < trades.size(); ++i)
    {
        if (i > 0)
            out << ',';
        const auto& trade = trades[i];
        out << '{'
            << "\"buy_order_id\":" << trade.GetBidTrade().orderId_ << ','
            << "\"sell_order_id\":" << trade.GetAskTrade().orderId_ << ','
            << "\"price\":" << trade.GetAskTrade().price_ << ','
            << "\"qty\":" << trade.GetAskTrade().quantity_
            << '}';
    }
    out << "]}";
    return out.str();
}

static std::string make_fill_event_json(const Trades& trades, const std::string& ticker)
{
    std::ostringstream out;
    out << '{' << "\"type\":\"FILL\"," << "\"ticker\":\"" << json_escape(ticker) << "\"," << "\"trades\":";
    out << '[';
    for (std::size_t i = 0; i < trades.size(); ++i)
    {
        if (i > 0)
            out << ',';
        const auto& trade = trades[i];
        out << '{'
            << "\"buy_order_id\":" << trade.GetBidTrade().orderId_ << ','
            << "\"sell_order_id\":" << trade.GetAskTrade().orderId_ << ','
            << "\"price\":" << trade.GetAskTrade().price_ << ','
            << "\"qty\":" << trade.GetAskTrade().quantity_
            << '}';
    }
    out << "]}";
    return out.str();
}

class Sha1
{
public:
    void update(const std::string& input)
    {
        for (unsigned char c : input)
        {
            buffer_[bufferSize_++] = c;
            bitLength_ += 8;
            if (bufferSize_ == 64)
            {
                transform(buffer_.data());
                bufferSize_ = 0;
            }
        }
    }

    std::array<std::uint8_t, 20> final()
    {
        std::size_t i = bufferSize_;
        buffer_[i++] = 0x80;

        if (i > 56)
        {
            while (i < 64)
                buffer_[i++] = 0x00;
            transform(buffer_.data());
            i = 0;
        }

        while (i < 56)
            buffer_[i++] = 0x00;

        for (int j = 7; j >= 0; --j)
            buffer_[i++] = static_cast<std::uint8_t>((bitLength_ >> (j * 8)) & 0xff);

        transform(buffer_.data());

        std::array<std::uint8_t, 20> digest{};
        for (int j = 0; j < 5; ++j)
        {
            digest[j * 4 + 0] = static_cast<std::uint8_t>((state_[j] >> 24) & 0xff);
            digest[j * 4 + 1] = static_cast<std::uint8_t>((state_[j] >> 16) & 0xff);
            digest[j * 4 + 2] = static_cast<std::uint8_t>((state_[j] >> 8) & 0xff);
            digest[j * 4 + 3] = static_cast<std::uint8_t>(state_[j] & 0xff);
        }
        return digest;
    }

private:
    void transform(const std::uint8_t* data)
    {
        std::uint32_t w[80];
        for (int i = 0; i < 16; ++i)
        {
            w[i] = (static_cast<std::uint32_t>(data[i * 4 + 0]) << 24) |
                   (static_cast<std::uint32_t>(data[i * 4 + 1]) << 16) |
                   (static_cast<std::uint32_t>(data[i * 4 + 2]) << 8) |
                   (static_cast<std::uint32_t>(data[i * 4 + 3]));
        }
        for (int i = 16; i < 80; ++i)
            w[i] = rotl(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);

        std::uint32_t a = state_[0];
        std::uint32_t b = state_[1];
        std::uint32_t c = state_[2];
        std::uint32_t d = state_[3];
        std::uint32_t e = state_[4];

        for (int i = 0; i < 80; ++i)
        {
            std::uint32_t f;
            std::uint32_t k;
            if (i < 20)
            {
                f = (b & c) | ((~b) & d);
                k = 0x5a827999;
            }
            else if (i < 40)
            {
                f = b ^ c ^ d;
                k = 0x6ed9eba1;
            }
            else if (i < 60)
            {
                f = (b & c) | (b & d) | (c & d);
                k = 0x8f1bbcdc;
            }
            else
            {
                f = b ^ c ^ d;
                k = 0xca62c1d6;
            }

            const std::uint32_t temp = rotl(a, 5) + f + e + k + w[i];
            e = d;
            d = c;
            c = rotl(b, 30);
            b = a;
            a = temp;
        }

        state_[0] += a;
        state_[1] += b;
        state_[2] += c;
        state_[3] += d;
        state_[4] += e;
    }

    static std::uint32_t rotl(std::uint32_t value, std::uint32_t count)
    {
        return (value << count) | (value >> (32 - count));
    }

    std::array<std::uint8_t, 64> buffer_{};
    std::size_t bufferSize_{ 0 };
    std::uint64_t bitLength_{ 0 };
    std::array<std::uint32_t, 5> state_{
        0x67452301,
        0xefcdab89,
        0x98badcfe,
        0x10325476,
        0xc3d2e1f0
    };
};

static std::string base64_encode(const std::array<std::uint8_t, 20>& data)
{
    static constexpr char alphabet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(28);

    std::uint32_t val = 0;
    int valb = -6;
    for (std::uint8_t c : data)
    {
        val = (val << 8) | c;
        valb += 8;
        while (valb >= 0)
        {
            out.push_back(alphabet[(val >> valb) & 0x3f]);
            valb -= 6;
        }
    }
    if (valb > -6)
        out.push_back(alphabet[((val << 8) >> (valb + 8)) & 0x3f]);
    while (out.size() % 4)
        out.push_back('=');
    return out;
}

static std::string websocket_accept_key(const std::string& client_key)
{
    Sha1 sha1;
    sha1.update(client_key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    return base64_encode(sha1.final());
}

static std::vector<std::uint8_t> websocket_text_frame(const std::string& payload)
{
    std::vector<std::uint8_t> frame;
    frame.reserve(payload.size() + 10);
    frame.push_back(0x81);

    if (payload.size() < 126)
    {
        frame.push_back(static_cast<std::uint8_t>(payload.size()));
    }
    else if (payload.size() <= 0xffff)
    {
        frame.push_back(126);
        frame.push_back(static_cast<std::uint8_t>((payload.size() >> 8) & 0xff));
        frame.push_back(static_cast<std::uint8_t>(payload.size() & 0xff));
    }
    else
    {
        frame.push_back(127);
        for (int shift = 56; shift >= 0; shift -= 8)
            frame.push_back(static_cast<std::uint8_t>((payload.size() >> shift) & 0xff));
    }

    frame.insert(frame.end(), payload.begin(), payload.end());
    return frame;
}

class WebSocketHub
{
public:
    void add_client(int fd)
    {
        std::scoped_lock lock{ mutex_ };
        clients_.push_back(fd);
    }

    void remove_client(int fd)
    {
        std::scoped_lock lock{ mutex_ };
        clients_.erase(std::remove(clients_.begin(), clients_.end(), fd), clients_.end());
    }

    void broadcast(const std::string& payload)
    {
        const auto frame = websocket_text_frame(payload);

        std::vector<int> snapshot;
        {
            std::scoped_lock lock{ mutex_ };
            snapshot = clients_;
        }

        std::vector<int> failed;
        for (int fd : snapshot)
        {
            if (!send_frame(fd, frame))
                failed.push_back(fd);
        }

        if (!failed.empty())
        {
            std::scoped_lock lock{ mutex_ };
            clients_.erase(std::remove_if(clients_.begin(), clients_.end(), [&](int fd)
            {
                return std::find(failed.begin(), failed.end(), fd) != failed.end();
            }), clients_.end());
        }
    }

private:
    static bool send_frame(int fd, const std::vector<std::uint8_t>& frame)
    {
        std::size_t sent = 0;
        while (sent < frame.size())
        {
            const auto n = ::send(fd, frame.data() + sent, frame.size() - sent, 0);
            if (n <= 0)
                return false;
            sent += static_cast<std::size_t>(n);
        }
        return true;
    }

    std::mutex mutex_;
    std::vector<int> clients_;
};

static bool consume_websocket_frames(int fd)
{
    while (true)
    {
        std::uint8_t header[2];
        const auto n = ::recv(fd, header, sizeof(header), MSG_WAITALL);
        if (n <= 0)
            return false;

        const std::uint8_t opcode = header[0] & 0x0f;
        std::uint64_t payload_length = header[1] & 0x7f;
        const bool masked = (header[1] & 0x80) != 0;

        if (payload_length == 126)
        {
            std::uint8_t ext[2];
            if (::recv(fd, ext, sizeof(ext), MSG_WAITALL) <= 0)
                return false;
            payload_length = (static_cast<std::uint64_t>(ext[0]) << 8) | ext[1];
        }
        else if (payload_length == 127)
        {
            std::uint8_t ext[8];
            if (::recv(fd, ext, sizeof(ext), MSG_WAITALL) <= 0)
                return false;
            payload_length = 0;
            for (std::uint8_t b : ext)
                payload_length = (payload_length << 8) | b;
        }

        std::uint8_t mask[4]{};
        if (masked)
        {
            if (::recv(fd, mask, sizeof(mask), MSG_WAITALL) <= 0)
                return false;
        }

        if (payload_length > 0)
        {
            std::vector<std::uint8_t> payload(payload_length);
            if (::recv(fd, payload.data(), payload.size(), MSG_WAITALL) <= 0)
                return false;

            if (masked)
            {
                for (std::size_t i = 0; i < payload.size(); ++i)
                    payload[i] ^= mask[i % 4];
            }
        }

        if (opcode == 0x8)
            return true;
    }
}

class ExchangeServer
{
public:
    void run(std::uint16_t port)
    {
        server_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (server_fd_ < 0)
            throw std::runtime_error("failed to create socket");

        int opt = 1;
        ::setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#ifdef SO_REUSEPORT
        ::setsockopt(server_fd_, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
#endif

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = INADDR_ANY;
        address.sin_port = htons(port);

        if (::bind(server_fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0)
            throw std::runtime_error("failed to bind socket");

        if (::listen(server_fd_, 64) < 0)
            throw std::runtime_error("failed to listen on socket");

        std::cout << "Exchange service listening on port " << port << std::endl;

        while (true)
        {
            sockaddr_in client_addr{};
            socklen_t client_len = sizeof(client_addr);
            const int client_fd = ::accept(server_fd_, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
            if (client_fd < 0)
                continue;

            std::thread(&ExchangeServer::handle_connection, this, client_fd).detach();
        }
    }

    ~ExchangeServer()
    {
        if (server_fd_ >= 0)
            ::close(server_fd_);
    }

private:
    struct OrderRequest
    {
        std::string ticker;
        Side side;
        Price price;
        Quantity qty;
    };

    std::shared_ptr<Orderbook> get_book(const std::string& ticker)
    {
        std::scoped_lock lock{ registry_mutex_ };
        auto& book = books_[ticker];
        if (!book)
            book = std::make_shared<Orderbook>();
        return book;
    }

    static std::vector<std::string> split_levels(const LevelInfos& infos, std::size_t max_levels)
    {
        std::vector<std::string> out;
        const auto limit = std::min(max_levels, infos.size());
        out.reserve(limit);
        for (std::size_t i = 0; i < limit; ++i)
        {
            std::ostringstream entry;
            entry << '{' << "\"price\":" << infos[i].price_ << ',' << "\"qty\":" << infos[i].quantity_ << '}';
            out.push_back(entry.str());
        }
        return out;
    }

    std::string serialize_book(const std::string& ticker)
    {
        auto book = get_book(ticker);
        auto infos = book->GetOrderInfos();

        auto bids = split_levels(infos.GetBids(), 10);
        auto asks = split_levels(infos.GetAsks(), 10);

        std::ostringstream out;
        out << '{'
            << "\"ticker\":\"" << json_escape(ticker) << "\",";
        out << "\"bids\":[";
        for (std::size_t i = 0; i < bids.size(); ++i)
        {
            if (i > 0)
                out << ',';
            out << bids[i];
        }
        out << "],";
        out << "\"asks\":[";
        for (std::size_t i = 0; i < asks.size(); ++i)
        {
            if (i > 0)
                out << ',';
            out << asks[i];
        }
        out << "]}";
        return out.str();
    }

    std::optional<OrderRequest> parse_order_request(const std::string& body)
    {
        auto ticker = extract_json_string(body, "ticker");
        auto side_text = extract_json_string(body, "side");
        auto price = extract_json_integer(body, "price");
        auto qty = extract_json_integer(body, "qty");

        if (!ticker || !side_text || !price || !qty)
            return std::nullopt;

        auto side = parse_side(*side_text);
        if (!side)
            return std::nullopt;

        return OrderRequest{ *ticker, *side, static_cast<Price>(*price), static_cast<Quantity>(*qty) };
    }

    void cleanup_filled_orders(const std::string& ticker, OrderId new_order_id, const Trades& trades, const std::shared_ptr<Orderbook>& book)
    {
        std::scoped_lock lock{ registry_mutex_ };

        if (!book->HasOrder(new_order_id))
            order_to_ticker_.erase(new_order_id);

        for (const auto& trade : trades)
        {
            if (!book->HasOrder(trade.GetBidTrade().orderId_))
                order_to_ticker_.erase(trade.GetBidTrade().orderId_);
            if (!book->HasOrder(trade.GetAskTrade().orderId_))
                order_to_ticker_.erase(trade.GetAskTrade().orderId_);
        }

        (void)ticker;
    }

    std::string handle_order(const HttpRequest& request)
    {
        auto parsed = parse_order_request(request.body);
        if (!parsed)
            return build_http_response(400, "Bad Request", "{\"error\":\"invalid order payload\"}");

        const OrderId order_id = next_order_id_.fetch_add(1, std::memory_order_relaxed);
        auto book = get_book(parsed->ticker);
        auto order = std::make_shared<Order>(OrderType::GoodTillCancel, order_id, parsed->side, parsed->price, parsed->qty);
        auto trades = book->AddOrder(order);

        {
            std::scoped_lock lock{ registry_mutex_ };
            order_to_ticker_[order_id] = parsed->ticker;
        }

        cleanup_filled_orders(parsed->ticker, order_id, trades, book);
        if (!trades.empty())
            ws_hub_.broadcast(make_fill_event_json(trades, parsed->ticker));

        std::ostringstream body;
        body << '{' << "\"order_id\":" << order_id << ',' << "\"ticker\":\"" << json_escape(parsed->ticker) << "\",";
        body << "\"trades\":";
        body << '[';
        for (std::size_t i = 0; i < trades.size(); ++i)
        {
            if (i > 0)
                body << ',';
            const auto& trade = trades[i];
            body << '{'
                 << "\"buy_order_id\":" << trade.GetBidTrade().orderId_ << ','
                 << "\"sell_order_id\":" << trade.GetAskTrade().orderId_ << ','
                 << "\"price\":" << trade.GetAskTrade().price_ << ','
                 << "\"qty\":" << trade.GetAskTrade().quantity_
                 << '}';
        }
        body << "]}";

        return build_http_response(200, "OK", body.str());
    }

    std::string handle_cancel(const std::string& path)
    {
        const auto parts = split_path(path);
        if (parts.size() != 2)
            return build_http_response(404, "Not Found", "{\"error\":\"unknown order route\"}");

        try
        {
            const auto order_id = static_cast<OrderId>(std::stoull(parts[1]));
            std::string ticker;
            {
                std::scoped_lock lock{ registry_mutex_ };
                auto it = order_to_ticker_.find(order_id);
                if (it == order_to_ticker_.end())
                    return build_http_response(404, "Not Found", "{\"error\":\"order not found\"}");
                ticker = it->second;
            }

            auto book = get_book(ticker);
            book->CancelOrder(order_id);

            {
                std::scoped_lock lock{ registry_mutex_ };
                order_to_ticker_.erase(order_id);
            }

            std::ostringstream body;
            body << '{' << "\"order_id\":" << order_id << ',' << "\"status\":\"cancelled\"}";
            return build_http_response(200, "OK", body.str());
        }
        catch (...)
        {
            return build_http_response(400, "Bad Request", "{\"error\":\"invalid order id\"}");
        }
    }

    std::string handle_get_book(const std::string& path)
    {
        const auto parts = split_path(path);
        if (parts.size() != 2)
            return build_http_response(404, "Not Found", "{\"error\":\"unknown book route\"}");

        auto body = serialize_book(parts[1]);
        return build_http_response(200, "OK", body);
    }

    static std::string extract_websocket_key(const HttpRequest& request)
    {
        auto it = request.headers.find("sec-websocket-key");
        if (it == request.headers.end())
            return {};
        return it->second;
    }

    void handle_websocket(int client_fd, const HttpRequest& request)
    {
        const auto key = extract_websocket_key(request);
        if (key.empty())
        {
            ::close(client_fd);
            return;
        }

        std::ostringstream response;
        response << "HTTP/1.1 101 Switching Protocols\r\n";
        response << "Upgrade: websocket\r\n";
        response << "Connection: Upgrade\r\n";
        response << "Sec-WebSocket-Accept: " << websocket_accept_key(key) << "\r\n\r\n";
        if (!send_all(client_fd, response.str()))
        {
            ::close(client_fd);
            return;
        }

        ws_hub_.add_client(client_fd);
        std::thread([this, client_fd]
        {
            consume_websocket_frames(client_fd);
            ws_hub_.remove_client(client_fd);
            ::close(client_fd);
        }).detach();
    }

    void handle_connection(int client_fd)
    {
        auto request = read_http_request(client_fd);
        if (!request)
        {
            ::close(client_fd);
            return;
        }

        const auto upgrade_it = request->headers.find("upgrade");
        if (request->path == "/stream" && upgrade_it != request->headers.end() && to_lower(upgrade_it->second) == "websocket")
        {
            handle_websocket(client_fd, *request);
            return;
        }

        std::string response;
        if (request->method == "POST" && request->path == "/order")
        {
            response = handle_order(*request);
        }
        else if (request->method == "DELETE" && request->path.rfind("/order/", 0) == 0)
        {
            response = handle_cancel(request->path);
        }
        else if (request->method == "GET" && request->path.rfind("/book/", 0) == 0)
        {
            response = handle_get_book(request->path);
        }
        else if (request->method == "GET" && request->path == "/health")
        {
            response = build_http_response(200, "OK", "{\"status\":\"ok\"}");
        }
        else
        {
            response = build_http_response(404, "Not Found", "{\"error\":\"unknown route\"}");
        }

        send_all(client_fd, response);
        ::close(client_fd);
    }

    int server_fd_{ -1 };
    std::unordered_map<std::string, std::shared_ptr<Orderbook>> books_;
    std::unordered_map<OrderId, std::string> order_to_ticker_;
    std::mutex registry_mutex_;
    WebSocketHub ws_hub_;
    std::atomic<OrderId> next_order_id_{ 1 };
};

int main()
{
    try
    {
        ExchangeServer server;
        server.run(8080);
    }
    catch (const std::exception& ex)
    {
        std::cerr << "server failed: " << ex.what() << std::endl;
        return 1;
    }

    return 0;
}
