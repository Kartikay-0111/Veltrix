#include "correctness_engine.hpp"
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include <sstream>
#include <thread>
#include <chrono>
#include <regex>

using namespace std::chrono_literals;

CorrectnessEngine::CorrectnessEngine(const std::string &sandbox_host,
                                     const std::string &sandbox_port)
    : sandbox_host_(sandbox_host), sandbox_port_(sandbox_port)
{
}

bool CorrectnessEngine::run_check(const std::string &ticker)
{
    std::cout << "[Correctness] Starting check for ticker=" << ticker << "\n";

    // Fetch a baseline snapshot to choose a safe price and baseline quantity.
    std::string pre_snapshot = fetch_book_snapshot(ticker);
    auto bids = parse_levels(pre_snapshot, "bids");
    auto asks = parse_levels(pre_snapshot, "asks");
    const int price = choose_watermark_price(bids, asks);
    int baseline_qty = 0;
    if (auto level = find_level(bids, price))
        baseline_qty = level->quantity;

    // ── Step 1: Inject watermark order at $0.01 ───────────────────────────────
    uint64_t order_id = inject_watermark(ticker, price, 1);
    if (order_id == 0)
    {
        std::cerr << "[Correctness] Failed to inject watermark — FAIL\n";
        last_result_ = false;
        return false;
    }
    shadow_book_.record_watermark(ticker, price, 1, order_id, baseline_qty);

    // ── Step 2: Wait 200ms for engine to process ──────────────────────────────
    std::this_thread::sleep_for(200ms);

    // ── Step 3: Fetch book snapshot ───────────────────────────────────────────
    std::string snapshot = fetch_book_snapshot(ticker);
    if (snapshot.empty())
    {
        std::cerr << "[Correctness] Empty snapshot — FAIL\n";
        last_result_ = false;
        cancel_watermark(order_id);
        return false;
    }

    // ── Step 4: Verify watermark appears in the book ──────────────────────────
    bool correct = shadow_book_.verify_snapshot(ticker, snapshot);
    last_result_ = correct;

    // ── Step 5: Clean up — cancel watermark order ─────────────────────────────
    cancel_watermark(order_id);

    // Expire any old watermarks
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::system_clock::now().time_since_epoch())
                      .count();
    shadow_book_.expire_watermarks(now_ms);

    return correct;
}

uint64_t CorrectnessEngine::inject_watermark(const std::string &ticker,
                                             int price,
                                             int qty)
{
    std::ostringstream body;
    body << "{"
         << "\"ticker\":\"" << ticker << "\","
         << "\"side\":\"buy\","
         << "\"price\":" << price << ","
         << "\"qty\":" << qty
         << "}";

    std::string response = http_post("/order", body.str());
    if (response.empty())
        return 0;

    return parse_order_id(response);
}

std::string CorrectnessEngine::fetch_book_snapshot(const std::string &ticker)
{
    return http_get("/book/" + ticker);
}

void CorrectnessEngine::cancel_watermark(uint64_t order_id)
{
    if (order_id == 0)
        return;
    http_delete("/order/" + std::to_string(order_id));
}

// ── Minimal synchronous HTTP over raw BSD sockets ────────────────────────────
// We deliberately avoid Boost.Asio here. The correctness engine runs once
// every 10 seconds — a blocking socket call is perfectly fine.

static int connect_to(const std::string &host, const std::string &port)
{
    struct addrinfo hints{}, *res = nullptr;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    if (getaddrinfo(host.c_str(), port.c_str(), &hints, &res) != 0)
        return -1;

    int fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd < 0)
    {
        freeaddrinfo(res);
        return -1;
    }

    // 2-second connect timeout
    struct timeval tv{2, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    if (connect(fd, res->ai_addr, res->ai_addrlen) != 0)
    {
        freeaddrinfo(res);
        close(fd);
        return -1;
    }
    freeaddrinfo(res);
    return fd;
}

static std::string read_http_body(int fd)
{
    std::string raw;
    char buf[4096];
    ssize_t n;
    while ((n = recv(fd, buf, sizeof(buf) - 1, 0)) > 0)
    {
        buf[n] = '\0';
        raw += buf;
    }
    // Strip HTTP headers — body starts after \r\n\r\n
    auto pos = raw.find("\r\n\r\n");
    if (pos == std::string::npos)
        return raw;
    return raw.substr(pos + 4);
}

std::string CorrectnessEngine::http_post(const std::string &path,
                                         const std::string &body)
{
    int fd = connect_to(sandbox_host_, sandbox_port_);
    if (fd < 0)
        return "";

    std::ostringstream req;
    req << "POST " << path << " HTTP/1.0\r\n"
        << "Host: " << sandbox_host_ << "\r\n"
        << "Content-Type: application/json\r\n"
        << "Content-Length: " << body.size() << "\r\n"
        << "\r\n"
        << body;

    std::string req_str = req.str();
    send(fd, req_str.c_str(), req_str.size(), 0);

    std::string response = read_http_body(fd);
    close(fd);
    return response;
}

std::string CorrectnessEngine::http_get(const std::string &path)
{
    int fd = connect_to(sandbox_host_, sandbox_port_);
    if (fd < 0)
        return "";

    std::ostringstream req;
    req << "GET " << path << " HTTP/1.0\r\n"
        << "Host: " << sandbox_host_ << "\r\n"
        << "\r\n";

    std::string req_str = req.str();
    send(fd, req_str.c_str(), req_str.size(), 0);

    std::string response = read_http_body(fd);
    close(fd);
    return response;
}

void CorrectnessEngine::http_delete(const std::string &path)
{
    int fd = connect_to(sandbox_host_, sandbox_port_);
    if (fd < 0)
        return;

    std::ostringstream req;
    req << "DELETE " << path << " HTTP/1.0\r\n"
        << "Host: " << sandbox_host_ << "\r\n"
        << "\r\n";

    std::string req_str = req.str();
    send(fd, req_str.c_str(), req_str.size(), 0);
    close(fd);
}

uint64_t CorrectnessEngine::parse_order_id(const std::string &json)
{
    // Match "order_id": 12345 or "id": 12345
    std::regex re("\"(?:order_id|id)\"\\s*:\\s*(\\d+)");
    std::smatch m;
    if (std::regex_search(json, m, re))
    {
        return std::stoull(m[1].str());
    }
    return 0;
}

std::vector<BookLevel> CorrectnessEngine::parse_levels(
    const std::string &json, const std::string &key) const
{
    std::vector<BookLevel> levels;
    if (json.empty())
        return levels;

    std::regex block_re("\"" + key + "\"\\s*:\\s*\\[([^\\]]*)\\]");
    std::smatch block_match;
    if (!std::regex_search(json, block_match, block_re))
        return levels;

    std::string content = block_match[1].str();
    std::regex level_re("\"price\"\\s*:\\s*([0-9.]+).*?\"qty\"\\s*:\\s*(\\d+)");
    auto begin = std::sregex_iterator(content.begin(), content.end(), level_re);
    auto end = std::sregex_iterator();

    for (auto it = begin; it != end; ++it)
    {
        BookLevel l;
        l.price = std::stod((*it)[1].str());
        l.quantity = std::stoi((*it)[2].str());
        levels.push_back(l);
    }

    return levels;
}

std::optional<BookLevel> CorrectnessEngine::find_level(
    const std::vector<BookLevel> &levels, int price) const
{
    for (const auto &l : levels)
    {
        if (static_cast<int>(l.price) == price)
            return l;
    }
    return std::nullopt;
}

int CorrectnessEngine::choose_watermark_price(
    const std::vector<BookLevel> &bids,
    const std::vector<BookLevel> &asks) const
{
    if (!asks.empty())
    {
        int best_ask = static_cast<int>(asks.front().price);
        int price = best_ask - 1;
        return price < 0 ? 0 : price;
    }

    if (!bids.empty())
    {
        int best_bid = static_cast<int>(bids.front().price);
        return best_bid + 1;
    }

    return 1;
}
