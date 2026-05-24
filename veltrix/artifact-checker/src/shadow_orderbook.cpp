#include "shadow_orderbook.hpp"
#include <regex>
#include <iostream>
#include <cmath>
#include <chrono>
#include <algorithm>

void ShadowOrderbook::record_watermark(const std::string &ticker,
                                       double price,
                                       int qty,
                                       uint64_t order_id,
                                       int baseline_qty)
{
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::system_clock::now().time_since_epoch())
                      .count();

    watermarks_.push_back({ticker, price, qty, order_id, now_ms, baseline_qty, false});
    std::cout << "[ShadowBook] Watermark recorded: "
              << ticker << " BUY @$" << price
              << " qty=" << qty
              << " id=" << order_id << "\n";
}

bool ShadowOrderbook::verify_snapshot(const std::string &ticker,
                                      const std::string &snapshot_json) const
{
    auto bids = parse_bids(snapshot_json);
    if (bids.empty())
    {
        std::cerr << "[ShadowBook] Could not parse bids from snapshot\n";
        return false;
    }

    // Find any watermark for this ticker
    for (const auto &wm : watermarks_)
    {
        if (wm.ticker != ticker || wm.verified)
            continue;

        auto level = find_level(bids, wm.price);
        if (level.has_value() && level->quantity >= (wm.baseline_qty + wm.qty))
        {
            std::cout << "[ShadowBook] ✓ Watermark $" << wm.price
                      << " found in book — correctness PASS\n";
            return true;
        }
        else
        {
            std::cerr << "[ShadowBook] ✗ Watermark $" << wm.price
                      << " NOT found in book — correctness FAIL\n";
            return false;
        }
    }

    // No pending watermarks — can't verify, assume correct
    return true;
}

void ShadowOrderbook::expire_watermarks(int64_t now_ms, int64_t max_age_ms)
{
    watermarks_.erase(
        std::remove_if(watermarks_.begin(), watermarks_.end(),
                       [&](const WatermarkEntry &w)
                       {
                           return (now_ms - w.injected_at_ms) > max_age_ms;
                       }),
        watermarks_.end());
}

std::vector<BookLevel> ShadowOrderbook::parse_bids(const std::string &json) const
{
    std::vector<BookLevel> levels;

    // Match the "bids" array in JSON
    // Expected: {"bids":[{"price":100.0,"qty":10},...],"asks":[...]}
    std::regex bids_block_re("\"bids\"\\s*:\\s*\\[([^\\]]*)\\]");
    std::smatch block_match;
    if (!std::regex_search(json, block_match, bids_block_re))
        return levels;

    std::string bids_content = block_match[1].str();

    // Match individual {price:X, qty:Y} objects
    std::regex level_re("\"price\"\\s*:\\s*([0-9.]+).*?\"qty\"\\s*:\\s*(\\d+)");
    auto begin = std::sregex_iterator(bids_content.begin(),
                                      bids_content.end(), level_re);
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

std::optional<BookLevel> ShadowOrderbook::find_level(
    const std::vector<BookLevel> &levels, double price) const
{
    constexpr double EPSILON = 0.0001;
    for (const auto &l : levels)
    {
        if (std::abs(l.price - price) < EPSILON)
            return l;
    }
    return std::nullopt;
}
