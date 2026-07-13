#include "thread_worker.hpp"
#include "rest_bot.hpp"
#include "grpc_telemetry.hpp"
#include <boost/asio/buffers_iterator.hpp>
#include <boost/json.hpp>
#include <iostream>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <pthread.h>
#include <sched.h>
// Awaitable operator overloads (e.g. a || b)
#include <boost/asio/experimental/awaitable_operators.hpp>
using namespace boost::asio::experimental::awaitable_operators;

using namespace std::chrono_literals;

static std::size_t parse_content_length(const std::string &header);

namespace json = boost::json;

// ─── Response parsing (real JSON, not hand-rolled string scanning) ────────────
// The observation stream is the ground truth for "what the contestant did", so it
// must be parsed with a conforming JSON reader: nested objects, arrays, scientific
// notation, and arbitrary whitespace all parse correctly, and a malformed body is
// detected (parsed == false) instead of silently mis-scanned into a bogus fill.
struct ParsedTrade
{
    uint64_t buy_order_id = 0;
    uint64_t sell_order_id = 0;
    double price = 0.0;
    int qty = 0;
};

struct ParsedResponse
{
    bool parsed = false;         // body was a well-formed JSON object
    bool has_order_id = false;   // a usable (non-zero) server order_id was present
    uint64_t order_id = 0;
    std::vector<ParsedTrade> trades;
};

// Coerce a JSON value that may be a number OR a numeric string into a uint64.
// Contestant servers vary: some emit order_id as 42, others as "42".
static uint64_t json_to_u64(const json::value &v)
{
    switch (v.kind())
    {
    case json::kind::int64:
        return v.get_int64() < 0 ? 0 : static_cast<uint64_t>(v.get_int64());
    case json::kind::uint64:
        return v.get_uint64();
    case json::kind::double_:
        return static_cast<uint64_t>(v.get_double());
    case json::kind::string:
        try { return std::stoull(std::string(v.get_string())); } catch (...) { return 0; }
    default:
        return 0;
    }
}

static double json_to_double(const json::value &v)
{
    switch (v.kind())
    {
    case json::kind::double_:
        return v.get_double();
    case json::kind::int64:
        return static_cast<double>(v.get_int64());
    case json::kind::uint64:
        return static_cast<double>(v.get_uint64());
    case json::kind::string:
        try { return std::stod(std::string(v.get_string())); } catch (...) { return 0.0; }
    default:
        return 0.0;
    }
}

static ParsedResponse parse_response(const std::string &body)
{
    ParsedResponse out;
    if (body.empty())
        return out;

    json::value root;
    try
    {
        root = json::parse(body);
    }
    catch (const std::exception &)
    {
        return out; // malformed → parsed stays false (caller treats as unknown)
    }
    if (!root.is_object())
        return out;

    out.parsed = true;
    const json::object &obj = root.as_object();

    if (const json::value *oid = obj.if_contains("order_id"))
    {
        out.order_id = json_to_u64(*oid);
        out.has_order_id = (out.order_id != 0);
    }

    if (const json::value *tr = obj.if_contains("trades"); tr && tr->is_array())
    {
        for (const json::value &item : tr->as_array())
        {
            if (!item.is_object())
                continue;
            const json::object &to = item.as_object();
            ParsedTrade trade;
            if (const json::value *p = to.if_contains("buy_order_id"))
                trade.buy_order_id = json_to_u64(*p);
            if (const json::value *p = to.if_contains("sell_order_id"))
                trade.sell_order_id = json_to_u64(*p);
            if (const json::value *p = to.if_contains("price"))
                trade.price = json_to_double(*p);
            if (const json::value *p = to.if_contains("qty"))
                trade.qty = static_cast<int>(json_to_double(*p));
            else if (const json::value *p2 = to.if_contains("quantity"))
                trade.qty = static_cast<int>(json_to_double(*p2));
            out.trades.push_back(trade);
        }
    }

    return out;
}

ThreadWorker::ThreadWorker(int thread_id,
                           int bots_this_thread,
                           const BenchmarkConfig &cfg,
                           std::shared_ptr<GrpcTelemetryClient> grpc_client)
    : thread_id_(thread_id), bots_this_thread_(bots_this_thread),
      cfg_(cfg), grpc_client_(std::move(grpc_client)), ioc_(1)
{
}

void ThreadWorker::start()
{
    running_ = true;

    thread_ = std::jthread([this]()
                           {
        // ── Pin this thread to a specific CPU core ────────────────────────────
        // Thread 0 → core 0, Thread 1 → core 1, etc.
        // Prevents OS scheduler from migrating the thread mid-benchmark,
        // which would invalidate our latency measurements.
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(thread_id_, &cpuset);
        pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);

        std::cout << "[Worker " << thread_id_ << "] Starting "
                  << bots_this_thread_ << " bots on core " << thread_id_ << "\n";

        // ── Open the gRPC stream for this benchmark ──────────────────────────
        try
        {
            grpc_stream_ = grpc_client_->open_stream();
            std::cout << "[Worker " << thread_id_ << "] gRPC stream opened\n";
        }
        catch (const std::exception &e)
        {
            std::cerr << "[Worker " << thread_id_ << "] gRPC stream open failed: " << e.what() << "\n";
        }

        // ── Spawn all bot coroutines into this thread's event loop ────────────
        for (int i = 0; i < bots_this_thread_; ++i) {
            uint64_t bot_id = static_cast<uint64_t>(thread_id_) * 10000 + i;
            asio::co_spawn(ioc_, run_bot(bot_id), asio::detached);
        }

        // ── Spawn the telemetry flush coroutine ───────────────────────────────
        asio::co_spawn(ioc_, flush_loop(), asio::detached);

        // ── Run the event loop — blocks until ioc_.stop() is called ──────────
        // io_uring handles ALL I/O multiplexing inside here.
        // No threads are blocked waiting for sockets. Ever.
        ioc_.run();

        // ── End-of-run marker: only the worker that actually ran the serialized
        //    correctness writer emits it, so the checker finalizes the verdict
        //    exactly once. Recorded here (after the event loop stops) so it can
        //    never be lost to a suspended coroutine.
        if (cfg_.mode == "correctness" && bots_this_thread_ > 0)
        {
            audit_log_.record_end_of_run(cfg_.submission_id, next_seq_++);
        }

        // ── Final flush: send any remaining events before closing ─────────────
        if (grpc_stream_ && (!audit_log_.empty() || counters_.latency_samples > 0))
        {
            grpc_client_->send_batch(*grpc_stream_, audit_log_, counters_,
                                     cfg_.submission_id, thread_id_);
            audit_log_.clear();
            counters_.reset();
        }

        // ── Close the gRPC stream ─────────────────────────────────────────────
        if (grpc_stream_)
        {
            auto response = grpc_client_->finish(*grpc_stream_);
            std::cout << "[Worker " << thread_id_ << "] gRPC stream closed\n";
            grpc_stream_.reset();
        }

        std::cout << "[Worker " << thread_id_ << "] Stopped.\n"; });
}

void ThreadWorker::stop()
{
    running_ = false;
    ioc_.stop();
}

void ThreadWorker::join()
{
    if (thread_.joinable())
        thread_.join();
}

// ─────────────────────────────────────────────────────────────────────────────
// run_bot — one coroutine per bot
//
// Connects once, then hammers orders until duration expires.
// co_await suspends this coroutine without blocking the thread.
// The thread moves to the next coroutine instantly.
// ─────────────────────────────────────────────────────────────────────────────
asio::awaitable<void> ThreadWorker::run_bot(uint64_t bot_id)
{
    auto executor = co_await asio::this_coro::executor;

    // Connect with a 5s timeout
    tcp::socket socket(executor);
    tcp::resolver resolver(executor);

    try
    {
        auto endpoints = co_await resolver.async_resolve(
            cfg_.target_host, cfg_.target_port, asio::use_awaitable);
        co_await asio::async_connect(socket, endpoints, asio::use_awaitable);
        socket.set_option(tcp::no_delay(true)); // disable Nagle for lower latency

        co_await send_orders(socket, bot_id);
    }
    catch (const std::exception &e)
    {
        // ECONNREFUSED or DNS failure
        std::cerr << "[run_bot] EXCEPTION ESCAPED! " << e.what() << "\n";
        ++counters_.counts[ECONNREF];
        // Don't crash — bot silently exits its coroutine
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// send_orders — the inner hot loop
//
// Fires one request, awaits response, records latency, repeat.
// Captures Intent (OrderSubmitted) BEFORE sending, and Observation
// (TradeExecuted, unrolled) AFTER receiving response.
// ─────────────────────────────────────────────────────────────────────────────
asio::awaitable<void> ThreadWorker::send_orders(tcp::socket &socket,
                                                uint64_t bot_id)
{
    auto executor = co_await asio::this_coro::executor;

    const bool correctness = (cfg_.mode == "correctness");
    RestBot bot(cfg_.target_host, cfg_.target_port, bot_id, cfg_.seed);
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(cfg_.duration_secs);

    while (running_ && std::chrono::steady_clock::now() < deadline)
    {
        std::string request = bot.generate_request();
        const auto &last = bot.last_order();

        // Reserve this order's sequence number up front (correctness mode only),
        // so the intent orders before its own fills even though the intent is
        // recorded after the response (once its server id is known). Recording
        // post-response also avoids the flush_loop clearing the audit buffer
        // between the intent and its server-id attachment.
        const uint64_t order_seq = correctness ? next_seq_++ : 0;

        // Record exactly one order entry per attempt (correctness mode) tagged with
        // its outcome, so a lost or rejected response is never a silent hole in the
        // seq stream that the checker would otherwise read as a false verdict.
        // contestant_order_id is 0 unless the server returned a usable id.
        auto emit_intent = [&](OrderOutcome outcome, uint64_t contestant_order_id)
        {
            if (!correctness)
                return;
            uint64_t cancel_target_id = 0;
            if (last.type == "CANCEL")
            {
                try { cancel_target_id = std::stoull(last.order_id); }
                catch (...) {}
            }
            audit_log_.record_order(
                cfg_.submission_id,
                static_cast<int32_t>(bot_id),
                last.order_id,
                last.type,                                // action = LIMIT | MARKET | CANCEL | FOK | FAK | GFD
                last.type == "CANCEL" ? "" : last.action, // side = BUY | SELL (empty for CANCEL)
                last.ticker,
                last.price,
                last.quantity,
                cancel_target_id,
                order_seq,
                contestant_order_id,
                outcome);
        };

        // ── Record start time (nanosecond precision) ──────────────────────────
        auto t0 = std::chrono::steady_clock::now();

        try
        {
            // ── Write request to socket (async, non-blocking) ─────────────────
            co_await asio::async_write(socket,
                                       asio::buffer(request), asio::use_awaitable);

            // ── Read response headers ─────────────────────────────────────────
            // Correctness mode: 5s timeout — 1 bot with minimal load, so a
            // slow response is likely a transient network hiccup rather than a
            // hung server. A 1s timeout risks a ghost order: the server processes
            // the request but the bot times out, moves to the next order, and the
            // shadow engine diverges from the real order book → wrong INCORRECT
            // verdict on a correct submission.
            // Performance mode: 1s timeout — we want to detect slow servers fast
            // so bots can keep saturating throughput measurements.
            const auto request_timeout = (cfg_.mode == "correctness") ? 5s : 1s;
            asio::steady_timer timeout_timer(executor, request_timeout);
            asio::streambuf response_buf;

            bool timed_out = false;
            std::size_t header_bytes = 0;
            try
            {
                auto res = co_await (
                    asio::async_read_until(socket, response_buf, "\r\n\r\n", asio::use_awaitable) ||
                    timeout_timer.async_wait(asio::use_awaitable));
                if (res.index() == 0)
                    header_bytes = std::get<0>(res);
                else
                    timed_out = true;
            }
            catch (const boost::system::system_error &e)
            {
                ++counters_.counts[OTHER_ERR];
                emit_intent(OrderOutcome::Unknown, 0); // response lost mid-read
                continue;
            }

            if (timed_out)
            {
                record(0, 0.0, true); // count as TIMEOUT only — not a latency sample
                emit_intent(OrderOutcome::Unknown, 0); // server may have applied it
                continue;
            }

            std::string header;
            header.resize(header_bytes);
            std::istream header_stream(&response_buf);
            header_stream.read(&header[0], static_cast<std::streamsize>(header_bytes));

            int status = parse_status_code(header);
            std::size_t content_length = parse_content_length(header);

            // Drain any response body so the keep-alive socket stays in sync.
            std::size_t buffered = response_buf.size();
            if (buffered < content_length)
            {
                const std::size_t remaining = content_length - buffered;
                try
                {
                    co_await asio::async_read(
                        socket,
                        response_buf,
                        asio::transfer_exactly(remaining),
                        asio::use_awaitable);
                }
                catch (const boost::system::system_error &e)
                {
                    ++counters_.counts[OTHER_ERR];
                    emit_intent(OrderOutcome::Unknown, 0); // body lost mid-read
                    continue;
                }
            }

            auto t1 = std::chrono::steady_clock::now();
            double lat = std::chrono::duration<double, std::milli>(t1 - t0).count();

            std::string response_body;
            if (response_buf.size() > 0)
            {
                response_body.assign(
                    asio::buffers_begin(response_buf.data()),
                    asio::buffers_end(response_buf.data()));
            }

            // ── Stream A: latency/throughput metrics ─────────────────────────
            record(status, lat, false);

            // ── Stream B: OBSERVATION — classify outcome, capture ground truth ─
            if (status == 200)
            {
                ParsedResponse resp = parse_response(response_body);
                if (!resp.parsed || !resp.has_order_id)
                {
                    // Server answered 200 but the body could not be parsed or carried
                    // no usable order_id: it applied *something* we cannot map, so the
                    // outcome is unknowable → force Unverified downstream.
                    emit_intent(OrderOutcome::Unknown, 0);
                }
                else
                {
                    const uint64_t contestant_order_id = resp.order_id;
                    emit_intent(OrderOutcome::Ok, contestant_order_id);

                    // Unroll each trade from the parsed trades[] array (correctness only —
                    // the performance run emits metrics only, keeping the stream light).
                    if (correctness)
                    {
                        for (const auto &trade : resp.trades)
                        {
                            // Determine which side is the "matched" resting order.
                            const uint64_t matched_id = (trade.buy_order_id == contestant_order_id)
                                                            ? trade.sell_order_id
                                                            : trade.buy_order_id;

                            audit_log_.record_trade(
                                cfg_.submission_id,
                                static_cast<int32_t>(bot_id),
                                last.ticker,
                                contestant_order_id,
                                matched_id,
                                trade.price,
                                trade.qty,
                                last.order_id, // join key: bot-generated aggressor order_id
                                next_seq_++);
                        }
                    }

                    // Feed server-assigned order IDs back to the bot for the cancel flow
                    // (needed in both modes so the bot can generate valid cancels).
                    if (bot.order_type() != OrderType::CANCEL)
                        bot.record_accepted_order(contestant_order_id);
                }
            }
            else if (status >= 400 && status < 500)
            {
                // Clean client-side rejection: the server did not apply the order, so
                // its book is unchanged. Record REJECTED — replay skips it as a no-op
                // while the seq stays contiguous (no over-conservative Unverified).
                emit_intent(OrderOutcome::Rejected, 0);
            }
            else
            {
                // 5xx, a 0/garbage status, or any other non-200: the server may or may
                // not have applied the order — unknowable → Unverified downstream.
                emit_intent(OrderOutcome::Unknown, 0);
            }

            response_buf.consume(response_buf.size());
        }
        catch (const boost::system::system_error &e)
        {
            ++counters_.counts[OTHER_ERR];
            emit_intent(OrderOutcome::Unknown, 0); // request write failed / unexpected
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// flush_loop — async timer, fires every 500ms
//
// We use boost::asio::steady_timer instead of sleep_for() so the thread
// is NEVER blocked — it stays in the event loop processing bot sockets
// while the timer is pending.
//
// Sends AuditLog (orders + trades) and TelemetryCounters via gRPC as a
// single compressed protobuf AuditBatch.
// ─────────────────────────────────────────────────────────────────────────────
asio::awaitable<void> ThreadWorker::flush_loop()
{
    auto executor = co_await asio::this_coro::executor;
    asio::steady_timer timer(executor);

    while (running_)
    {
        timer.expires_after(std::chrono::milliseconds(cfg_.flush_interval_ms));
        co_await timer.async_wait(asio::use_awaitable);

        // Send the audit batch via gRPC
        if (grpc_stream_ && (!audit_log_.empty() || counters_.latency_samples > 0))
        {
            bool ok = grpc_client_->send_batch(*grpc_stream_, audit_log_, counters_,
                                                cfg_.submission_id, thread_id_);
            if (!ok)
            {
                std::cerr << "[Worker " << thread_id_ << "] gRPC batch send failed, "
                          << "reopening stream\n";
                try
                {
                    grpc_stream_ = grpc_client_->open_stream();
                }
                catch (const std::exception &e)
                {
                    std::cerr << "[Worker " << thread_id_ << "] gRPC stream reopen failed: "
                              << e.what() << "\n";
                    grpc_stream_.reset();
                }
            }
        }

        // Reset counters and audit log for next window
        audit_log_.clear();
        counters_.reset();
    }
}

static std::size_t parse_content_length(const std::string &header)
{
    std::string lower = header;
    std::transform(lower.begin(), lower.end(), lower.begin(),
                   [](unsigned char c)
                   { return static_cast<char>(std::tolower(c)); });

    const std::string key = "content-length:";
    const auto pos = lower.find(key);
    if (pos == std::string::npos)
        return 0;

    auto start = pos + key.size();
    while (start < lower.size() && std::isspace(static_cast<unsigned char>(lower[start])))
        ++start;

    auto end = lower.find("\r\n", start);
    const auto len_text = lower.substr(start, end - start);
    try
    {
        return static_cast<std::size_t>(std::stoul(len_text));
    }
    catch (...)
    {
        return 0;
    }
}

int ThreadWorker::parse_status_code(const std::string &response)
{
    // HTTP/1.1 200 OK  →  parse the 3-digit code after "HTTP/1.x "
    if (response.size() < 12)
        return 0;
    try
    {
        return std::stoi(response.substr(9, 3));
    }
    catch (...)
    {
        return 0;
    }
}

void ThreadWorker::record(int status_code, double latency_ms, bool timed_out)
{
    if (timed_out)
    {
        // A timeout is an error, not a served request. Count it in the TIMEOUT
        // bucket only — do NOT feed it into the latency histogram, or percentiles
        // would be dominated by timeout penalties instead of real served latency.
        ++counters_.counts[TIMEOUT];
        return;
    }
    if (status_code == 200)
        ++counters_.counts[HTTP_200];
    else if (status_code >= 400 && status_code < 500)
        ++counters_.counts[HTTP_4XX];
    else if (status_code >= 500)
        ++counters_.counts[HTTP_5XX];
    else
        ++counters_.counts[OTHER_ERR];

    counters_.record_latency(latency_ms);
}
