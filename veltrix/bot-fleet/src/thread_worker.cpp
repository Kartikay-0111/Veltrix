#include "thread_worker.hpp"
#include "rest_bot.hpp"
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

ThreadWorker::ThreadWorker(int thread_id,
                           int bots_this_thread,
                           const BenchmarkConfig &cfg,
                           std::shared_ptr<TelemetryProducer> producer)
    : thread_id_(thread_id), bots_this_thread_(bots_this_thread), cfg_(cfg), producer_(std::move(producer)), ioc_(1) // 1 thread per io_context → maps to 1 io_uring instance
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
        ++counters_.counts[ECONNREF];
        // Don't crash — bot silently exits its coroutine
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// send_orders — the inner hot loop
//
// Fires one request, awaits response, records latency, repeat.
// The co_await calls are what make this non-blocking — the thread
// is free to process other bots while waiting for network I/O.
// ─────────────────────────────────────────────────────────────────────────────
asio::awaitable<void> ThreadWorker::send_orders(tcp::socket &socket,
                                                uint64_t bot_id)
{
    auto executor = co_await asio::this_coro::executor;

    RestBot bot(cfg_.target_host, cfg_.target_port, bot_id);
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(cfg_.duration_secs);

    while (running_ && std::chrono::steady_clock::now() < deadline)
    {
        std::string request = bot.generate_request();

        // ── Record start time (nanosecond precision) ──────────────────────────
        auto t0 = std::chrono::steady_clock::now();

        try
        {
            // ── Write request to socket (async, non-blocking) ─────────────────
            co_await asio::async_write(socket,
                                       asio::buffer(request), asio::use_awaitable);

            // ── Read response headers with a 1s timeout ──────────────────────
            asio::steady_timer timeout_timer(executor, 1s);
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
                continue;
            }

            auto t1 = std::chrono::steady_clock::now();
            double lat = std::chrono::duration<double, std::milli>(t1 - t0).count();

            if (timed_out)
            {
                ++counters_.counts[TIMEOUT];
                counters_.record_latency(1000.0); // penalise timeout as 1s
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
                    continue;
                }
            }
            response_buf.consume(response_buf.size());

            record(status, lat, false);
        }
        catch (const boost::system::system_error &e)
        {
            ++counters_.counts[OTHER_ERR];
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// flush_loop — async timer, fires every 500ms
//
// We use boost::asio::steady_timer instead of sleep_for() so the thread
// is NEVER blocked — it stays in the event loop processing bot sockets
// while the timer is pending.
// ─────────────────────────────────────────────────────────────────────────────
asio::awaitable<void> ThreadWorker::flush_loop()
{
    auto executor = co_await asio::this_coro::executor;
    asio::steady_timer timer(executor);

    while (running_)
    {
        timer.expires_after(std::chrono::milliseconds(cfg_.flush_interval_ms));
        co_await timer.async_wait(asio::use_awaitable);

        // Produce current snapshot to the telemetry ingester
        producer_->flush(counters_, cfg_.submission_id, thread_id_);

        // Reset counters for next window
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
