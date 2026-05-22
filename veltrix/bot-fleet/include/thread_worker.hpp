#pragma once

// ── Enable io_uring backend for Boost.Asio ───────────────────────────────────
// Must be defined BEFORE any Boost.Asio include.
// This tells Boost.Asio to use io_uring instead of epoll as the I/O backend.
// On kernel 5.10+ this gives you async I/O with zero syscalls on the hot path.
#define BOOST_ASIO_HAS_IO_URING 1
#define BOOST_ASIO_DISABLE_EPOLL 1

#include <boost/asio.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/steady_timer.hpp>
#include <thread>
#include <memory>
#include <string>
#include <vector>
#include "telemetry.hpp"
#include "bot_payload.hpp"

namespace asio = boost::asio;
using tcp = asio::ip::tcp;

// ─────────────────────────────────────────────────────────────────────────────
// BenchmarkConfig — passed to every thread worker at launch
// ─────────────────────────────────────────────────────────────────────────────
struct BenchmarkConfig
{
    std::string submission_id;
    std::string target_host;
    std::string target_port;
    int num_bots; // total bots across ALL threads
    int duration_secs;
    std::string protocol; // "rest" | "websocket"
    std::string telemetry_ingester_host;
    std::string telemetry_ingester_port;
    int flush_interval_ms = 500;
};

// ─────────────────────────────────────────────────────────────────────────────
// ThreadWorker — one instance per CPU core
//
// Owns:
//   - Its own io_context (mapped to a private io_uring instance in kernel)
//   - Its own TelemetryCounters (lock-free, no sharing)
//   - A slice of the total bot count
//
// Threading model: shared-nothing. Zero mutexes. Zero locks.
// ─────────────────────────────────────────────────────────────────────────────
class ThreadWorker
{
public:
    ThreadWorker(int thread_id,
                 int bots_this_thread,
                 const BenchmarkConfig &cfg,
                 std::shared_ptr<TelemetryProducer> producer);

    // Launch the worker on its own OS thread. Returns immediately.
    void start();

    // Signal the worker to stop after its current round completes
    void stop();

    // Block until the worker thread exits
    void join();

    int thread_id() const { return thread_id_; }

private:
    int thread_id_;
    int bots_this_thread_;
    BenchmarkConfig cfg_;
    std::shared_ptr<TelemetryProducer> producer_;

    // Each worker owns its io_context — backed by its own io_uring instance
    asio::io_context ioc_;
    std::atomic<bool> running_{false};
    std::jthread thread_; // C++20 — auto-joins on destruction

    TelemetryCounters counters_; // private, never shared

    // ── Core coroutines (these run inside the event loop) ─────────────────────

    // Run one bot: connect → send order → read response → record latency → repeat
    asio::awaitable<void> run_bot(uint64_t bot_id);

    // Fire orders continuously until benchmark duration expires
    asio::awaitable<void> send_orders(tcp::socket &socket, uint64_t bot_id);

    // Async timer: every flush_interval_ms, package counters and send to the ingester
    asio::awaitable<void> flush_loop();

    // Parse HTTP response status code from raw bytes
    int parse_status_code(const std::string &response);

    // Record event + latency into lock-free per-thread counters
    void record(int status_code, double latency_ms, bool timed_out);
};
