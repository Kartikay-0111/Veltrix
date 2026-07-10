#pragma once

#define BOOST_ASIO_HAS_IO_URING 1
#define BOOST_ASIO_DISABLE_EPOLL 1

#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <atomic>
#include <mutex>
#include <string>
#include <memory>
#include <vector>
#include "thread_worker.hpp"

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = asio::ip::tcp;

class GrpcTelemetryClient;

// ─────────────────────────────────────────────────────────────────────────────
// FleetCommander — HTTP server that receives the start signal
//
// Listens on port 7070 for a POST /benchmark from the Python sandbox manager.
// When triggered:
//   1. Reads target IP:port, num_bots, duration from JSON body
//   2. Detects how many CPU cores are available
//   3. Divides bots evenly across cores
//   4. Spawns one ThreadWorker per core
//   5. Waits for duration to expire
//   6. Stops all workers cleanly
//
// Owns a long-lived GrpcTelemetryClient (channel opened once at startup).
// ─────────────────────────────────────────────────────────────────────────────
class FleetCommander
{
public:
    explicit FleetCommander(uint16_t listen_port,
                            const std::string &grpc_target);

    // Blocking — runs the HTTP server forever
    void run();

private:
    uint16_t listen_port_;
    std::string grpc_target_;
    asio::io_context ioc_;

    // Long-lived gRPC channel to the Go telemetry ingester.
    // Created once at startup. Thread-safe. Shared across all benchmarks.
    std::shared_ptr<GrpcTelemetryClient> grpc_client_;

    // ── Core partition manager ────────────────────────────────────────────────
    // core_in_use_[i] == true means core i is claimed by a running benchmark.
    // Guarded by core_mutex_. Written only inside launch_benchmark().
    std::vector<bool> core_in_use_;
    std::mutex        core_mutex_;
    int               total_cores_    = 0;
    int               min_cores_per_benchmark_ = 2; // each benchmark gets >= this many cores

    // Reported via GET /health so the Go fleet pool can track load.
    std::atomic<int>  active_benchmarks_{0};
    int               max_concurrent_  = 0; // computed at startup

    // Handle one incoming HTTP connection
    asio::awaitable<void> handle_connection(tcp::socket socket);

    // Parse the JSON body from /benchmark POST
    BenchmarkConfig parse_benchmark_request(const std::string &body);

    // Claim a non-overlapping slice of free cores for this benchmark.
    // Returns the core IDs assigned, or empty if all cores are busy.
    std::vector<int> claim_cores();

    // Release cores back to the free pool after a benchmark finishes.
    void release_cores(const std::vector<int> &cores);

    // Core orchestration: spawn workers on claimed cores, wait, stop
    void launch_benchmark(const BenchmarkConfig &cfg);

    // Parse a single JSON string value: {"key":"value"} → value
    std::string extract_json_string(const std::string &json,
                                    const std::string &key);
    int extract_json_int(const std::string &json,
                         const std::string &key);
};
