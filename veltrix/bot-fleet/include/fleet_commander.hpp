#pragma once

#define BOOST_ASIO_HAS_IO_URING  1
#define BOOST_ASIO_DISABLE_EPOLL 1

#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <string>
#include <memory>
#include <vector>
#include "thread_worker.hpp"
#include "telemetry.hpp"

namespace asio  = boost::asio;
namespace beast = boost::beast;
namespace http  = beast::http;
using tcp       = asio::ip::tcp;

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
// ─────────────────────────────────────────────────────────────────────────────
class FleetCommander {
public:
    explicit FleetCommander(uint16_t    listen_port,
                            std::string redpanda_brokers);

    // Blocking — runs the HTTP server forever
    void run();

private:
    uint16_t    listen_port_;
    std::string redpanda_brokers_;
    asio::io_context ioc_;

    // Handle one incoming HTTP connection
    asio::awaitable<void> handle_connection(tcp::socket socket);

    // Parse the JSON body from /benchmark POST
    BenchmarkConfig parse_benchmark_request(const std::string& body);

    // Core orchestration: spawn workers, wait, stop
    void launch_benchmark(const BenchmarkConfig& cfg);

    // Parse a single JSON string value: {"key":"value"} → value
    std::string extract_json_string(const std::string& json,
                                    const std::string& key);
    int         extract_json_int(const std::string& json,
                                 const std::string& key);
};
