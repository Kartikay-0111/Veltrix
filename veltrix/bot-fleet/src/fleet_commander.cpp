#include "fleet_commander.hpp"
#include "grpc_telemetry.hpp"
#include <iostream>
#include <thread>
#include <stdexcept>
#include <regex>
#include <chrono>

using namespace std::chrono_literals;

FleetCommander::FleetCommander(uint16_t listen_port,
                               const std::string &grpc_target)
    : listen_port_(listen_port), grpc_target_(grpc_target), ioc_(1)
{
    // Detect hardware and initialise core partition manager.
    total_cores_ = static_cast<int>(std::thread::hardware_concurrency());
    if (total_cores_ == 0)
        total_cores_ = 4;

    core_in_use_.assign(total_cores_, false);

    // Each benchmark gets at least min_cores_per_benchmark_ dedicated cores.
    max_concurrent_ = std::max(1, total_cores_ / min_cores_per_benchmark_);

    std::cout << "[FleetCommander] Core partition: "
              << total_cores_ << " cores, "
              << min_cores_per_benchmark_ << " min/benchmark, "
              << max_concurrent_ << " max concurrent benchmarks\n";

    // Create the long-lived gRPC channel at startup.
    grpc_client_ = std::make_shared<GrpcTelemetryClient>(grpc_target_);
}

void FleetCommander::run()
{
    std::cout << "[FleetCommander] Listening on port " << listen_port_ << "\n";

    asio::co_spawn(ioc_, [this]() -> asio::awaitable<void>
                   {
        try {
            auto executor = co_await asio::this_coro::executor;
            
            // Explicitly define the endpoint (tcp::v4() maps to 0.0.0.0 inside Docker)
            tcp::endpoint endpoint(tcp::v4(), listen_port_);
            tcp::acceptor acceptor(executor);
            
            // Open the acceptor and set the vital REUSE ADDRESS flag
            acceptor.open(endpoint.protocol());
            acceptor.set_option(asio::socket_base::reuse_address(true));
            acceptor.bind(endpoint);
            acceptor.listen();

            while (true) {
                tcp::socket socket = co_await acceptor.async_accept(asio::use_awaitable);
                asio::co_spawn(executor,
                    handle_connection(std::move(socket)), asio::detached);
            }
        } 
        catch (const std::exception& e) {
            // THE TRAP: If the network fails, it will print to terminal
            std::cerr << "[FATAL ERROR] Acceptor coroutine crashed: " << e.what() << "\n";
        } }, asio::detached);

    // This will now block infinitely unless the coroutine crashes
    ioc_.run();
}

asio::awaitable<void> FleetCommander::handle_connection(tcp::socket socket)
{
    try
    {
        beast::flat_buffer buffer;
        http::request<http::string_body> req;

        co_await http::async_read(socket, buffer, req, asio::use_awaitable);

        if (req.method() == http::verb::get &&
            req.target() == "/health")
        {
            // Health check with load info for the Go fleet pool.
            int active = active_benchmarks_.load();
            http::response<http::string_body> res{http::status::ok, req.version()};
            res.set(http::field::content_type, "application/json");
            res.body() = "{\"status\":\"ok\","
                         "\"active\":" + std::to_string(active) + ","
                         "\"max\":"    + std::to_string(max_concurrent_) + "}";
            res.prepare_payload();
            co_await http::async_write(socket, res, asio::use_awaitable);
            co_return;
        }

        if (req.method() == http::verb::post &&
            req.target() == "/benchmark")
        {

            BenchmarkConfig cfg = parse_benchmark_request(std::string(req.body()));

            // Respond immediately — benchmark runs async
            http::response<http::string_body> res{http::status::accepted, req.version()};
            res.set(http::field::content_type, "application/json");
            res.body() = "{\"status\":\"benchmark_started\","
                         "\"submission_id\":\"" +
                         cfg.submission_id + "\"}";
            res.prepare_payload();
            co_await http::async_write(socket, res, asio::use_awaitable);

            // Launch benchmark in a detached thread so HTTP server stays responsive
            std::thread([this, cfg]()
                        { launch_benchmark(cfg); })
                .detach();

            co_return;
        }

        // 404 for anything else
        http::response<http::string_body> res{http::status::not_found, req.version()};
        res.body() = "{\"error\":\"not found\"}";
        res.prepare_payload();
        co_await http::async_write(socket, res, asio::use_awaitable);
    }
    catch (const std::exception &e)
    {
        std::cerr << "[FleetCommander] Connection error: " << e.what() << "\n";
    }
}

void FleetCommander::launch_benchmark(const BenchmarkConfig &cfg)
{
    // ── Claim an exclusive slice of free cores ───────────────────────────────
    // Blocks until cores are available so this machine never oversubscribes.
    std::vector<int> my_cores;
    while (my_cores.empty()) {
        my_cores = claim_cores();
        if (my_cores.empty()) {
            std::cout << "[FleetCommander] All cores busy, waiting 1s for " << cfg.submission_id << "\n";
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

    ++active_benchmarks_;

    // Distribute bots evenly across the claimed cores.
    int num_workers   = static_cast<int>(my_cores.size());
    int bots_per_core = cfg.num_bots / num_workers;
    int leftover_bots = cfg.num_bots % num_workers;

    std::cout << "[FleetCommander] Launching benchmark:\n"
              << "  submission_id : " << cfg.submission_id << "\n"
              << "  mode          : " << cfg.mode << "\n"
              << "  target        : " << cfg.target_host << ":" << cfg.target_port << "\n"
              << "  total bots    : " << cfg.num_bots << "\n"
              << "  assigned cores: [";
    for (int i = 0; i < num_workers; ++i)
        std::cout << my_cores[i] << (i + 1 < num_workers ? "," : "");
    std::cout << "]\n"
              << "  duration      : " << cfg.duration_secs << "s\n";

    std::vector<std::unique_ptr<ThreadWorker>> workers;
    workers.reserve(num_workers);

    for (int i = 0; i < num_workers; ++i) {
        // Pass the actual core ID — ThreadWorker pins itself to this core.
        int bots = bots_per_core + (i == 0 ? leftover_bots : 0);
        workers.push_back(std::make_unique<ThreadWorker>(
            my_cores[i], // thread_id_ == physical core to pin to
            bots, cfg, grpc_client_));
    }

    // ── Start all workers ─────────────────────────────────────────────────────
    for (auto &w : workers)
        w->start();

    std::cout << "[FleetCommander] All workers running. Duration: " << cfg.duration_secs << "s\n";

    // ── Wait for benchmark duration ───────────────────────────────────────────
    std::this_thread::sleep_for(std::chrono::seconds(cfg.duration_secs));

    // ── Stop all workers cleanly ─────────────────────────────────────────────
    for (auto &w : workers)
        w->stop();
    for (auto &w : workers)
        w->join();

    // ── Release cores and decrement active counter ───────────────────────────
    release_cores(my_cores);
    --active_benchmarks_;

    std::cout << "[FleetCommander] Benchmark complete for " << cfg.submission_id << "\n";
}

// ─────────────────────────────────────────────────────────────────────────────
// claim_cores — atomically grab min_cores_per_benchmark_ free cores.
// Returns the core IDs that were claimed. Returns empty if not enough free.
// ─────────────────────────────────────────────────────────────────────────────
std::vector<int> FleetCommander::claim_cores()
{
    std::lock_guard<std::mutex> lock(core_mutex_);

    std::vector<int> claimed;
    claimed.reserve(min_cores_per_benchmark_);

    for (int c = 0; c < total_cores_ && (int)claimed.size() < min_cores_per_benchmark_; ++c) {
        if (!core_in_use_[c])
            claimed.push_back(c);
    }

    // Only commit if we found enough cores — all-or-nothing.
    if ((int)claimed.size() < min_cores_per_benchmark_)
        return {}; // not enough free cores, caller will retry

    for (int c : claimed)
        core_in_use_[c] = true;

    return claimed;
}

// release_cores — mark the given cores as free. Called after workers join.
void FleetCommander::release_cores(const std::vector<int> &cores)
{
    std::lock_guard<std::mutex> lock(core_mutex_);
    for (int c : cores)
        core_in_use_[c] = false;
}

BenchmarkConfig FleetCommander::parse_benchmark_request(const std::string &body)
{
    // Minimal JSON parser — avoids a heavy dependency like nlohmann/json
    // for a handful of fields. In production, use nlohmann or rapidjson.
    BenchmarkConfig cfg;
    cfg.submission_id = extract_json_string(body, "submission_id");
    cfg.target_host = extract_json_string(body, "target_host");
    cfg.target_port = extract_json_string(body, "target_port");
    cfg.protocol = extract_json_string(body, "protocol");
    cfg.num_bots = extract_json_int(body, "num_bots");
    cfg.duration_secs = extract_json_int(body, "duration_secs");
    cfg.mode = extract_json_string(body, "mode");
    cfg.seed = static_cast<uint64_t>(extract_json_int(body, "seed"));

    if (cfg.mode.empty())
        cfg.mode = "performance";

    if (cfg.mode == "correctness")
    {
        // Serialized single writer: exactly one bot so send-order == the
        // contestant's process-order == the seq the golden model replays.
        cfg.num_bots = 1;
        if (cfg.seed == 0)
            cfg.seed = 42; // deterministic default so every contestant gets the same stream
    }
    else if (cfg.num_bots <= 0)
    {
        cfg.num_bots = 1000;
    }

    if (cfg.duration_secs <= 0)
        cfg.duration_secs = 60;
    if (cfg.protocol.empty())
        cfg.protocol = "rest";

    return cfg;
}

// ── Minimal JSON field extractors ────────────────────────────────────────────
std::string FleetCommander::extract_json_string(const std::string &json,
                                                const std::string &key)
{
    std::regex re("\"" + key + "\"\\s*:\\s*\"([^\"]+)\"");
    std::smatch m;
    if (std::regex_search(json, m, re))
        return m[1].str();
    return "";
}

int FleetCommander::extract_json_int(const std::string &json,
                                     const std::string &key)
{
    std::regex re("\"" + key + "\"\\s*:\\s*(\\d+)");
    std::smatch m;
    if (std::regex_search(json, m, re))
        return std::stoi(m[1].str());
    return 0;
}
