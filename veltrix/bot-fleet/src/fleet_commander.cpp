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
    // Create the long-lived gRPC channel at startup.
    // This is thread-safe and internally multiplexed over HTTP/2.
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
            // Health check endpoint for Docker/K8s probes
            http::response<http::string_body> res{http::status::ok, req.version()};
            res.set(http::field::content_type, "application/json");
            res.body() = "{\"status\":\"ok\"}";
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
    int num_cores = static_cast<int>(std::thread::hardware_concurrency());
    if (num_cores == 0)
        num_cores = 4; // safe fallback

    std::cout << "[FleetCommander] Launching benchmark:\n"
              << "  submission_id : " << cfg.submission_id << "\n"
              << "  mode          : " << cfg.mode << "\n"
              << "  target        : " << cfg.target_host << ":" << cfg.target_port << "\n"
              << "  total bots    : " << cfg.num_bots << "\n"
              << "  cores         : " << num_cores << "\n"
              << "  duration      : " << cfg.duration_secs << "s\n"
              << "  gRPC target   : " << grpc_target_ << "\n";

    // ── Divide bots evenly across cores ──────────────────────────────────────
    int bots_per_core = cfg.num_bots / num_cores;
    int leftover_bots = cfg.num_bots % num_cores;

    std::vector<std::unique_ptr<ThreadWorker>> workers;
    workers.reserve(num_cores);

    for (int i = 0; i < num_cores; ++i)
    {
        // Give leftover bots to the first worker
        int bots = bots_per_core + (i == 0 ? leftover_bots : 0);
        workers.push_back(std::make_unique<ThreadWorker>(i, bots, cfg, grpc_client_));
    }

    // ── Start all workers ─────────────────────────────────────────────────────
    for (auto &w : workers)
        w->start();

    std::cout << "[FleetCommander] All workers running. Duration: "<< cfg.duration_secs << "s\n";

    // ── Wait for benchmark duration ───────────────────────────────────────────
    std::this_thread::sleep_for(std::chrono::seconds(cfg.duration_secs));

    // ── Stop all workers cleanly ──────────────────────────────────────────────
    for (auto &w : workers)
        w->stop();
    for (auto &w : workers)
        w->join();

    std::cout << "[FleetCommander] Benchmark complete for "<< cfg.submission_id << "\n";
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
