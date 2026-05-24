#include "artifact_checker.hpp"
#include <iostream>
#include <stdexcept>
#include <regex>
#include <sstream>
#include <chrono>
#include <pthread.h>

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

// ─────────────────────────────────────────────────────────────────────────────
// Constructor — wire up Redpanda consumer with correct config
// ─────────────────────────────────────────────────────────────────────────────
ArtifactChecker::ArtifactChecker(const std::string &redpanda_brokers,
                                 const std::string &topic,
                                 const std::string &consumer_group,
                                 const std::string &db_conn_str,
                                 const std::string &sandbox_host,
                                 const std::string &sandbox_port)
    : topic_(topic)
    , db_writer_(db_conn_str)
    , default_sandbox_host_(sandbox_host)
    , default_sandbox_port_(sandbox_port)
    , last_db_write_(Clock::now())
    , last_correctness_check_(Clock::now())
{
    // ── Build rdkafka consumer config ─────────────────────────────────────────
    // The fix for the bot-fleet linker issue:
    //   We use RdKafka::Conf (C++ wrapper) via pkg-config rdkafka++
    //   Both rdkafka and rdkafka++ are explicitly linked in CMakeLists.txt
    std::string errstr;

    auto *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    auto set = [&](const std::string &key, const std::string &val)
    {
        if (conf->set(key, val, errstr) != RdKafka::Conf::CONF_OK)
        {
            throw std::runtime_error("Kafka config [" + key + "]: " + errstr);
        }
    };

    set("bootstrap.servers", redpanda_brokers);
    set("group.id", consumer_group);
    set("auto.offset.reset", "latest"); // only new messages, not history
    set("enable.auto.commit", "true");  // commit offsets automatically
    set("fetch.wait.max.ms", "0");      // non-blocking fetch (spin-poll)
    set("fetch.min.bytes", "1");        // return immediately if any data

    consumer_.reset(RdKafka::KafkaConsumer::create(conf, errstr));
    delete conf;

    if (!consumer_)
    {
        throw std::runtime_error("Failed to create Kafka consumer: " + errstr);
    }

    // Subscribe to topic
    RdKafka::ErrorCode err = consumer_->subscribe({topic_});
    if (err != RdKafka::ERR_NO_ERROR)
    {
        throw std::runtime_error("Subscribe failed: " +
                                 RdKafka::err2str(err));
    }

    std::cout << "[ArtifactChecker] Subscribed to topic: " << topic_ << "\n"
              << "[ArtifactChecker] Brokers: " << redpanda_brokers << "\n";
}

// ─────────────────────────────────────────────────────────────────────────────
// run() — The main spin-poll loop
//
// This function DELIBERATELY pegs the CPU to 100%.
// consume(0) is non-blocking — if no message is ready, it returns immediately.
// This is the "spin-poll" strategy from the blueprint:
//   We sacrifice one CPU core to guarantee microsecond reaction time
//   when a new telemetry message arrives from the bot fleet.
// ─────────────────────────────────────────────────────────────────────────────
void ArtifactChecker::run()
{
    // ── Pin to CPU core 7 (dedicated, isolated from bot fleet) ───────────────
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(7, &cpuset); // core 7 — furthest from bot fleet cores (0-6)
    pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);

    std::cout << "[ArtifactChecker] Running on CPU core 7. Spin-polling...\n";

    while (running_)
    {
        // ── Non-blocking consume (0ms timeout = return immediately) ───────────
        std::unique_ptr<RdKafka::Message> msg(consumer_->consume(0));

        if (msg)
        {
            switch (msg->err())
            {
            case RdKafka::ERR_NO_ERROR:
                process_message(msg.get());
                break;

            case RdKafka::ERR__TIMED_OUT:
            case RdKafka::ERR__PARTITION_EOF:
                // No message available — continue spinning
                break;

            default:
                std::cerr << "[ArtifactChecker] Consume error: "
                          << msg->errstr() << "\n";
            }
        }

        // ── Periodic: write to TimescaleDB every 1 second ─────────────────────
        auto now = Clock::now();
        if (now - last_db_write_ >= 1s)
        {
            flush_to_db();
            last_db_write_ = now;
        }

        // ── Periodic: correctness check every 10 seconds ─────────────────────
        if (now - last_correctness_check_ >= 10s)
        {
            std::cout << "[ArtifactChecker] Running correctness checks...\n";
            for (auto &entry : states_)
            {
                run_correctness_for_submission(entry.second);
            }
            last_correctness_check_ = now;
        }
    }

    consumer_->close();
    std::cout << "[ArtifactChecker] Shutdown complete.\n";
}

// ─────────────────────────────────────────────────────────────────────────────
// process_message — Parse and accumulate one Redpanda message
// ─────────────────────────────────────────────────────────────────────────────
void ArtifactChecker::process_message(RdKafka::Message *msg)
{
    std::string json(static_cast<const char *>(msg->payload()), msg->len());
    parse_and_accumulate(json);
}

void ArtifactChecker::parse_and_accumulate(const std::string &json)
{
    std::string submission_id = extract_string(json, "submission_id");
    if (submission_id.empty())
        return;

    // Get or create state for this submission
    auto &state = states_[submission_id];
    state.submission_id = submission_id;

    // Accumulate counters
    state.successful_orders += extract_int64(json, "http_200");
    state.total_orders += extract_int64(json, "samples");

    // Merge histogram (the φ(a ∪ b) = φ(a) + φ(b) operation)
    auto hist_buckets = extract_hist(json);
    int64_t samples = extract_int64(json, "samples");
    state.histogram.load(hist_buckets, samples);
}

// ─────────────────────────────────────────────────────────────────────────────
// flush_to_db — Write one row per submission and reset state
// ─────────────────────────────────────────────────────────────────────────────
void ArtifactChecker::flush_to_db()
{
    for (auto &[sub_id, state] : states_)
    {
        if (state.total_orders == 0)
            continue; // no data this second

        MetricRow row;
        row.team_id = sub_id;
        row.tps = static_cast<int>(state.successful_orders);
        row.p50_latency_ms = state.histogram.p50();
        row.p90_latency_ms = state.histogram.p90();
        row.p99_latency_ms = state.histogram.p99();
        row.is_correct = state.last_correct;

        std::cout << "[ArtifactChecker] " << sub_id
                  << " TPS=" << row.tps
                  << " p50=" << row.p50_latency_ms << "ms"
                  << " p99=" << row.p99_latency_ms << "ms"
                  << " correct=" << (row.is_correct ? "YES" : "NO") << "\n";

        db_writer_.write(row);

        // Reset for next second
        state.histogram.reset();
        state.successful_orders = 0;
        state.total_orders = 0;
    }
}

std::optional<std::pair<std::string, std::string>> ArtifactChecker::parse_endpoint_url(
    const std::string &url) const
{
    std::string work = url;
    const auto scheme_pos = work.find("://");
    if (scheme_pos != std::string::npos)
    {
        work = work.substr(scheme_pos + 3);
    }

    const auto path_pos = work.find('/');
    if (path_pos != std::string::npos)
    {
        work = work.substr(0, path_pos);
    }

    const auto colon_pos = work.rfind(':');
    if (colon_pos == std::string::npos)
        return std::nullopt;

    const std::string host = work.substr(0, colon_pos);
    const std::string port = work.substr(colon_pos + 1);
    if (host.empty() || port.empty())
        return std::nullopt;

    return std::make_optional(std::make_pair(host, port));
}

std::optional<std::pair<std::string, std::string>> ArtifactChecker::resolve_endpoint(
    SubmissionState &state)
{
    auto endpoint_url = db_writer_.get_endpoint_url(state.submission_id);
    if (!endpoint_url.has_value())
    {
        if (!state.endpoint_missing_logged)
        {
            std::cerr << "[ArtifactChecker] No endpoint_url for submission "
                      << state.submission_id << "\n";
            state.endpoint_missing_logged = true;
        }
        if (!state.sandbox_host.empty() && !state.sandbox_port.empty())
        {
            return std::make_optional(std::make_pair(state.sandbox_host, state.sandbox_port));
        }
        return std::nullopt;
    }

    auto parsed = parse_endpoint_url(*endpoint_url);
    if (!parsed.has_value())
    {
        std::cerr << "[ArtifactChecker] Invalid endpoint_url for submission "
                  << state.submission_id << ": " << *endpoint_url << "\n";
        return std::nullopt;
    }

    auto host = parsed->first;
    auto port = parsed->second;
    if (host == "host.docker.internal" || host == "localhost" || host == "127.0.0.1" || host == "::1")
    {
        host = "sandbox-" + state.submission_id;
        port = default_sandbox_port_;
    }

    state.sandbox_host = host;
    state.sandbox_port = port;
    state.endpoint_missing_logged = false;
    return std::make_optional(std::make_pair(host, port));
}

void ArtifactChecker::run_correctness_for_submission(SubmissionState &state)
{
    auto endpoint = resolve_endpoint(state);
    if (!endpoint.has_value())
        return;

    CorrectnessEngine checker(endpoint->first, endpoint->second);
    state.last_correct = checker.run_check("AAPL");
}

// ─────────────────────────────────────────────────────────────────────────────
// JSON field extractors (regex-based, no external dependency)
// ─────────────────────────────────────────────────────────────────────────────
double ArtifactChecker::extract_double(const std::string &json,
                                       const std::string &key)
{
    std::regex re("\"" + key + "\"\\s*:\\s*([0-9.eE+\\-]+)");
    std::smatch m;
    if (std::regex_search(json, m, re))
        return std::stod(m[1].str());
    return 0.0;
}

int64_t ArtifactChecker::extract_int64(const std::string &json,
                                       const std::string &key)
{
    std::regex re("\"" + key + "\"\\s*:\\s*(-?\\d+)");
    std::smatch m;
    if (std::regex_search(json, m, re))
        return std::stoll(m[1].str());
    return 0;
}

std::string ArtifactChecker::extract_string(const std::string &json,
                                            const std::string &key)
{
    std::regex re("\"" + key + "\"\\s*:\\s*\"([^\"]+)\"");
    std::smatch m;
    if (std::regex_search(json, m, re))
        return m[1].str();
    return "";
}

std::array<int64_t, 5> ArtifactChecker::extract_hist(const std::string &json)
{
    // Match "hist":[a,b,c,d,e]
    std::regex re("\"hist\"\\s*:\\s*\\[(\\d+),(\\d+),(\\d+),(\\d+),(\\d+)\\]");
    std::smatch m;
    std::array<int64_t, 5> result = {};
    if (std::regex_search(json, m, re))
    {
        for (int i = 0; i < 5; ++i)
            result[i] = std::stoll(m[i + 1].str());
    }
    return result;
}
