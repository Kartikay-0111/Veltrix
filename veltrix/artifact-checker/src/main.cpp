#include "artifact_checker.hpp"
#include <iostream>
#include <cstdlib>
#include <csignal>

// ─────────────────────────────────────────────────────────────────────────────
// Global checker pointer for signal handler
// ─────────────────────────────────────────────────────────────────────────────
static ArtifactChecker *g_checker = nullptr;

void signal_handler(int sig)
{
    std::cout << "\n[Main] Signal " << sig << " received — shutting down...\n";
    if (g_checker)
        g_checker->stop();
}

static std::string getenv_or(const char *key, const char *fallback)
{
    const char *val = std::getenv(key);
    return val ? std::string(val) : std::string(fallback);
}

int main()
{
    // ── Config from environment variables ─────────────────────────────────────
    std::string brokers = getenv_or("REDPANDA_BROKERS", "redpanda:9092");
    std::string topic = getenv_or("METRICS_TOPIC", "order_metrics");
    std::string group = getenv_or("CONSUMER_GROUP", "artifact-checker");
    std::string sandbox_host = getenv_or("SANDBOX_HOST", "localhost");
    std::string sandbox_port = getenv_or("SANDBOX_PORT", "8080");

    // Build libpqxx connection string from env vars
    std::string db_conn =
        "host=" + getenv_or("POSTGRES_HOST", "postgres") +
        " port=" + getenv_or("POSTGRES_PORT", "5432") +
        " dbname=" + getenv_or("POSTGRES_DB", "iicpc_db") +
        " user=" + getenv_or("POSTGRES_USER", "iicpc") +
        " password=" + getenv_or("POSTGRES_PASSWORD", "iicpc_secret");

    std::cout << "╔══════════════════════════════════════════╗\n"
              << "║   VELTRIX Artifact Checker  (Part 3)    ║\n"
              << "╚══════════════════════════════════════════╝\n"
              << "  Redpanda  : " << brokers << "\n"
              << "  Topic     : " << topic << "\n"
              << "  Sandbox   : " << sandbox_host << ":" << sandbox_port << "\n\n";

    // ── Signal handling for clean shutdown ────────────────────────────────────
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    try
    {
        ArtifactChecker checker(brokers, topic, group,
                                db_conn, sandbox_host, sandbox_port);
        g_checker = &checker;
        checker.run(); // blocks — spin-polls on CPU core 7
    }
    catch (const std::exception &e)
    {
        std::cerr << "[FATAL] " << e.what() << "\n";
        return 1;
    }

    return 0;
}