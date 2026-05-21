#include "fleet_commander.hpp"
#include <iostream>
#include <cstdlib>

// ─────────────────────────────────────────────────────────────────────────────
// main — Bot Fleet entry point
//
// Reads config from environment variables so it works cleanly
// in Docker/Kubernetes without recompiling.
//
// Environment variables:
//   FLEET_LISTEN_PORT      — port the FleetCommander HTTP server binds to (default: 7070)
//   REDPANDA_BROKERS       — e.g. "redpanda:9092"
// ─────────────────────────────────────────────────────────────────────────────

static std::string getenv_or(const char* key, const char* fallback) {
    const char* val = std::getenv(key);
    return val ? std::string(val) : std::string(fallback);
}

int main() {
    uint16_t    port     = static_cast<uint16_t>(
                               std::stoi(getenv_or("FLEET_LISTEN_PORT", "7070"))
                           );
    std::string brokers  = getenv_or("REDPANDA_BROKERS", "redpanda:9092");

    std::cout << "╔══════════════════════════════════════╗\n"
              << "║    VELTRIX Bot Fleet Commander       ║\n"
              << "╚══════════════════════════════════════╝\n"
              << "  Listen port     : " << port    << "\n"
              << "  Redpanda        : " << brokers << "\n"
              << "  Hardware cores  : " << std::thread::hardware_concurrency() << "\n\n";

    FleetCommander commander(port, brokers);
    commander.run();   // blocks forever

    return 0;
}
