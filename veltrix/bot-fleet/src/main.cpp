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
//   TELEMETRY_GRPC_TARGET  — gRPC target for telemetry ingester (default: "telemetry-ingester:8091")
// ─────────────────────────────────────────────────────────────────────────────

static std::string getenv_or(const char *key, const char *fallback)
{
    const char *val = std::getenv(key);
    return val ? std::string(val) : std::string(fallback);
}

int main()
{
    std::cout.setf(std::ios::unitbuf);
    std::cerr.setf(std::ios::unitbuf);

    uint16_t port = static_cast<uint16_t>(
        std::stoi(getenv_or("FLEET_LISTEN_PORT", "7070")));

    std::string grpc_target = getenv_or("TELEMETRY_GRPC_TARGET", "telemetry-ingester:8091");

    std::cout << "╔══════════════════════════════════════╗\n"
              << "║    VELTRIX Bot Fleet Commander       ║\n"
              << "╚══════════════════════════════════════╝\n"
              << "  Listen port     : " << port << "\n"
              << "  gRPC target     : " << grpc_target << "\n"
              << "  Hardware cores  : " << std::thread::hardware_concurrency() << "\n\n";

    FleetCommander commander(port, grpc_target);
    commander.run(); // blocks forever

    return 0;
}
