#pragma once

#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include "telemetry.pb.h"
#include "telemetry.grpc.pb.h"
#include "audit_log.hpp"
#include "telemetry.hpp"

// ─────────────────────────────────────────────────────────────────────────────
// StreamHandle — Represents one open client-side gRPC stream.
//
// Created per-benchmark per-thread. The 500ms flush loop calls Send() on it
// until the benchmark ends, then calls Finish().
// ─────────────────────────────────────────────────────────────────────────────
struct StreamHandle
{
    grpc::ClientContext context;
    telemetry::StreamTelemetryResponse response;
    std::unique_ptr<grpc::ClientWriter<telemetry::AuditBatch>> writer;
};

// ─────────────────────────────────────────────────────────────────────────────
// GrpcTelemetryClient — Long-lived gRPC channel to the Go telemetry ingester.
//
// The channel is created once at FleetCommander startup and shared across all
// benchmarks. Each ThreadWorker opens a new stream (StreamHandle) per benchmark.
//
// Threading model: the grpc::Channel is thread-safe. Each StreamHandle is
// owned by exactly one thread. No locks needed.
// ─────────────────────────────────────────────────────────────────────────────
class GrpcTelemetryClient
{
public:
    explicit GrpcTelemetryClient(const std::string &target);
    ~GrpcTelemetryClient() = default;

    // Open a new client-side stream. Called once per benchmark per thread.
    std::unique_ptr<StreamHandle> open_stream();

    // Send one AuditBatch (orders + trades + metrics). Called every 500ms.
    // Returns true if the write succeeded.
    bool send_batch(StreamHandle &stream,
                    const AuditLog &log,
                    const TelemetryCounters &counters,
                    const std::string &submission_id,
                    int thread_id);

    // Close the stream and get the server's ack.
    telemetry::StreamTelemetryResponse finish(StreamHandle &stream);

    // Non-copyable
    GrpcTelemetryClient(const GrpcTelemetryClient &) = delete;
    GrpcTelemetryClient &operator=(const GrpcTelemetryClient &) = delete;

private:
    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<telemetry::TelemetryService::Stub> stub_;
};
