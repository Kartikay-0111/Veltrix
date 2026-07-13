#include "grpc_telemetry.hpp"
#include <iostream>
#include <chrono>
#include <cmath>

GrpcTelemetryClient::GrpcTelemetryClient(const std::string &target)
{
    // Build channel arguments with gzip compression enabled
    grpc::ChannelArguments args;
    args.SetCompressionAlgorithm(GRPC_COMPRESS_GZIP);

    // Max message size: 64 MB (matching the Go server)
    args.SetMaxReceiveMessageSize(64 * 1024 * 1024);
    args.SetMaxSendMessageSize(64 * 1024 * 1024);

    channel_ = grpc::CreateCustomChannel(
        target, grpc::InsecureChannelCredentials(), args);

    stub_ = telemetry::TelemetryService::NewStub(channel_);

    std::cout << "[gRPC] Channel created → " << target << "\n";
}

std::unique_ptr<StreamHandle> GrpcTelemetryClient::open_stream()
{
    auto handle = std::make_unique<StreamHandle>();

    // Set gzip compression on the call
    handle->context.set_compression_algorithm(GRPC_COMPRESS_GZIP);

    handle->writer = stub_->StreamTelemetry(&handle->context, &handle->response);

    std::cout << "[gRPC] Stream opened\n";
    return handle;
}

bool GrpcTelemetryClient::send_batch(
    StreamHandle &stream,
    const AuditLog &log,
    const TelemetryCounters &counters,
    const std::string &submission_id,
    int thread_id)
{
    telemetry::AuditBatch batch;

    // ── Pack OrderSubmitted events ───────────────────────────────────────────
    for (const auto &order : log.orders())
    {
        auto *pb_order = batch.add_orders();
        pb_order->set_timestamp_us(order.timestamp_us);
        pb_order->set_submission_id(order.submission_id);
        pb_order->set_bot_id(order.bot_id);
        pb_order->set_order_id(order.order_id);
        pb_order->set_action(order.action);
        pb_order->set_side(order.side);
        pb_order->set_ticker(order.ticker);
        pb_order->set_price(order.price);
        pb_order->set_quantity(order.quantity);
        pb_order->set_cancel_target_id(order.cancel_target_id);
        pb_order->set_seq(order.seq);
        pb_order->set_contestant_order_id(order.contestant_order_id);
        pb_order->set_end_of_run(order.end_of_run);
        pb_order->set_outcome(static_cast<int32_t>(order.outcome));
    }

    // ── Pack TradeExecuted events ────────────────────────────────────────────
    for (const auto &trade : log.trades())
    {
        auto *pb_trade = batch.add_trades();
        pb_trade->set_timestamp_us(trade.timestamp_us);
        pb_trade->set_submission_id(trade.submission_id);
        pb_trade->set_bot_id(trade.bot_id);
        pb_trade->set_contestant_order_id(trade.contestant_order_id);
        pb_trade->set_matched_order_id(trade.matched_order_id);
        pb_trade->set_execution_price(trade.execution_price);
        pb_trade->set_execution_quantity(trade.execution_quantity);
        pb_trade->set_ticker(trade.ticker);
        pb_trade->set_aggressor_order_id(trade.aggressor_order_id);
        pb_trade->set_seq(trade.seq);
    }

    // ── Pack MetricsBatch ────────────────────────────────────────────────────
    auto *pb_metrics = batch.mutable_metrics();
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::system_clock::now().time_since_epoch())
                      .count();

    pb_metrics->set_timestamp_ms(now_ms);
    pb_metrics->set_submission_id(submission_id);
    pb_metrics->set_thread_id(thread_id);
    pb_metrics->set_http_200(counters.counts[HTTP_200]);
    pb_metrics->set_http_4xx(counters.counts[HTTP_4XX]);
    pb_metrics->set_http_5xx(counters.counts[HTTP_5XX]);
    pb_metrics->set_timeout(counters.counts[TIMEOUT]);
    pb_metrics->set_econnref(counters.counts[ECONNREF]);
    pb_metrics->set_other_err(counters.counts[OTHER_ERR]);

    double avg_latency = (counters.latency_samples > 0)
                             ? counters.total_latency_ms / static_cast<double>(counters.latency_samples)
                             : 0.0;
    if (std::isnan(avg_latency) || std::isinf(avg_latency))
        avg_latency = 0.0;

    pb_metrics->set_avg_latency_ms(avg_latency);
    pb_metrics->set_samples(counters.latency_samples);

    for (const auto &bucket : counters.histogram.buckets)
    {
        pb_metrics->add_histogram(bucket);
    }

    // ── Write to the stream ──────────────────────────────────────────────────
    if (!stream.writer->Write(batch))
    {
        std::cerr << "[gRPC] Write failed (stream broken)\n";
        return false;
    }

    return true;
}

telemetry::StreamTelemetryResponse GrpcTelemetryClient::finish(StreamHandle &stream)
{
    stream.writer->WritesDone();
    grpc::Status status = stream.writer->Finish();

    if (!status.ok())
    {
        std::cerr << "[gRPC] Stream finish failed: " << status.error_message() << "\n";
    }

    return stream.response;
}
