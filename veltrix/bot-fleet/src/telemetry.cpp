#include "telemetry.hpp"
#include <sstream>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <chrono>

TelemetryProducer::TelemetryProducer(const std::string& brokers,
                                      const std::string& topic)
    : topic_(topic)
{
    char errstr[512];

    // ── Create Kafka config ───────────────────────────────────────────────────
    rd_kafka_conf_t* conf = rd_kafka_conf_new();

    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers.c_str(),
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        throw std::runtime_error(std::string("Kafka config error: ") + errstr);
    }

    // Tune for low-latency flushing (we control batching via our 500ms timer)
    rd_kafka_conf_set(conf, "linger.ms",       "0",    errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "batch.num.messages", "1", errstr, sizeof(errstr));

    // ── Create producer handle ────────────────────────────────────────────────
    rk_ = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk_) {
        throw std::runtime_error(std::string("Failed to create Kafka producer: ") + errstr);
    }

    // ── Create topic handle ───────────────────────────────────────────────────
    rkt_ = rd_kafka_topic_new(rk_, topic.c_str(), nullptr);
    if (!rkt_) {
        rd_kafka_destroy(rk_);
        throw std::runtime_error("Failed to create Kafka topic handle: " + topic);
    }

    std::cout << "[Telemetry] Connected to Redpanda at " << brokers
              << " topic=" << topic << "\n";
}

TelemetryProducer::~TelemetryProducer() {
    // Flush any in-flight messages before destroying
    rd_kafka_flush(rk_, 3000);
    rd_kafka_topic_destroy(rkt_);
    rd_kafka_destroy(rk_);
}

void TelemetryProducer::flush(const TelemetryCounters& c,
                               const std::string&       submission_id,
                               int                      thread_id)
{
    if (c.latency_samples == 0 && c.counts[HTTP_200] == 0) return;

    double avg_latency = (c.latency_samples > 0)
        ? c.total_latency_ms / static_cast<double>(c.latency_samples)
        : 0.0;

    // Unix timestamp in milliseconds
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();

    // ── Serialise to JSON ─────────────────────────────────────────────────────
    std::ostringstream json;
    json << std::fixed << std::setprecision(3);
    json << "{"
         << "\"ts\":"            << now_ms            << ","
         << "\"submission_id\":\"" << submission_id << "\","
         << "\"thread_id\":"     << thread_id         << ","
         << "\"http_200\":"      << c.counts[HTTP_200]  << ","
         << "\"http_4xx\":"      << c.counts[HTTP_4XX]  << ","
         << "\"http_5xx\":"      << c.counts[HTTP_5XX]  << ","
         << "\"timeout\":"       << c.counts[TIMEOUT]   << ","
         << "\"econnref\":"      << c.counts[ECONNREF]  << ","
         << "\"other_err\":"     << c.counts[OTHER_ERR] << ","
         << "\"avg_latency_ms\":" << avg_latency        << ","
         << "\"samples\":"       << c.latency_samples   << ","
         // Latency histogram buckets: <1ms, 1-5ms, 5-10ms, 10-50ms, 50ms+
         << "\"hist\":["
             << c.latency_buckets[0] << ","
             << c.latency_buckets[1] << ","
             << c.latency_buckets[2] << ","
             << c.latency_buckets[3] << ","
             << c.latency_buckets[4]
         << "]"
         << "}";

    std::string payload = json.str();

    // ── Produce to Redpanda (fire and forget — async) ─────────────────────────
    int err = rd_kafka_produce(
        rkt_,
        RD_KAFKA_PARTITION_UA,          // auto partition
        RD_KAFKA_MSG_F_COPY,            // copy payload (we own the string)
        const_cast<char*>(payload.data()),
        payload.size(),
        submission_id.c_str(),          // use submission_id as message key
        submission_id.size(),           // for consistent partitioning
        nullptr                         // opaque
    );

    if (err == -1) {
        std::cerr << "[Telemetry] Produce failed: "
                  << rd_kafka_err2str(rd_kafka_last_error()) << "\n";
    }

    // Poll to serve delivery callbacks (keeps internal queue drained)
    rd_kafka_poll(rk_, 0);
}
