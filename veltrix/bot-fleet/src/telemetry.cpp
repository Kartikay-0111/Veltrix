#include "telemetry.hpp"

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>

namespace beast = boost::beast;
namespace http = beast::http;

TelemetryProducer::TelemetryProducer(std::string host, std::string port)
    : host_(std::move(host)), port_(std::move(port))
{
}

void TelemetryProducer::flush(const TelemetryCounters &c,
                              const std::string &submission_id,
                              int thread_id)
{
    if (c.latency_samples == 0 && c.counts[HTTP_200] == 0)
        return;

    double avg_latency = (c.latency_samples > 0)
                             ? c.total_latency_ms / static_cast<double>(c.latency_samples)
                             : 0.0;

    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::system_clock::now().time_since_epoch())
                      .count();

    std::ostringstream json;
    json << std::fixed << std::setprecision(3);
    json << "{";
    json << "\"ts\":" << now_ms << ",";
    json << "\"submission_id\":\"" << submission_id << "\",";
    json << "\"thread_id\":" << thread_id << ",";
    json << "\"http_200\":" << c.counts[HTTP_200] << ",";
    json << "\"http_4xx\":" << c.counts[HTTP_4XX] << ",";
    json << "\"http_5xx\":" << c.counts[HTTP_5XX] << ",";
    json << "\"timeout\":" << c.counts[TIMEOUT] << ",";
    json << "\"econnref\":" << c.counts[ECONNREF] << ",";
    json << "\"other_err\":" << c.counts[OTHER_ERR] << ",";
    json << "\"avg_latency_ms\":" << avg_latency << ",";
    json << "\"samples\":" << c.latency_samples << ",";
    json << "\"hist\":["
         << c.latency_buckets[0] << ","
         << c.latency_buckets[1] << ","
         << c.latency_buckets[2] << ","
         << c.latency_buckets[3] << ","
         << c.latency_buckets[4]
         << "]";
    json << "}";

    std::string payload = json.str();

    try
    {
        asio::io_context ioc;
        tcp::resolver resolver(ioc);
        beast::tcp_stream stream(ioc);
        auto endpoints = resolver.resolve(host_, port_);
        stream.connect(endpoints);

        http::request<http::string_body> req{http::verb::post, "/metrics", 11};
        req.set(http::field::host, host_ + ":" + port_);
        req.set(http::field::content_type, "application/json");
        req.set(http::field::connection, "close");
        req.body() = payload;
        req.prepare_payload();

        http::write(stream, req);

        beast::flat_buffer buffer;
        http::response<http::string_body> res;
        http::read(stream, buffer, res);
        stream.socket().shutdown(tcp::socket::shutdown_both);
    }
    catch (const std::exception &e)
    {
        std::cerr << "[Telemetry] HTTP flush failed to "
                  << host_ << ":" << port_ << " - " << e.what() << "\n";
    }
}
