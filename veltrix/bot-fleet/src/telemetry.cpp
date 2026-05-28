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
        struct addrinfo hints{}, *res;
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        if (getaddrinfo(host_.c_str(), port_.c_str(), &hints, &res) != 0)
            return;

        int sock = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
        if (sock < 0) {
            freeaddrinfo(res);
            return;
        }

        if (connect(sock, res->ai_addr, res->ai_addrlen) < 0) {
            close(sock);
            freeaddrinfo(res);
            return;
        }
        freeaddrinfo(res);

        std::ostringstream req;
        req << "POST /metrics HTTP/1.1\r\n"
            << "Host: " << host_ << ":" << port_ << "\r\n"
            << "Content-Type: application/json\r\n"
            << "Content-Length: " << payload.size() << "\r\n"
            << "Connection: close\r\n"
            << "\r\n"
            << payload;

        std::string request_str = req.str();
        send(sock, request_str.c_str(), request_str.size(), 0);

        char buf[1024];
        while (recv(sock, buf, sizeof(buf), 0) > 0) {}

        close(sock);
    }
    catch (const std::exception &e)
    {
        std::cerr << "[Telemetry] HTTP flush failed to "
                  << host_ << ":" << port_ << " - " << e.what() << "\n";
    }
}
