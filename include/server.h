#pragma once

#include "network.h"
#include "protocol.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace dkv {

struct LatencySnapshot {
    uint64_t total_requests{0};
    double avg_us{0.0};
    uint64_t p50_us{0};
    uint64_t p95_us{0};
    uint64_t p99_us{0};
};

class LatencyTracker {
public:
    explicit LatencyTracker(size_t max_samples = 16384) : max_samples_(max_samples) {
        samples_.reserve(max_samples_);
    }

    void record(uint64_t micros);
    [[nodiscard]] LatencySnapshot snapshot() const;

private:
    size_t max_samples_;
    mutable std::mutex mutex_;
    std::vector<uint64_t> samples_;
    size_t cursor_{0};
    uint64_t total_count_{0};
    uint64_t total_sum_us_{0};
};

struct ServerConfig {
    std::string host{"0.0.0.0"};
    uint16_t port{7001};
    size_t worker_threads{8};
    bool thread_per_client{false};
    int client_timeout_ms{30000};
};

class KvServer {
public:
    using FrameDispatcher = std::function<Frame(const Frame&)>;

    KvServer(ServerConfig cfg, FrameDispatcher dispatcher);
    ~KvServer();

    KvServer(const KvServer&) = delete;
    KvServer& operator=(const KvServer&) = delete;

    void start();
    void stop();

    [[nodiscard]] uint16_t boundPort() const noexcept { return listener_.localPort(); }
    [[nodiscard]] LatencySnapshot metrics() const { return latency_.snapshot(); }

private:
    void acceptLoop();
    void handleClient(TcpSocket client);

    ServerConfig cfg_;
    FrameDispatcher dispatcher_;
    TcpListener listener_;
    std::unique_ptr<ThreadPool> pool_;
    std::atomic<bool> running_{false};
    std::thread accept_thread_;
    std::mutex detached_mutex_;
    std::vector<std::thread> client_threads_;
    LatencyTracker latency_;
};

} // namespace dkv
