#include "server.h"

#include <chrono>

namespace dkv {

void LatencyTracker::record(uint64_t micros) {
    std::lock_guard lock(mutex_);
    ++total_count_;
    total_sum_us_ += micros;
    if (samples_.size() < max_samples_) {
        samples_.push_back(micros);
    } else {
        samples_[cursor_] = micros;
        cursor_ = (cursor_ + 1) % max_samples_;
    }
}

LatencySnapshot LatencyTracker::snapshot() const {
    std::lock_guard lock(mutex_);
    LatencySnapshot out{};
    out.total_requests = total_count_;
    if (samples_.empty()) return out;

    out.avg_us = static_cast<double>(total_sum_us_) / static_cast<double>(total_count_);
    std::vector<uint64_t> sorted = samples_;
    std::sort(sorted.begin(), sorted.end());
    auto pick = [&](double q) -> uint64_t {
        size_t idx = static_cast<size_t>(q * static_cast<double>(sorted.size() - 1));
        return sorted[idx];
    };
    out.p50_us = pick(0.50);
    out.p95_us = pick(0.95);
    out.p99_us = pick(0.99);
    return out;
}

KvServer::KvServer(ServerConfig cfg, FrameDispatcher dispatcher)
    : cfg_(std::move(cfg)), dispatcher_(std::move(dispatcher)) {}

KvServer::~KvServer() {
    stop();
}

void KvServer::start() {
    if (running_.exchange(true)) return;
    listener_.bindAndListen(cfg_.host, cfg_.port);
    if (!cfg_.thread_per_client) {
        pool_ = std::make_unique<ThreadPool>(cfg_.worker_threads);
    }
    accept_thread_ = std::thread([this] { acceptLoop(); });
}

void KvServer::stop() {
    if (!running_.exchange(false)) return;
    listener_.stop();
    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }
    if (pool_) {
        pool_->shutdown();
        pool_.reset();
    }
    std::lock_guard lock(detached_mutex_);
    for (auto& t : client_threads_) {
        if (t.joinable()) t.join();
    }
    client_threads_.clear();
}

void KvServer::acceptLoop() {
    while (running_.load(std::memory_order_relaxed)) {
        auto maybe_sock = listener_.acceptClient();
        if (!maybe_sock.has_value()) {
            if (!running_.load()) break;
            continue;
        }
        auto sock_ptr = std::make_shared<TcpSocket>(std::move(*maybe_sock));
        if (cfg_.thread_per_client) {
            std::lock_guard lock(detached_mutex_);
            client_threads_.emplace_back([this, sock_ptr]() mutable {
                handleClient(std::move(*sock_ptr));
            });
        } else {
            pool_->submit([this, sock_ptr]() mutable {
                handleClient(std::move(*sock_ptr));
            });
        }
    }
}

void KvServer::handleClient(TcpSocket client) {
    client.setTimeouts(cfg_.client_timeout_ms, cfg_.client_timeout_ms);
    while (running_.load(std::memory_order_relaxed)) {
        try {
            auto maybe_frame = client.recvFrame();
            if (!maybe_frame.has_value()) {
                break; // Clean disconnect
            }
            const auto t0 = std::chrono::steady_clock::now();
            Frame reply = dispatcher_(*maybe_frame);
            const auto t1 = std::chrono::steady_clock::now();
            auto us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
            latency_.record(static_cast<uint64_t>(us));

            client.sendFrame(reply.type, reply.payload, reply.flags);
        } catch (const ProtocolError& pe) {
            // Return structured error response on malformed request without crashing server
            ServerResponse err;
            err.status = StatusCode::BAD_REQUEST;
            err.message = pe.what();
            try {
                auto payload = err.serialize();
                client.sendFrame(MessageType::SERVER_RESP, payload);
            } catch (...) {}
            break;
        } catch (const NetworkError&) {
            break;
        } catch (const std::exception& ex) {
            ServerResponse err;
            err.status = StatusCode::INTERNAL_ERROR;
            err.message = ex.what();
            try {
                auto payload = err.serialize();
                client.sendFrame(MessageType::SERVER_RESP, payload);
            } catch (...) {}
            break;
        }
    }
    client.close();
}

} // namespace dkv
