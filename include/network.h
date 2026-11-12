#pragma once

#include "protocol.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <queue>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace dkv {

class NetworkError : public std::runtime_error {
public:
    explicit NetworkError(const std::string& msg) : std::runtime_error(msg) {}
};

/**
 * RAII wrapper around a POSIX TCP socket file descriptor.
 * Handles TCP stream partial reads/writes, socket timeouts, TCP_NODELAY, and SIGPIPE suppression.
 */
class TcpSocket {
public:
    TcpSocket() = default;
    explicit TcpSocket(int fd) noexcept : fd_(fd) {}
    ~TcpSocket();

    TcpSocket(const TcpSocket&) = delete;
    TcpSocket& operator=(const TcpSocket&) = delete;

    TcpSocket(TcpSocket&& other) noexcept;
    TcpSocket& operator=(TcpSocket&& other) noexcept;

    static TcpSocket connectTo(const std::string& host, uint16_t port, int timeout_ms = 2000);

    void setTimeouts(int recv_timeout_ms, int send_timeout_ms);
    void setNoDelay(bool enable);

    void sendAll(std::span<const uint8_t> data);
    bool recvExact(std::span<uint8_t> buf); // returns false on clean EOF before any byte read

    void sendFrame(MessageType type, std::span<const uint8_t> payload, uint16_t flags = 0);
    std::optional<Frame> recvFrame();

    void shutdownBoth() noexcept;
    void close() noexcept;
    [[nodiscard]] bool isValid() const noexcept { return fd_ >= 0; }
    [[nodiscard]] int fd() const noexcept { return fd_; }

private:
    int fd_{-1};
};

class TcpListener {
public:
    TcpListener() = default;
    ~TcpListener();

    TcpListener(const TcpListener&) = delete;
    TcpListener& operator=(const TcpListener&) = delete;

    void bindAndListen(const std::string& host, uint16_t port, int backlog = 128);
    std::optional<TcpSocket> acceptClient(std::string* peer_out = nullptr);
    void stop() noexcept;

    [[nodiscard]] uint16_t localPort() const noexcept { return bound_port_; }

private:
    int listen_fd_{-1};
    uint16_t bound_port_{0};
};

/**
 * Bounded/worker thread pool for serving concurrent client sessions and RPC tasks.
 */
class ThreadPool {
public:
    explicit ThreadPool(size_t num_workers);
    ~ThreadPool();

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    bool submit(std::function<void()> task);
    void shutdown();

private:
    void workerLoop();

    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool stopping_{false};
};

} // namespace dkv
