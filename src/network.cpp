#include "network.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

namespace dkv {

TcpSocket::~TcpSocket() {
    close();
}

TcpSocket::TcpSocket(TcpSocket&& other) noexcept : fd_(other.fd_) {
    other.fd_ = -1;
}

TcpSocket& TcpSocket::operator=(TcpSocket&& other) noexcept {
    if (this != &other) {
        close();
        fd_ = other.fd_;
        other.fd_ = -1;
    }
    return *this;
}

TcpSocket TcpSocket::connectTo(const std::string& host, uint16_t port, int timeout_ms) {
    struct addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo* res = nullptr;
    std::string port_str = std::to_string(port);
    int rc = ::getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res);
    if (rc != 0 || res == nullptr) {
        throw NetworkError("getaddrinfo failed for " + host + ":" + port_str);
    }

    int sfd = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (sfd < 0) {
        ::freeaddrinfo(res);
        throw NetworkError(std::string("socket() failed: ") + std::strerror(errno));
    }

    int flags = ::fcntl(sfd, F_GETFL, 0);
    ::fcntl(sfd, F_SETFL, flags | O_NONBLOCK);

    int conn_rc = ::connect(sfd, res->ai_addr, res->ai_addrlen);
    ::freeaddrinfo(res);

    if (conn_rc < 0 && errno != EINPROGRESS) {
        ::close(sfd);
        throw NetworkError(std::string("connect() failed: ") + std::strerror(errno));
    }

    if (conn_rc < 0) {
        struct pollfd pfd{};
        pfd.fd = sfd;
        pfd.events = POLLOUT;
        int prc = ::poll(&pfd, 1, timeout_ms);
        if (prc <= 0) {
            ::close(sfd);
            throw NetworkError("TCP connect timed out to " + host + ":" + port_str);
        }
        int so_error = 0;
        socklen_t len = sizeof(so_error);
        ::getsockopt(sfd, SOL_SOCKET, SO_ERROR, &so_error, &len);
        if (so_error != 0) {
            ::close(sfd);
            throw NetworkError(std::string("Async connect failed: ") + std::strerror(so_error));
        }
    }

    ::fcntl(sfd, F_SETFL, flags);
    TcpSocket sock(sfd);
    sock.setNoDelay(true);
    sock.setTimeouts(timeout_ms, timeout_ms);
    return sock;
}

void TcpSocket::setTimeouts(int recv_timeout_ms, int send_timeout_ms) {
    if (fd_ < 0) return;
    struct timeval rtv{};
    rtv.tv_sec = recv_timeout_ms / 1000;
    rtv.tv_usec = (recv_timeout_ms % 1000) * 1000;
    ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &rtv, sizeof(rtv));

    struct timeval stv{};
    stv.tv_sec = send_timeout_ms / 1000;
    stv.tv_usec = (send_timeout_ms % 1000) * 1000;
    ::setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &stv, sizeof(stv));
}

void TcpSocket::setNoDelay(bool enable) {
    if (fd_ < 0) return;
    int flag = enable ? 1 : 0;
    ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
}

void TcpSocket::sendAll(std::span<const uint8_t> data) {
    if (fd_ < 0) throw NetworkError("Attempted sendAll on closed socket");
    size_t sent = 0;
    while (sent < data.size()) {
        ssize_t n = ::send(fd_, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            throw NetworkError(std::string("TCP send failed: ") + std::strerror(errno));
        }
        if (n == 0) {
            throw NetworkError("TCP peer closed connection during send");
        }
        sent += static_cast<size_t>(n);
    }
}

bool TcpSocket::recvExact(std::span<uint8_t> buf) {
    if (fd_ < 0) throw NetworkError("Attempted recvExact on closed socket");
    size_t got = 0;
    while (got < buf.size()) {
        ssize_t n = ::recv(fd_, buf.data() + got, buf.size() - got, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            throw NetworkError(std::string("TCP recv failed: ") + std::strerror(errno));
        }
        if (n == 0) {
            if (got == 0) return false; // Clean client disconnect between frames
            throw ProtocolError("Unexpected EOF mid-frame on TCP byte stream");
        }
        got += static_cast<size_t>(n);
    }
    return true;
}

void TcpSocket::sendFrame(MessageType type, std::span<const uint8_t> payload, uint16_t flags) {
    auto encoded = encodeFrame(type, payload, flags);
    sendAll(encoded);
}

std::optional<Frame> TcpSocket::recvFrame() {
    std::array<uint8_t, FRAME_HEADER_SIZE> raw_hdr{};
    if (!recvExact(raw_hdr)) {
        return std::nullopt;
    }
    FrameHeader hdr = decodeHeader(raw_hdr);
    Frame frame;
    frame.type = hdr.type;
    frame.flags = hdr.flags;
    frame.payload.resize(hdr.payload_len);
    if (hdr.payload_len > 0) {
        if (!recvExact(frame.payload)) {
            throw ProtocolError("Truncated frame payload");
        }
    }
    uint32_t actual_crc = computeCrc32(frame.payload);
    if (actual_crc != hdr.crc32) {
        throw ProtocolError("Frame CRC32 mismatch");
    }
    return frame;
}

void TcpSocket::shutdownBoth() noexcept {
    if (fd_ >= 0) {
        ::shutdown(fd_, SHUT_RDWR);
    }
}

void TcpSocket::close() noexcept {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

TcpListener::~TcpListener() {
    stop();
}

void TcpListener::bindAndListen(const std::string& host, uint16_t port, int backlog) {
    stop();
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        throw NetworkError(std::string("socket() failed: ") + std::strerror(errno));
    }

    int opt = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (host.empty() || host == "0.0.0.0") {
        addr.sin_addr.s_addr = INADDR_ANY;
    } else if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        throw NetworkError("Invalid IPv4 bind address: " + host);
    }

    if (::bind(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        int err = errno;
        ::close(listen_fd_);
        listen_fd_ = -1;
        throw NetworkError(std::string("bind() failed: ") + std::strerror(err));
    }

    struct sockaddr_in actual{};
    socklen_t alen = sizeof(actual);
    if (::getsockname(listen_fd_, reinterpret_cast<struct sockaddr*>(&actual), &alen) == 0) {
        bound_port_ = ntohs(actual.sin_port);
    } else {
        bound_port_ = port;
    }

    if (::listen(listen_fd_, backlog) < 0) {
        int err = errno;
        ::close(listen_fd_);
        listen_fd_ = -1;
        throw NetworkError(std::string("listen() failed: ") + std::strerror(err));
    }
}

std::optional<TcpSocket> TcpListener::acceptClient(std::string* peer_out) {
    if (listen_fd_ < 0) return std::nullopt;
    struct sockaddr_in caddr{};
    socklen_t clen = sizeof(caddr);
    while (true) {
        int cfd = ::accept(listen_fd_, reinterpret_cast<struct sockaddr*>(&caddr), &clen);
        if (cfd < 0) {
            if (errno == EINTR) continue;
            return std::nullopt;
        }
        if (peer_out != nullptr) {
            char ip[INET_ADDRSTRLEN]{};
            ::inet_ntop(AF_INET, &caddr.sin_addr, ip, sizeof(ip));
            *peer_out = std::string(ip) + ":" + std::to_string(ntohs(caddr.sin_port));
        }
        TcpSocket client(cfd);
        client.setNoDelay(true);
        return client;
    }
}

void TcpListener::stop() noexcept {
    if (listen_fd_ >= 0) {
        ::shutdown(listen_fd_, SHUT_RDWR);
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
}

ThreadPool::ThreadPool(size_t num_workers) {
    if (num_workers == 0) num_workers = 4;
    workers_.reserve(num_workers);
    for (size_t i = 0; i < num_workers; ++i) {
        workers_.emplace_back([this] { workerLoop(); });
    }
}

ThreadPool::~ThreadPool() {
    shutdown();
}

bool ThreadPool::submit(std::function<void()> task) {
    {
        std::lock_guard lock(mutex_);
        if (stopping_) return false;
        tasks_.push(std::move(task));
    }
    cv_.notify_one();
    return true;
}

void ThreadPool::shutdown() {
    {
        std::lock_guard lock(mutex_);
        if (stopping_) return;
        stopping_ = true;
    }
    cv_.notify_all();
    for (auto& w : workers_) {
        if (w.joinable()) w.join();
    }
}

void ThreadPool::workerLoop() {
    while (true) {
        std::function<void()> task;
        {
            std::unique_lock lock(mutex_);
            cv_.wait(lock, [this] { return stopping_ || !tasks_.empty(); });
            if (stopping_ && tasks_.empty()) return;
            task = std::move(tasks_.front());
            tasks_.pop();
        }
        task();
    }
}

} // namespace dkv
