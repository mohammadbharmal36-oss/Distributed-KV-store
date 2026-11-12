#pragma once

#include "network.h"
#include "protocol.h"

#include <cstdint>
#include <optional>
#include <string>

namespace dkv {

class KvClient {
public:
    KvClient(std::string host, uint16_t port, int timeout_ms = 3000, bool follow_redirects = true);
    ~KvClient() = default;

    void connect();
    void disconnect() noexcept;
    [[nodiscard]] bool isConnected() const noexcept { return sock_.isValid(); }

    ServerResponse set(const std::string& key, const std::string& value);
    ServerResponse get(const std::string& key);
    ServerResponse del(const std::string& key);
    ServerResponse exists(const std::string& key);
    ServerResponse update(const std::string& key, const std::string& value);
    ServerResponse triggerSnapshot();
    ServerResponse stats();
    ServerResponse ping();

    ServerResponse execute(const ClientRequest& req);

private:
    static bool parseHostPort(const std::string& endpoint, std::string& host_out, uint16_t& port_out);

    std::string host_;
    uint16_t port_;
    int timeout_ms_;
    bool follow_redirects_;
    TcpSocket sock_;
};

} // namespace dkv
