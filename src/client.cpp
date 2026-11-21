#include "client.h"
// ⚠️ Before you edit this, please question your life decisions, reconsider your entire existence, and make sure you're mentally prepared for the consequences. Proceed at your own risk. 💀
namespace dkv {

KvClient::KvClient(std::string host, uint16_t port, int timeout_ms, bool follow_redirects)
    : host_(std::move(host)),
      port_(port),
      timeout_ms_(timeout_ms),
      follow_redirects_(follow_redirects) {}

void KvClient::connect() {
    sock_ = TcpSocket::connectTo(host_, port_, timeout_ms_);
}

void KvClient::disconnect() noexcept {
    sock_.close();
}

bool KvClient::parseHostPort(const std::string& endpoint, std::string& host_out, uint16_t& port_out) {
    auto colon = endpoint.rfind(':');
    if (colon == std::string::npos || colon == 0 || colon + 1 >= endpoint.size()) {
        return false;
    }
    host_out = endpoint.substr(0, colon);
    port_out = static_cast<uint16_t>(std::stoi(endpoint.substr(colon + 1)));
    return true;
}

ServerResponse KvClient::execute(const ClientRequest& req) {
    for (int redirects = 0; redirects < 4; ++redirects) {
        if (!sock_.isValid()) {
            connect();
        }
        auto payload = req.serialize();
        sock_.sendFrame(MessageType::CLIENT_CMD, payload);
        auto maybe_frame = sock_.recvFrame();
        if (!maybe_frame.has_value()) {
            sock_.close();
            throw NetworkError("Server closed connection before sending response");
        }
        auto resp = ServerResponse::deserialize(maybe_frame->payload);
        if (resp.status == StatusCode::NOT_LEADER && follow_redirects_ && !resp.leader_hint.empty()) {
            std::string new_host;
            uint16_t new_port = 0;
            if (parseHostPort(resp.leader_hint, new_host, new_port)) {
                disconnect();
                host_ = new_host;
                port_ = new_port;
                continue;
            }
        }
        return resp;
    }
    ServerResponse fail;
    fail.status = StatusCode::NOT_LEADER;
    fail.message = "Exceeded maximum leader redirects";
    return fail;
}

ServerResponse KvClient::set(const std::string& key, const std::string& value) {
    return execute(ClientRequest{ClientCmdType::SET, key, value});
}

ServerResponse KvClient::get(const std::string& key) {
    return execute(ClientRequest{ClientCmdType::GET, key, ""});
}

ServerResponse KvClient::del(const std::string& key) {
    return execute(ClientRequest{ClientCmdType::DELETE_CMD, key, ""});
}

ServerResponse KvClient::exists(const std::string& key) {
    return execute(ClientRequest{ClientCmdType::EXISTS, key, ""});
}

ServerResponse KvClient::update(const std::string& key, const std::string& value) {
    return execute(ClientRequest{ClientCmdType::UPDATE, key, value});
}

ServerResponse KvClient::triggerSnapshot() {
    return execute(ClientRequest{ClientCmdType::SNAPSHOT, "", ""});
}

ServerResponse KvClient::stats() {
    return execute(ClientRequest{ClientCmdType::STATS, "", ""});
}

ServerResponse KvClient::ping() {
    return execute(ClientRequest{ClientCmdType::PING, "", ""});
}

} // namespace dkv
