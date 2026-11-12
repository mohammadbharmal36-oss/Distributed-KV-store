#include "client.h"
#include "node.h"

#include <gtest/gtest.h>
#include <filesystem>
#include <thread>
#include <vector>

namespace dkv::testing {

class NetworkIntegrationTest : public ::testing::Test {
protected:
    std::filesystem::path dir_;
    std::unique_ptr<ClusterNode> node_;
    uint16_t port_{0};

    void SetUp() override {
        dir_ = std::filesystem::temp_directory_path() /
               ("dkv_net_test_" + std::to_string(reinterpret_cast<uintptr_t>(this)));
        std::filesystem::remove_all(dir_);

        NodeConfig cfg;
        cfg.id = 1;
        cfg.host = "127.0.0.1";
        cfg.client_port = 0; // OS-assigned ephemeral port
        cfg.data_dir = dir_;
        cfg.mode = ClusterMode::STANDALONE;
        node_ = std::make_unique<ClusterNode>(cfg);
        node_->start();
        port_ = node_->clientPort();
    }

    void TearDown() override {
        if (node_) node_->stop();
        std::filesystem::remove_all(dir_);
    }
};

TEST_F(NetworkIntegrationTest, HandlesByteByByteFragmentedTcpStream) {
    // Verify server correctly reassembles TCP byte stream even when 1 byte arrives per send()
    ClientRequest req{ClientCmdType::SET, "stream_key", "fragmented_tcp_payload"};
    auto wire = encodeFrame(MessageType::CLIENT_CMD, req.serialize());

    TcpSocket raw = TcpSocket::connectTo("127.0.0.1", port_);
    for (uint8_t byte : wire) {
        raw.sendAll(std::span<const uint8_t>(&byte, 1));
    }
    auto reply_frame = raw.recvFrame();
    ASSERT_TRUE(reply_frame.has_value());
    auto resp = ServerResponse::deserialize(reply_frame->payload);
    EXPECT_EQ(resp.status, StatusCode::OK);

    KvClient verify("127.0.0.1", port_);
    auto get_resp = verify.get("stream_key");
    EXPECT_EQ(get_resp.status, StatusCode::OK);
    EXPECT_EQ(get_resp.value, "fragmented_tcp_payload");
}

TEST_F(NetworkIntegrationTest, SurvivesMalformedClientDataWithoutCrashing) {
    {
        TcpSocket bad = TcpSocket::connectTo("127.0.0.1", port_);
        std::vector<uint8_t> garbage(32, 0xFF);
        bad.sendAll(garbage);
        // Server must reject bad magic header and stay healthy
        auto maybe = bad.recvFrame();
        if (maybe.has_value()) {
            auto r = ServerResponse::deserialize(maybe->payload);
            EXPECT_EQ(r.status, StatusCode::BAD_REQUEST);
        }
    }

    // Subsequent legitimate client must succeed without issue
    KvClient healthy("127.0.0.1", port_);
    EXPECT_EQ(healthy.set("alive", "yes").status, StatusCode::OK);
    EXPECT_EQ(healthy.get("alive").value, "yes");
}

TEST_F(NetworkIntegrationTest, ConcurrentClientsExecuteSimultaneousOperations) {
    constexpr int kClients = 12;
    constexpr int kOpsPerClient = 100;

    std::vector<std::thread> clients;
    for (int c = 0; c < kClients; ++c) {
        clients.emplace_back([&, c] {
            KvClient cli("127.0.0.1", port_);
            cli.connect();
            for (int i = 0; i < kOpsPerClient; ++i) {
                std::string k = "c" + std::to_string(c) + "_k" + std::to_string(i);
                ASSERT_EQ(cli.set(k, "v1").status, StatusCode::OK);
                ASSERT_EQ(cli.update(k, "v2").status, StatusCode::OK);
                auto g = cli.get(k);
                ASSERT_EQ(g.status, StatusCode::OK);
                EXPECT_EQ(g.value, "v2");
            }
        });
    }
    for (auto& t : clients) t.join();
    EXPECT_EQ(node_->storage().size(), static_cast<size_t>(kClients * kOpsPerClient));
}

} // namespace dkv::testing
