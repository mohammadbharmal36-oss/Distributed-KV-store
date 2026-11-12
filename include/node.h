#pragma once

#include "raft.h"
#include "replication.h"
#include "server.h"
#include "storage.h"
#include "wal.h"

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace dkv {

enum class ClusterMode {
    STANDALONE,
    PRIMARY_REPLICA,
    RAFT
};

struct NodeConfig {
    uint32_t id{1};
    std::string host{"127.0.0.1"};
    uint16_t client_port{7001};
    uint16_t peer_port{7101};
    std::filesystem::path data_dir{"./data/node1"};
    ClusterMode mode{ClusterMode::RAFT};
    bool is_primary{true};
    SyncMode sync_mode{SyncMode::SYNC_MAJORITY};
    size_t worker_threads{8};
    std::vector<PeerEndpoint> peers;
};

class ClusterNode {
public:
    explicit ClusterNode(NodeConfig cfg);
    ~ClusterNode();

    void start();
    void stop();

    [[nodiscard]] uint16_t clientPort() const noexcept;
    [[nodiscard]] uint16_t peerPort() const noexcept;
    [[nodiscard]] StorageEngine& storage() noexcept { return storage_; }
    [[nodiscard]] RaftNode* raft() noexcept { return raft_.get(); }

private:
    Frame handleClientFrame(const Frame& frame);
    Frame handlePeerFrame(const Frame& frame);
    ServerResponse executeClientRequest(const ClientRequest& req);
    std::string lookupLeaderEndpoint(uint32_t leader_id) const;

    NodeConfig cfg_;
    StorageEngine storage_;
    std::unique_ptr<WriteAheadLog> standalone_wal_;
    uint64_t standalone_seq_{0};
    std::mutex standalone_mutex_;

    std::unique_ptr<ReplicationManager> repl_;
    std::unique_ptr<RaftNode> raft_;
    std::unique_ptr<KvServer> client_server_;
    std::unique_ptr<KvServer> peer_server_;
};

} // namespace dkv
