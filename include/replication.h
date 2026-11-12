#pragma once

#include "network.h"
#include "protocol.h"
#include "storage.h"
#include "wal.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace dkv {

enum class SyncMode {
    ASYNC,
    SYNC_MAJORITY,
    SYNC_ALL
};

enum class PeerHealth {
    HEALTHY,
    SUSPECT,
    UNREACHABLE
};

struct PeerEndpoint {
    uint32_t id{0};
    std::string host{"127.0.0.1"};
    uint16_t peer_port{7101};
    uint16_t client_port{7001};
};

struct PeerStatus {
    uint32_t id{0};
    PeerHealth health{PeerHealth::HEALTHY};
    uint64_t acked_seq{0};
    uint32_t missed_heartbeats{0};
    double smoothed_rtt_ms{0.0};
};

/**
 * Multi-sample Failure Detector.
 * Never declares a peer permanently dead after a single missed heartbeat:
 *   0 .. suspect_threshold-1  -> HEALTHY
 *   suspect_threshold .. dead_threshold-1 -> SUSPECT
 *   >= dead_threshold         -> UNREACHABLE (automatically recovers to HEALTHY on next PONG)
 */
class FailureDetector {
public:
    FailureDetector(uint32_t suspect_threshold = 3, uint32_t dead_threshold = 6);

    void registerPeer(uint32_t peer_id);
    void recordSuccess(uint32_t peer_id, double rtt_ms);
    void recordMiss(uint32_t peer_id);

    [[nodiscard]] PeerHealth healthOf(uint32_t peer_id) const;
    [[nodiscard]] uint32_t missedCount(uint32_t peer_id) const;
    [[nodiscard]] double rttOf(uint32_t peer_id) const;

private:
    struct Entry {
        uint32_t missed{0};
        double srtt_ms{0.0};
        PeerHealth state{PeerHealth::HEALTHY};
    };

    uint32_t suspect_threshold_;
    uint32_t dead_threshold_;
    mutable std::mutex mutex_;
    std::unordered_map<uint32_t, Entry> peers_;
};

/**
 * Primary-Replica Replication Coordinator (Phases 7-9).
 * Supports both synchronous (majority or all-Ack) and asynchronous WAL shipping,
 * automatic lagging-replica catch-up on reconnect, and periodic heartbeat health checks.
 */
class ReplicationManager {
public:
    using RpcTransport = std::function<std::optional<Frame>(const PeerEndpoint&, MessageType, const std::vector<uint8_t>&)>;

    ReplicationManager(uint32_t self_id,
                       bool is_primary,
                       SyncMode sync_mode,
                       std::vector<PeerEndpoint> peers,
                       StorageEngine& storage,
                       WriteAheadLog& wal,
                       RpcTransport transport = nullptr);
    ~ReplicationManager();

    void start(int heartbeat_interval_ms = 150);
    void stop();

    // Primary write entrypoint: appends to local WAL, replicates according to SyncMode
    bool replicateWrite(OpType op, const std::string& key, const std::string& value, int quorum_timeout_ms = 1500);

    // Replica RPC handlers
    ReplAckMsg onReplicaAppend(const ReplAppendMsg& msg);
    HeartbeatMsg onHeartbeatPing(const HeartbeatMsg& ping);

    [[nodiscard]] bool isPrimary() const noexcept { return is_primary_.load(); }
    [[nodiscard]] bool primarySuspectedDead() const;
    [[nodiscard]] uint64_t localLastSeq() const;
    [[nodiscard]] std::vector<PeerStatus> peerStatuses() const;

private:
    void backgroundLoop(int interval_ms);
    void shipPendingToPeer(const PeerEndpoint& peer);
    static std::optional<Frame> defaultTcpTransport(const PeerEndpoint& peer,
                                                    MessageType type,
                                                    const std::vector<uint8_t>& payload);

    uint32_t self_id_;
    std::atomic<bool> is_primary_;
    SyncMode sync_mode_;
    std::vector<PeerEndpoint> peers_;
    StorageEngine& storage_;
    WriteAheadLog& wal_;
    RpcTransport transport_;
    FailureDetector detector_;

    mutable std::mutex mutex_;
    std::condition_variable ack_cv_;
    std::vector<LogRecord> history_;
    uint64_t next_seq_{1};
    uint64_t applied_seq_{0};
    std::unordered_map<uint32_t, uint64_t> peer_acked_seq_;
    uint32_t primary_missed_ticks_{0};

    std::atomic<bool> running_{false};
    std::thread bg_thread_;
};

} // namespace dkv
