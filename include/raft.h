#pragma once

#include "protocol.h"
#include "replication.h"
#include "storage.h"
#include "wal.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace dkv {

enum class RaftRole {
    FOLLOWER,
    CANDIDATE,
    LEADER
};

struct ProposeResult {
    bool accepted{false};
    bool committed{false};
    uint64_t index{0};
    uint64_t term{0};
    uint32_t leader_id{0};
};

/**
 * Safety-complete Raft Consensus Module (Ongaro & Ousterhout §5).
 *
 * Persistent State on all servers (flushed before replying to RPCs):
 *   - current_term_  (stored in raft_meta.dat)
 *   - voted_for_     (stored in raft_meta.dat)
 *   - log_           (stored in WriteAheadLog + optional compacted Snapshot)
 *
 * Enforced Safety Invariants:
 *   1. Election Safety (§5.2): At most one leader can be elected in a given term.
 *   2. Leader Append-Only (§5.3): A leader never overwrites or deletes its own entries.
 *   3. Log Matching (§5.3): Identical (index, term) implies identical prefix up to index.
 *   4. Election Restriction (§5.4.1): Candidate log must be at least as up-to-date as voter's.
 *   5. State Machine Commitment Safety (§5.4.2): Leader only commits entries from current_term_
 *      by counting replicas; prior-term entries are committed indirectly.
 */
class RaftNode {
public:
    using RpcFunc = std::function<std::optional<Frame>(const PeerEndpoint&, MessageType, const std::vector<uint8_t>&)>;

    RaftNode(uint32_t node_id,
             std::vector<PeerEndpoint> peers,
             std::filesystem::path data_dir,
             StorageEngine& storage,
             RpcFunc rpc_sender = nullptr);
    ~RaftNode();

    RaftNode(const RaftNode&) = delete;
    RaftNode& operator=(const RaftNode&) = delete;

    void start();
    void stop();

    // Client command proposal on Leader
    ProposeResult propose(OpType op, const std::string& key, const std::string& value, int timeout_ms = 2000);

    // Deterministic step hooks (used by both background timer thread and deterministic unit tests)
    void triggerElection();
    void broadcastAppendEntries();

    // Raft RPC handlers
    RequestVoteReply handleRequestVote(const RequestVoteArgs& args);
    AppendEntriesReply handleAppendEntries(const AppendEntriesArgs& args);

    // Log compaction / snapshotting
    bool createSnapshot();

    [[nodiscard]] RaftRole role() const;
    [[nodiscard]] uint64_t currentTerm() const;
    [[nodiscard]] uint64_t commitIndex() const;
    [[nodiscard]] uint64_t lastApplied() const;
    [[nodiscard]] uint32_t currentLeaderId() const;
    [[nodiscard]] uint32_t id() const noexcept { return id_; }
    [[nodiscard]] size_t logSize() const;

private:
    void loadPersistentState();
    void persistMetadataLocked();
    [[nodiscard]] uint64_t lastLogIndexLocked() const;
    [[nodiscard]] uint64_t lastLogTermLocked() const;
    [[nodiscard]] uint64_t termAtLocked(uint64_t index) const;
    [[nodiscard]] const LogRecord* recordAtLocked(uint64_t index) const;

    void becomeFollowerLocked(uint64_t new_term, uint32_t leader_hint = 0);
    void advanceCommitIndexLocked();
    void applyCommittedLocked();
    void resetElectionDeadlineLocked();
    void tickerLoop();

    static std::optional<Frame> defaultTcpRpc(const PeerEndpoint& peer,
                                              MessageType type,
                                              const std::vector<uint8_t>& payload);

    uint32_t id_;
    std::vector<PeerEndpoint> peers_;
    std::filesystem::path data_dir_;
    std::filesystem::path meta_path_;
    std::filesystem::path snap_path_;
    StorageEngine& storage_;
    WriteAheadLog wal_;
    RpcFunc rpc_sender_;

    mutable std::mutex mutex_;
    std::condition_variable commit_cv_;

    // Persistent state
    uint64_t current_term_{0};
    std::optional<uint32_t> voted_for_;
    std::vector<LogRecord> log_;
    uint64_t base_snap_index_{0};
    uint64_t base_snap_term_{0};

    // Volatile state
    RaftRole role_{RaftRole::FOLLOWER};
    uint32_t leader_id_{0};
    uint64_t commit_index_{0};
    uint64_t last_applied_{0};

    // Leader volatile state
    std::unordered_map<uint32_t, uint64_t> next_index_;
    std::unordered_map<uint32_t, uint64_t> match_index_;

    // Timers
    std::mt19937 rng_;
    std::chrono::steady_clock::time_point election_deadline_;
    std::chrono::steady_clock::time_point last_heartbeat_sent_;

    std::atomic<bool> running_{false};
    std::thread ticker_thread_;
};

} // namespace dkv
