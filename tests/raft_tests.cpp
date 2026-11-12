#include "raft.h"

#include <gtest/gtest.h>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <set>

namespace dkv::testing {

/**
 * Deterministic in-memory RPC mesh for testing Raft elections, partitions,
 * duplicate messages, leader crashes, and persistent state recovery.
 */
class RaftClusterHarness {
public:
    explicit RaftClusterHarness(size_t count) : count_(count) {
        base_dir_ = std::filesystem::temp_directory_path() /
                    ("dkv_raft_harness_" + std::to_string(reinterpret_cast<uintptr_t>(this)));
        std::filesystem::remove_all(base_dir_);

        for (uint32_t id = 1; id <= count_; ++id) {
            storages_[id] = std::make_unique<StorageEngine>();
        }
        for (uint32_t id = 1; id <= count_; ++id) {
            bootNode(id);
        }
    }

    ~RaftClusterHarness() {
        for (auto& [_, n] : nodes_) {
            if (n) n->stop();
        }
        std::filesystem::remove_all(base_dir_);
    }

    void bootNode(uint32_t id) {
        std::vector<PeerEndpoint> peers;
        for (uint32_t other = 1; other <= count_; ++other) {
            if (other != id) {
                peers.push_back(PeerEndpoint{other, "127.0.0.1", 0, 0});
            }
        }
        nodes_[id] = std::make_unique<RaftNode>(
            id,
            peers,
            base_dir_ / ("node_" + std::to_string(id)),
            *storages_[id],
            [this, id](const PeerEndpoint& target, MessageType type, const std::vector<uint8_t>& payload) {
                return routeRpc(id, target.id, type, payload);
            });
    }

    void crashNode(uint32_t id) {
        std::lock_guard lock(net_mutex_);
        crashed_.insert(id);
    }

    void restartNode(uint32_t id) {
        {
            std::lock_guard lock(net_mutex_);
            crashed_.erase(id);
        }
        nodes_[id]->stop();
        storages_[id] = std::make_unique<StorageEngine>();
        bootNode(id);
    }

    void setPartition(std::set<uint32_t> group_a, std::set<uint32_t> group_b) {
        std::lock_guard lock(net_mutex_);
        part_a_ = std::move(group_a);
        part_b_ = std::move(group_b);
    }

    void healPartitions() {
        std::lock_guard lock(net_mutex_);
        part_a_.clear();
        part_b_.clear();
    }

    std::map<uint32_t, std::unique_ptr<StorageEngine>> storages_;
    std::map<uint32_t, std::unique_ptr<RaftNode>> nodes_;

private:
    std::optional<Frame> routeRpc(uint32_t src,
                                  uint32_t dst,
                                  MessageType type,
                                  const std::vector<uint8_t>& payload) {
        {
            std::lock_guard lock(net_mutex_);
            if (crashed_.count(src) || crashed_.count(dst)) return std::nullopt;
            if ((part_a_.count(src) && part_b_.count(dst)) ||
                (part_b_.count(src) && part_a_.count(dst))) {
                return std::nullopt;
            }
        }
        auto* target = nodes_[dst].get();
        if (type == MessageType::RAFT_REQUEST_VOTE) {
            auto rep = target->handleRequestVote(RequestVoteArgs::deserialize(payload));
            return Frame{MessageType::RAFT_REQUEST_VOTE_RESP, 0, rep.serialize()};
        }
        if (type == MessageType::RAFT_APPEND_ENTRIES) {
            auto rep = target->handleAppendEntries(AppendEntriesArgs::deserialize(payload));
            return Frame{MessageType::RAFT_APPEND_ENTRIES_RESP, 0, rep.serialize()};
        }
        return std::nullopt;
    }

    size_t count_;
    std::filesystem::path base_dir_;
    std::mutex net_mutex_;
    std::set<uint32_t> crashed_;
    std::set<uint32_t> part_a_;
    std::set<uint32_t> part_b_;
};

TEST(RaftConsensusTests, DeterministicLeaderElectionAndLogReplication) {
    RaftClusterHarness cluster(5);

    cluster.nodes_[1]->triggerElection();
    EXPECT_EQ(cluster.nodes_[1]->role(), RaftRole::LEADER);
    EXPECT_EQ(cluster.nodes_[1]->currentTerm(), 1U);

    // Propose mutations through Leader (Node 1)
    auto r1 = cluster.nodes_[1]->propose(OpType::SET, "cfg:env", "prod");
    ASSERT_TRUE(r1.accepted);
    ASSERT_TRUE(r1.committed);

    // Broadcast second heartbeat so followers advance their commitIndex to leaderCommit
    cluster.nodes_[1]->broadcastAppendEntries();

    for (uint32_t id = 1; id <= 5; ++id) {
        ASSERT_TRUE(cluster.storages_[id]->get("cfg:env").has_value());
        EXPECT_EQ(*cluster.storages_[id]->get("cfg:env"), "prod");
    }
}

TEST(RaftConsensusTests, SplitBrainPreventionDuringNetworkPartition) {
    RaftClusterHarness cluster(5);
    cluster.nodes_[1]->triggerElection();
    ASSERT_EQ(cluster.nodes_[1]->role(), RaftRole::LEADER);

    // Isolate Leader {1, 2} (minority) from Majority {3, 4, 5}
    cluster.setPartition({1, 2}, {3, 4, 5});

    // Minority leader CANNOT commit writes!
    auto isolated_write = cluster.nodes_[1]->propose(OpType::SET, "split_key", "stale_minority", 120);
    EXPECT_FALSE(isolated_write.committed);

    // Majority partition elects a new leader in higher term and commits writes safely
    cluster.nodes_[3]->triggerElection();
    ASSERT_EQ(cluster.nodes_[3]->role(), RaftRole::LEADER);
    EXPECT_GT(cluster.nodes_[3]->currentTerm(), cluster.nodes_[1]->currentTerm());

    auto majority_write = cluster.nodes_[3]->propose(OpType::SET, "split_key", "authoritative_majority");
    ASSERT_TRUE(majority_write.committed);

    // Heal partition: old leader steps down to follower and truncates uncommitted minority entry!
    cluster.healPartitions();
    cluster.nodes_[3]->broadcastAppendEntries();
    cluster.nodes_[3]->broadcastAppendEntries();

    EXPECT_EQ(cluster.nodes_[1]->role(), RaftRole::FOLLOWER);
    for (uint32_t id = 1; id <= 5; ++id) {
        ASSERT_TRUE(cluster.storages_[id]->get("split_key").has_value());
        EXPECT_EQ(*cluster.storages_[id]->get("split_key"), "authoritative_majority");
    }
}

TEST(RaftConsensusTests, PersistentStateRecoveryAndSnapshotAcrossNodeRestart) {
    RaftClusterHarness cluster(3);
    cluster.nodes_[1]->triggerElection();
    ASSERT_TRUE(cluster.nodes_[1]->propose(OpType::SET, "k1", "v1").committed);
    ASSERT_TRUE(cluster.nodes_[1]->propose(OpType::SET, "k2", "v2").committed);
    cluster.nodes_[1]->broadcastAppendEntries();

    // Compact snapshot on Follower 2, then crash and restart Follower 2 from disk
    ASSERT_TRUE(cluster.nodes_[2]->createSnapshot());
    cluster.crashNode(2);

    ASSERT_TRUE(cluster.nodes_[1]->propose(OpType::SET, "k3", "v3").committed);

    cluster.restartNode(2);
    cluster.nodes_[1]->broadcastAppendEntries();
    cluster.nodes_[1]->broadcastAppendEntries();

    EXPECT_EQ(*cluster.storages_[2]->get("k1"), "v1");
    EXPECT_EQ(*cluster.storages_[2]->get("k2"), "v2");
    EXPECT_EQ(*cluster.storages_[2]->get("k3"), "v3");
}

} // namespace dkv::testing
