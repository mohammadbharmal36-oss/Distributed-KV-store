#include "replication.h"

#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <thread>

namespace dkv::testing {

TEST(FailureDetectorTests, DoesNotDeclareDeadOnSingleMissedHeartbeat) {
    FailureDetector fd(3, 6);
    fd.registerPeer(2);

    EXPECT_EQ(fd.healthOf(2), PeerHealth::HEALTHY);

    // 1 or 2 missed heartbeats: still HEALTHY
    fd.recordMiss(2);
    EXPECT_EQ(fd.healthOf(2), PeerHealth::HEALTHY);
    fd.recordMiss(2);
    EXPECT_EQ(fd.healthOf(2), PeerHealth::HEALTHY);

    // 3rd miss transitions to SUSPECT, still not UNREACHABLE
    fd.recordMiss(2);
    EXPECT_EQ(fd.healthOf(2), PeerHealth::SUSPECT);

    // 6th consecutive miss transitions to UNREACHABLE
    fd.recordMiss(2);
    fd.recordMiss(2);
    fd.recordMiss(2);
    EXPECT_EQ(fd.healthOf(2), PeerHealth::UNREACHABLE);

    // Recovery upon heartbeat response restores HEALTHY immediately
    fd.recordSuccess(2, 0.8);
    EXPECT_EQ(fd.healthOf(2), PeerHealth::HEALTHY);
    EXPECT_EQ(fd.missedCount(2), 0U);
}

TEST(ReplicationTests, SyncMajorityReplicatesAndRecoversDisconnectedReplica) {
    auto base = std::filesystem::temp_directory_path() / "dkv_repl_unit";
    std::filesystem::remove_all(base);

    StorageEngine s1, s2, s3;
    WriteAheadLog w1(base / "n1/wal.log", false);
    WriteAheadLog w2(base / "n2/wal.log", false);
    WriteAheadLog w3(base / "n3/wal.log", false);

    std::atomic<bool> node3_offline{false};

    std::unique_ptr<ReplicationManager> r2, r3;
    auto router = [&](const PeerEndpoint& peer,
                      MessageType type,
                      const std::vector<uint8_t>& payload) -> std::optional<Frame> {
        if (peer.id == 3 && node3_offline.load()) {
            return std::nullopt; // Simulate network disconnection to Replica 3
        }
        ReplicationManager* target = (peer.id == 2) ? r2.get() : r3.get();
        if (type == MessageType::REPL_APPEND) {
            auto ack = target->onReplicaAppend(ReplAppendMsg::deserialize(payload));
            return Frame{MessageType::REPL_ACK, 0, ack.serialize()};
        }
        if (type == MessageType::HEARTBEAT_PING) {
            auto pong = target->onHeartbeatPing(HeartbeatMsg::deserialize(payload));
            return Frame{MessageType::HEARTBEAT_PONG, 0, pong.serialize()};
        }
        return std::nullopt;
    };

    r2 = std::make_unique<ReplicationManager>(2, false, SyncMode::SYNC_MAJORITY, std::vector<PeerEndpoint>{}, s2, w2);
    r3 = std::make_unique<ReplicationManager>(3, false, SyncMode::SYNC_MAJORITY, std::vector<PeerEndpoint>{}, s3, w3);

    std::vector<PeerEndpoint> peers{{2, "127.0.0.1", 0, 0}, {3, "127.0.0.1", 0, 0}};
    ReplicationManager primary(1, true, SyncMode::SYNC_MAJORITY, peers, s1, w1, router);

    // 1. Healthy cluster write replicates to all nodes
    ASSERT_TRUE(primary.replicateWrite(OpType::SET, "acct:1", "1000"));
    EXPECT_EQ(*s2.get("acct:1"), "1000");
    EXPECT_EQ(*s3.get("acct:1"), "1000");

    // 2. Disconnect Node 3: SYNC_MAJORITY (2/3) still succeeds via Primary + Node 2
    node3_offline.store(true);
    ASSERT_TRUE(primary.replicateWrite(OpType::SET, "acct:2", "2000"));
    EXPECT_EQ(*s2.get("acct:2"), "2000");
    EXPECT_FALSE(s3.exists("acct:2"));

    // 3. Reconnect Node 3 and start heartbeat recovery loop -> Node 3 catches up automatically
    node3_offline.store(false);
    primary.start(30);
    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    primary.stop();

    ASSERT_TRUE(s3.get("acct:2").has_value());
    EXPECT_EQ(*s3.get("acct:2"), "2000");

    std::filesystem::remove_all(base);
}

} // namespace dkv::testing
