#include "storage.h"
#include "wal.h"

#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>

namespace dkv::testing {

class WalTestFixture : public ::testing::Test {
protected:
    std::filesystem::path dir_;

    void SetUp() override {
        dir_ = std::filesystem::temp_directory_path() /
               ("dkv_wal_test_" + std::to_string(::testing::UnitTest::GetInstance()->random_seed()) +
                "_" + std::to_string(reinterpret_cast<uintptr_t>(this)));
        std::filesystem::remove_all(dir_);
        std::filesystem::create_directories(dir_);
    }

    void TearDown() override {
        std::filesystem::remove_all(dir_);
    }
};

TEST_F(WalTestFixture, CrashRecoveryReplaysCommittedMutations) {
    auto wal_file = dir_ / "wal.log";
    {
        WriteAheadLog wal(wal_file, true);
        wal.append(LogRecord{1, 1, OpType::SET, "user:1", "Ada"});
        wal.append(LogRecord{2, 1, OpType::SET, "user:2", "Grace"});
        wal.append(LogRecord{3, 1, OpType::UPDATE, "user:1", "Ada_Lovelace"});
        wal.append(LogRecord{4, 1, OpType::DELETE_OP, "user:2", ""});
    }

    StorageEngine recovered;
    WriteAheadLog wal2(wal_file, true);
    auto stats = wal2.replay([&](const LogRecord& rec) {
        recovered.apply(rec.op, rec.key, rec.value);
    });

    EXPECT_EQ(stats.records_replayed, 4U);
    EXPECT_FALSE(stats.tail_corrupted_and_truncated);
    ASSERT_TRUE(recovered.get("user:1").has_value());
    EXPECT_EQ(*recovered.get("user:1"), "Ada_Lovelace");
    EXPECT_FALSE(recovered.exists("user:2"));
}

TEST_F(WalTestFixture, SafelyTruncatesIncompleteOrCorruptedTailRecord) {
    auto wal_file = dir_ / "corrupt.log";
    {
        WriteAheadLog wal(wal_file, true);
        wal.append(LogRecord{1, 1, OpType::SET, "safe_k1", "val1"});
        wal.append(LogRecord{2, 1, OpType::SET, "safe_k2", "val2"});
    }

    // Simulate power loss mid-record by appending garbage / partial record bytes
    {
        std::ofstream out(wal_file, std::ios::binary | std::ios::app);
        const char partial_bytes[] = "\x00\x00\x00\x25\xDE\xAD\xBE\xEF\x01\x02";
        out.write(partial_bytes, sizeof(partial_bytes) - 1);
    }

    StorageEngine recovered;
    WriteAheadLog wal_recover(wal_file, true);
    auto stats = wal_recover.replay([&](const LogRecord& rec) {
        recovered.apply(rec.op, rec.key, rec.value);
    });

    EXPECT_EQ(stats.records_replayed, 2U);
    EXPECT_TRUE(stats.tail_corrupted_and_truncated);
    EXPECT_EQ(*recovered.get("safe_k1"), "val1");
    EXPECT_EQ(*recovered.get("safe_k2"), "val2");

    // Subsequent appends after auto-repair must succeed cleanly
    wal_recover.append(LogRecord{3, 1, OpType::SET, "after_repair", "ok"});
    StorageEngine check2;
    auto stats2 = wal_recover.replay([&](const LogRecord& r) { check2.apply(r.op, r.key, r.value); });
    EXPECT_EQ(stats2.records_replayed, 3U);
    EXPECT_FALSE(stats2.tail_corrupted_and_truncated);
    EXPECT_EQ(*check2.get("after_repair"), "ok");
}

TEST_F(WalTestFixture, AtomicSnapshotCompactsWalAndSpeedsRecovery) {
    auto wal_file = dir_ / "wal.log";
    auto snap_file = dir_ / "snapshot.dat";

    StorageEngine live;
    WriteAheadLog wal(wal_file, true);
    for (uint64_t i = 1; i <= 50; ++i) {
        std::string k = "k" + std::to_string(i);
        std::string v = "v" + std::to_string(i);
        wal.append(LogRecord{i, 1, OpType::SET, k, v});
        live.set(k, v);
    }

    auto snap = live.takeSnapshot(50, 1);
    wal.compactWithSnapshot(snap, snap_file);
    wal.append(LogRecord{51, 1, OpType::SET, "post_snap", "fresh"});
    wal.close();

    StorageEngine restarted;
    auto loaded_snap = WriteAheadLog::loadSnapshot(snap_file);
    ASSERT_TRUE(loaded_snap.has_value());
    restarted.restoreSnapshot(std::move(*loaded_snap));

    WriteAheadLog wal_reopen(wal_file, true);
    auto stats = wal_reopen.replay([&](const LogRecord& r) {
        restarted.apply(r.op, r.key, r.value);
    });

    EXPECT_EQ(stats.records_replayed, 1U); // Only record #51 needed replay!
    EXPECT_EQ(restarted.size(), 51U);
    EXPECT_EQ(*restarted.get("k25"), "v25");
    EXPECT_EQ(*restarted.get("post_snap"), "fresh");
}

} // namespace dkv::testing
