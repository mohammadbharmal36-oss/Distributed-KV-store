#include "storage.h"

#include <gtest/gtest.h>
#include <atomic>
#include <string>
#include <thread>
#include <vector>

namespace dkv::testing {

TEST(StorageEngineTests, EmptyDatabaseBehaviors) {
    StorageEngine db;
    EXPECT_EQ(db.size(), 0U);
    EXPECT_FALSE(db.get("missing").has_value());
    EXPECT_FALSE(db.exists("missing"));
    EXPECT_FALSE(db.del("missing"));
    EXPECT_FALSE(db.update("missing", "val"));
}

TEST(StorageEngineTests, SetGetUpdateDeleteLifecycle) {
    StorageEngine db;
    db.set("alpha", "v1");
    EXPECT_TRUE(db.exists("alpha"));
    ASSERT_TRUE(db.get("alpha").has_value());
    EXPECT_EQ(*db.get("alpha"), "v1");

    // Duplicate SET overwrites cleanly
    db.set("alpha", "v2");
    EXPECT_EQ(db.size(), 1U);
    EXPECT_EQ(*db.get("alpha"), "v2");

    // UPDATE modifies existing key
    EXPECT_TRUE(db.update("alpha", "v3"));
    EXPECT_EQ(*db.get("alpha"), "v3");

    // DELETE removes key
    EXPECT_TRUE(db.del("alpha"));
    EXPECT_FALSE(db.exists("alpha"));
    EXPECT_FALSE(db.del("alpha"));
}

TEST(StorageEngineTests, HandlesLargeValues) {
    StorageEngine db;
    std::string two_mb(2 * 1024 * 1024, 'X');
    two_mb.front() = 'A';
    two_mb.back() = 'Z';

    db.set("blob:large", two_mb);
    auto fetched = db.get("blob:large");
    ASSERT_TRUE(fetched.has_value());
    EXPECT_EQ(fetched->size(), two_mb.size());
    EXPECT_EQ(fetched->front(), 'A');
    EXPECT_EQ(fetched->back(), 'Z');
}

TEST(StorageEngineTests, ConcurrentSameKeyReadWriteSafety) {
    StorageEngine db;
    db.set("hot_key", "init");

    constexpr int kWriters = 8;
    constexpr int kReaders = 8;
    constexpr int kOps = 2000;

    std::vector<std::thread> threads;
    for (int w = 0; w < kWriters; ++w) {
        threads.emplace_back([&, w] {
            for (int i = 0; i < kOps; ++i) {
                std::string val = "w" + std::to_string(w) + "_" + std::to_string(i);
                if (i % 2 == 0) {
                    db.set("hot_key", val);
                } else {
                    db.update("hot_key", val);
                }
            }
        });
    }
    for (int r = 0; r < kReaders; ++r) {
        threads.emplace_back([&] {
            for (int i = 0; i < kOps; ++i) {
                auto v = db.get("hot_key");
                EXPECT_TRUE(v.has_value());
                EXPECT_FALSE(v->empty());
            }
        });
    }
    for (auto& t : threads) t.join();
    EXPECT_TRUE(db.exists("hot_key"));
}

TEST(StorageEngineTests, SnapshotTakeAndRestore) {
    StorageEngine db1;
    db1.set("k1", "100");
    db1.set("k2", "200");
    auto snap = db1.takeSnapshot(42, 3);

    StorageEngine db2;
    db2.set("stale", "should_disappear");
    db2.restoreSnapshot(snap);

    EXPECT_EQ(db2.lastSnapshotIndex(), 42U);
    EXPECT_EQ(db2.lastSnapshotTerm(), 3U);
    EXPECT_FALSE(db2.exists("stale"));
    EXPECT_EQ(*db2.get("k1"), "100");
    EXPECT_EQ(*db2.get("k2"), "200");
}

} // namespace dkv::testing
