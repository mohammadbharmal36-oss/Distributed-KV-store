#pragma once

#include "protocol.h"

#include <cstdint>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace dkv {

struct SnapshotData {
    uint64_t last_included_index{0};
    uint64_t last_included_term{0};
    std::unordered_map<std::string, std::string> kv;
};

/**
 * Thread-safe in-memory key-value storage engine.
 * Synchronization: Uses std::shared_mutex allowing concurrent lock-shared readers
 * (GET, EXISTS, SIZE) while serializing writers (SET, UPDATE, DELETE, Snapshot restore)
 * with std::unique_lock.
 */
class StorageEngine {
public:
    StorageEngine() = default;

    void set(const std::string& key, std::string value);
    [[nodiscard]] std::optional<std::string> get(const std::string& key) const;
    bool del(const std::string& key);
    [[nodiscard]] bool exists(const std::string& key) const;
    bool update(const std::string& key, std::string value);

    // Deterministic state-machine application used by WAL replay, replication, and Raft commit
    bool apply(OpType op, const std::string& key, const std::string& value);

    [[nodiscard]] size_t size() const;
    void clear();

    [[nodiscard]] SnapshotData takeSnapshot(uint64_t last_index, uint64_t last_term) const;
    void restoreSnapshot(SnapshotData snapshot);

    [[nodiscard]] uint64_t lastSnapshotIndex() const;
    [[nodiscard]] uint64_t lastSnapshotTerm() const;

private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, std::string> store_;
    uint64_t snapshot_index_{0};
    uint64_t snapshot_term_{0};
};

} // namespace dkv
