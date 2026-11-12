#pragma once

#include "protocol.h"
#include "storage.h"

#include <cstdint>
#include <filesystem>
#include <functional>
#include <mutex>
#include <optional>
#include <vector>

namespace dkv {

struct WalRecoveryStats {
    size_t records_replayed{0};
    bool tail_corrupted_and_truncated{false};
    uint64_t valid_bytes{0};
};

/**
 * Append-only Write-Ahead Log with per-record CRC32 validation and atomic snapshots.
 * Record Layout on Disk:
 *   [PayloadLen : 4B BE][CRC32 : 4B BE][LogRecord Binary Payload : PayloadLen Bytes]
 */
class WriteAheadLog {
public:
    explicit WriteAheadLog(std::filesystem::path wal_path, bool sync_on_write = true);
    ~WriteAheadLog();

    WriteAheadLog(const WriteAheadLog&) = delete;
    WriteAheadLog& operator=(const WriteAheadLog&) = delete;

    void append(const LogRecord& rec);
    void appendBatch(const std::vector<LogRecord>& records);

    WalRecoveryStats replay(const std::function<void(const LogRecord&)>& visitor);

    // Truncates uncommitted conflicting entries >= first_removed_index (used by Raft §5.3)
    void truncateSuffixFrom(uint64_t first_removed_index);

    // Atomically writes snapshot and compacts WAL up to snap.last_included_index
    void compactWithSnapshot(const SnapshotData& snap,
                             const std::filesystem::path& snap_path,
                             const std::vector<LogRecord>& retained_tail = {});

    static void saveSnapshotAtomic(const SnapshotData& snap, const std::filesystem::path& snap_path);
    static std::optional<SnapshotData> loadSnapshot(const std::filesystem::path& snap_path);

    void sync();
    void close();

private:
    void openFile();
    static void writeAllFd(int fd, const uint8_t* data, size_t len);
    static void fsyncFd(int fd);

    std::filesystem::path path_;
    bool sync_on_write_{true};
    int fd_{-1};
    mutable std::mutex mutex_;
};

} // namespace dkv
