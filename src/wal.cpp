#include "wal.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <stdexcept>
#include <sys/stat.h>
#include <unistd.h>

namespace dkv {

namespace {
constexpr uint32_t SNAPSHOT_MAGIC = 0x534E4150; // "SNAP"
} // namespace

WriteAheadLog::WriteAheadLog(std::filesystem::path wal_path, bool sync_on_write)
    : path_(std::move(wal_path)), sync_on_write_(sync_on_write) {
    if (path_.has_parent_path()) {
        std::filesystem::create_directories(path_.parent_path());
    }
    openFile();
}

WriteAheadLog::~WriteAheadLog() {
    close();
}

void WriteAheadLog::openFile() {
    fd_ = ::open(path_.c_str(), O_CREAT | O_RDWR | O_APPEND, 0644);
    if (fd_ < 0) {
        throw std::runtime_error("Failed to open WAL file '" + path_.string() + "': " + std::strerror(errno));
    }
}

void WriteAheadLog::writeAllFd(int fd, const uint8_t* data, size_t len) {
    size_t written = 0;
    while (written < len) {
        ssize_t n = ::write(fd, data + written, len - written);
        if (n < 0) {
            if (errno == EINTR) continue;
            throw std::runtime_error(std::string("WAL write error: ") + std::strerror(errno));
        }
        written += static_cast<size_t>(n);
    }
}

void WriteAheadLog::fsyncFd(int fd) {
#if defined(__linux__)
    if (::fdatasync(fd) < 0) {
        throw std::runtime_error(std::string("WAL fdatasync failed: ") + std::strerror(errno));
    }
#else
    if (::fsync(fd) < 0) {
        throw std::runtime_error(std::string("WAL fsync failed: ") + std::strerror(errno));
    }
#endif
}

void WriteAheadLog::append(const LogRecord& rec) {
    std::lock_guard lock(mutex_);
    BinaryWriter payload_writer;
    rec.writeTo(payload_writer);
    const auto& payload = payload_writer.buffer();

    BinaryWriter frame_writer;
    frame_writer.writeU32(static_cast<uint32_t>(payload.size()));
    frame_writer.writeU32(computeCrc32(payload));
    auto header = frame_writer.takeBuffer();

    header.insert(header.end(), payload.begin(), payload.end());
    writeAllFd(fd_, header.data(), header.size());
    if (sync_on_write_) {
        fsyncFd(fd_);
    }
}

void WriteAheadLog::appendBatch(const std::vector<LogRecord>& records) {
    if (records.empty()) return;
    std::lock_guard lock(mutex_);
    std::vector<uint8_t> combined;
    for (const auto& rec : records) {
        BinaryWriter pw;
        rec.writeTo(pw);
        const auto& payload = pw.buffer();

        BinaryWriter fw;
        fw.writeU32(static_cast<uint32_t>(payload.size()));
        fw.writeU32(computeCrc32(payload));
        const auto& hdr = fw.buffer();

        combined.insert(combined.end(), hdr.begin(), hdr.end());
        combined.insert(combined.end(), payload.begin(), payload.end());
    }
    writeAllFd(fd_, combined.data(), combined.size());
    if (sync_on_write_) {
        fsyncFd(fd_);
    }
}

WalRecoveryStats WriteAheadLog::replay(const std::function<void(const LogRecord&)>& visitor) {
    std::lock_guard lock(mutex_);
    WalRecoveryStats stats{};

    struct stat st{};
    if (::fstat(fd_, &st) < 0) {
        throw std::runtime_error("fstat failed during WAL replay");
    }
    const auto file_size = static_cast<uint64_t>(st.st_size);
    if (file_size == 0) {
        return stats;
    }

    std::vector<uint8_t> raw(file_size);
    size_t total_read = 0;
    while (total_read < file_size) {
        ssize_t n = ::pread(fd_, raw.data() + total_read, file_size - total_read, static_cast<off_t>(total_read));
        if (n < 0) {
            if (errno == EINTR) continue;
            throw std::runtime_error("pread failed during WAL replay");
        }
        if (n == 0) break;
        total_read += static_cast<size_t>(n);
    }

    size_t offset = 0;
    while (offset < total_read) {
        if (offset + 8 > total_read) {
            // Incomplete 8-byte record header from crash mid-write
            stats.tail_corrupted_and_truncated = true;
            break;
        }
        BinaryReader hdr_reader(std::span<const uint8_t>(raw.data() + offset, 8));
        uint32_t len = hdr_reader.readU32();
        uint32_t expected_crc = hdr_reader.readU32();

        if (len > MAX_FRAME_PAYLOAD || offset + 8 + len > total_read) {
            stats.tail_corrupted_and_truncated = true;
            break;
        }

        std::span<const uint8_t> payload_span(raw.data() + offset + 8, len);
        if (computeCrc32(payload_span) != expected_crc) {
            stats.tail_corrupted_and_truncated = true;
            break;
        }

        try {
            BinaryReader rec_reader(payload_span);
            LogRecord rec = LogRecord::readFrom(rec_reader);
            visitor(rec);
            offset += 8 + len;
            ++stats.records_replayed;
        } catch (const ProtocolError&) {
            stats.tail_corrupted_and_truncated = true;
            break;
        }
    }

    stats.valid_bytes = offset;
    if (stats.tail_corrupted_and_truncated) {
        if (::ftruncate(fd_, static_cast<off_t>(offset)) < 0) {
            throw std::runtime_error("ftruncate failed while repairing WAL tail");
        }
        fsyncFd(fd_);
    }
    return stats;
}

void WriteAheadLog::truncateSuffixFrom(uint64_t first_removed_index) {
    std::vector<LogRecord> kept;
    replay([&](const LogRecord& rec) {
        if (rec.index < first_removed_index) {
            kept.push_back(rec);
        }
    });

    std::lock_guard lock(mutex_);
    auto tmp_path = path_;
    tmp_path += ".rewrite.tmp";
    int tmp_fd = ::open(tmp_path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (tmp_fd < 0) {
        throw std::runtime_error("Failed to create temporary WAL rewrite file");
    }

    for (const auto& rec : kept) {
        BinaryWriter pw;
        rec.writeTo(pw);
        const auto& payload = pw.buffer();
        BinaryWriter fw;
        fw.writeU32(static_cast<uint32_t>(payload.size()));
        fw.writeU32(computeCrc32(payload));
        writeAllFd(tmp_fd, fw.buffer().data(), fw.buffer().size());
        writeAllFd(tmp_fd, payload.data(), payload.size());
    }
    fsyncFd(tmp_fd);
    ::close(tmp_fd);

    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    std::filesystem::rename(tmp_path, path_);
    openFile();
}

void WriteAheadLog::saveSnapshotAtomic(const SnapshotData& snap, const std::filesystem::path& snap_path) {
    if (snap_path.has_parent_path()) {
        std::filesystem::create_directories(snap_path.parent_path());
    }
    BinaryWriter body;
    body.writeU64(snap.last_included_index);
    body.writeU64(snap.last_included_term);
    body.writeU32(static_cast<uint32_t>(snap.kv.size()));
    for (const auto& [k, v] : snap.kv) {
        body.writeString(k);
        body.writeString(v);
    }
    const auto& payload = body.buffer();

    BinaryWriter file_writer;
    file_writer.writeU32(SNAPSHOT_MAGIC);
    file_writer.writeU32(static_cast<uint32_t>(payload.size()));
    file_writer.writeU32(computeCrc32(payload));
    auto full = file_writer.takeBuffer();
    full.insert(full.end(), payload.begin(), payload.end());

    auto tmp_path = snap_path;
    tmp_path += ".tmp";
    int sfd = ::open(tmp_path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (sfd < 0) {
        throw std::runtime_error("Failed to open snapshot temp file");
    }
    writeAllFd(sfd, full.data(), full.size());
    fsyncFd(sfd);
    ::close(sfd);

    std::filesystem::rename(tmp_path, snap_path);
}

std::optional<SnapshotData> WriteAheadLog::loadSnapshot(const std::filesystem::path& snap_path) {
    if (!std::filesystem::exists(snap_path)) {
        return std::nullopt;
    }
    int sfd = ::open(snap_path.c_str(), O_RDONLY);
    if (sfd < 0) return std::nullopt;

    struct stat st{};
    if (::fstat(sfd, &st) < 0 || st.st_size < 12) {
        ::close(sfd);
        return std::nullopt;
    }

    std::vector<uint8_t> raw(static_cast<size_t>(st.st_size));
    size_t got = 0;
    while (got < raw.size()) {
        ssize_t n = ::read(sfd, raw.data() + got, raw.size() - got);
        if (n <= 0) break;
        got += static_cast<size_t>(n);
    }
    ::close(sfd);
    if (got < 12) return std::nullopt;

    BinaryReader hdr(std::span<const uint8_t>(raw.data(), 12));
    uint32_t magic = hdr.readU32();
    uint32_t len = hdr.readU32();
    uint32_t crc = hdr.readU32();
    if (magic != SNAPSHOT_MAGIC || 12ULL + len > got) {
        return std::nullopt;
    }
    std::span<const uint8_t> payload(raw.data() + 12, len);
    if (computeCrc32(payload) != crc) {
        return std::nullopt;
    }

    BinaryReader br(payload);
    SnapshotData snap;
    snap.last_included_index = br.readU64();
    snap.last_included_term = br.readU64();
    uint32_t count = br.readU32();
    snap.kv.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        std::string k = br.readString();
        std::string v = br.readString();
        snap.kv.emplace(std::move(k), std::move(v));
    }
    return snap;
}

void WriteAheadLog::compactWithSnapshot(const SnapshotData& snap,
                                        const std::filesystem::path& snap_path,
                                        const std::vector<LogRecord>& retained_tail) {
    saveSnapshotAtomic(snap, snap_path);

    std::lock_guard lock(mutex_);
    auto tmp_wal = path_;
    tmp_wal += ".compact.tmp";
    int tfd = ::open(tmp_wal.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (tfd < 0) {
        throw std::runtime_error("Failed to open compacted WAL temp file");
    }
    for (const auto& rec : retained_tail) {
        BinaryWriter pw;
        rec.writeTo(pw);
        const auto& payload = pw.buffer();
        BinaryWriter fw;
        fw.writeU32(static_cast<uint32_t>(payload.size()));
        fw.writeU32(computeCrc32(payload));
        writeAllFd(tfd, fw.buffer().data(), fw.buffer().size());
        writeAllFd(tfd, payload.data(), payload.size());
    }
    fsyncFd(tfd);
    ::close(tfd);

    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    std::filesystem::rename(tmp_wal, path_);
    openFile();
}

void WriteAheadLog::sync() {
    std::lock_guard lock(mutex_);
    if (fd_ >= 0) {
        fsyncFd(fd_);
    }
}

void WriteAheadLog::close() {
    std::lock_guard lock(mutex_);
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

} // namespace dkv
