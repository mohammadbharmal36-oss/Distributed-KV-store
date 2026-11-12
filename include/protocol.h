#pragma once

#include <cstdint>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

namespace dkv {

constexpr uint32_t PROTOCOL_MAGIC = 0x4B563230; // ASCII "KV20"
constexpr uint8_t PROTOCOL_VERSION = 1;
constexpr size_t FRAME_HEADER_SIZE = 16;
constexpr uint32_t MAX_FRAME_PAYLOAD = 64U * 1024U * 1024U; // 64 MiB safety cap

class ProtocolError : public std::runtime_error {
public:
    explicit ProtocolError(const std::string& msg) : std::runtime_error(msg) {}
};

enum class OpType : uint8_t {
    SET = 1,
    DELETE_OP = 2,
    UPDATE = 3,
    NOOP = 4
};

enum class MessageType : uint8_t {
    CLIENT_CMD = 1,
    SERVER_RESP = 2,
    REPL_APPEND = 10,
    REPL_ACK = 11,
    HEARTBEAT_PING = 20,
    HEARTBEAT_PONG = 21,
    RAFT_REQUEST_VOTE = 30,
    RAFT_REQUEST_VOTE_RESP = 31,
    RAFT_APPEND_ENTRIES = 32,
    RAFT_APPEND_ENTRIES_RESP = 33
};

enum class ClientCmdType : uint8_t {
    SET = 1,
    GET = 2,
    DELETE_CMD = 3,
    EXISTS = 4,
    UPDATE = 5,
    SNAPSHOT = 6,
    STATS = 7,
    PING = 8
};

enum class StatusCode : uint8_t {
    OK = 0,
    NOT_FOUND = 1,
    EXISTS_TRUE = 2,
    EXISTS_FALSE = 3,
    NOT_LEADER = 4,
    QUORUM_TIMEOUT = 5,
    BAD_REQUEST = 6,
    INTERNAL_ERROR = 7
};

struct FrameHeader {
    uint32_t magic{PROTOCOL_MAGIC};
    uint8_t version{PROTOCOL_VERSION};
    MessageType type{MessageType::CLIENT_CMD};
    uint16_t flags{0};
    uint32_t payload_len{0};
    uint32_t crc32{0};
};

struct Frame {
    MessageType type{MessageType::CLIENT_CMD};
    uint16_t flags{0};
    std::vector<uint8_t> payload;
};

uint32_t computeCrc32(std::span<const uint8_t> data) noexcept;

class BinaryWriter {
public:
    void writeU8(uint8_t v);
    void writeBool(bool v);
    void writeU16(uint16_t v);
    void writeU32(uint32_t v);
    void writeU64(uint64_t v);
    void writeString(const std::string& s);
    [[nodiscard]] const std::vector<uint8_t>& buffer() const noexcept { return buf_; }
    std::vector<uint8_t> takeBuffer() noexcept { return std::move(buf_); }

private:
    std::vector<uint8_t> buf_;
};

class BinaryReader {
public:
    explicit BinaryReader(std::span<const uint8_t> data) : data_(data) {}
    uint8_t readU8();
    bool readBool();
    uint16_t readU16();
    uint32_t readU32();
    uint64_t readU64();
    std::string readString();
    [[nodiscard]] size_t remaining() const noexcept { return data_.size() - pos_; }

private:
    void require(size_t bytes) const;
    std::span<const uint8_t> data_;
    size_t pos_{0};
};

struct ClientRequest {
    ClientCmdType cmd{ClientCmdType::PING};
    std::string key;
    std::string value;

    [[nodiscard]] std::vector<uint8_t> serialize() const;
    static ClientRequest deserialize(std::span<const uint8_t> data);
};

struct ServerResponse {
    StatusCode status{StatusCode::OK};
    std::string value;
    std::string message;
    std::string leader_hint;

    [[nodiscard]] std::vector<uint8_t> serialize() const;
    static ServerResponse deserialize(std::span<const uint8_t> data);
};

struct LogRecord {
    uint64_t index{0};
    uint64_t term{0};
    OpType op{OpType::SET};
    std::string key;
    std::string value;

    void writeTo(BinaryWriter& w) const;
    static LogRecord readFrom(BinaryReader& r);
};

struct ReplAppendMsg {
    uint32_t primary_id{0};
    uint64_t commit_seq{0};
    std::vector<LogRecord> entries;

    [[nodiscard]] std::vector<uint8_t> serialize() const;
    static ReplAppendMsg deserialize(std::span<const uint8_t> data);
};

struct ReplAckMsg {
    uint32_t replica_id{0};
    uint64_t acked_seq{0};
    bool success{false};

    [[nodiscard]] std::vector<uint8_t> serialize() const;
    static ReplAckMsg deserialize(std::span<const uint8_t> data);
};

struct HeartbeatMsg {
    uint32_t sender_id{0};
    uint64_t term{0};
    uint64_t timestamp_ms{0};
    bool is_primary{false};

    [[nodiscard]] std::vector<uint8_t> serialize() const;
    static HeartbeatMsg deserialize(std::span<const uint8_t> data);
};

struct RequestVoteArgs {
    uint64_t term{0};
    uint32_t candidate_id{0};
    uint64_t last_log_index{0};
    uint64_t last_log_term{0};

    [[nodiscard]] std::vector<uint8_t> serialize() const;
    static RequestVoteArgs deserialize(std::span<const uint8_t> data);
};

struct RequestVoteReply {
    uint64_t term{0};
    bool vote_granted{false};

    [[nodiscard]] std::vector<uint8_t> serialize() const;
    static RequestVoteReply deserialize(std::span<const uint8_t> data);
};

struct AppendEntriesArgs {
    uint64_t term{0};
    uint32_t leader_id{0};
    uint64_t prev_log_index{0};
    uint64_t prev_log_term{0};
    std::vector<LogRecord> entries;
    uint64_t leader_commit{0};

    [[nodiscard]] std::vector<uint8_t> serialize() const;
    static AppendEntriesArgs deserialize(std::span<const uint8_t> data);
};

struct AppendEntriesReply {
    uint64_t term{0};
    bool success{false};
    uint64_t match_index{0};
    uint64_t conflict_index{0};
    uint64_t conflict_term{0};

    [[nodiscard]] std::vector<uint8_t> serialize() const;
    static AppendEntriesReply deserialize(std::span<const uint8_t> data);
};

std::vector<uint8_t> encodeFrame(MessageType type, std::span<const uint8_t> payload, uint16_t flags = 0);
FrameHeader decodeHeader(std::span<const uint8_t, FRAME_HEADER_SIZE> raw);

} // namespace dkv
