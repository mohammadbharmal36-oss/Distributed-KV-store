#include "protocol.h"

#include <array>
#include <cstring>

namespace dkv {

namespace {
constexpr std::array<uint32_t, 256> makeCrcTable() noexcept {
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int k = 0; k < 8; ++k) {
            c = (c & 1U) ? (0xEDB88320U ^ (c >> 1U)) : (c >> 1U);
        }
        table[i] = c;
    }
    return table;
}

constexpr auto CRC32_TABLE = makeCrcTable();
} // namespace

uint32_t computeCrc32(std::span<const uint8_t> data) noexcept {
    uint32_t crc = 0xFFFFFFFFU;
    for (uint8_t byte : data) {
        crc = CRC32_TABLE[(crc ^ byte) & 0xFFU] ^ (crc >> 8U);
    }
    return crc ^ 0xFFFFFFFFU;
}

void BinaryWriter::writeU8(uint8_t v) { buf_.push_back(v); }
void BinaryWriter::writeBool(bool v) { buf_.push_back(v ? 1U : 0U); }

void BinaryWriter::writeU16(uint16_t v) {
    buf_.push_back(static_cast<uint8_t>((v >> 8U) & 0xFFU));
    buf_.push_back(static_cast<uint8_t>(v & 0xFFU));
}

void BinaryWriter::writeU32(uint32_t v) {
    for (int shift = 24; shift >= 0; shift -= 8) {
        buf_.push_back(static_cast<uint8_t>((v >> shift) & 0xFFU));
    }
}

void BinaryWriter::writeU64(uint64_t v) {
    for (int shift = 56; shift >= 0; shift -= 8) {
        buf_.push_back(static_cast<uint8_t>((v >> shift) & 0xFFU));
    }
}

void BinaryWriter::writeString(const std::string& s) {
    if (s.size() > MAX_FRAME_PAYLOAD) {
        throw ProtocolError("String exceeds maximum frame payload size");
    }
    writeU32(static_cast<uint32_t>(s.size()));
    buf_.insert(buf_.end(), s.begin(), s.end());
}

void BinaryReader::require(size_t bytes) const {
    if (pos_ + bytes > data_.size()) {
        throw ProtocolError("Truncated binary payload: unexpected end of stream");
    }
}

uint8_t BinaryReader::readU8() {
    require(1);
    return data_[pos_++];
}

bool BinaryReader::readBool() {
    uint8_t v = readU8();
    if (v > 1U) {
        throw ProtocolError("Invalid boolean byte in binary frame");
    }
    return v == 1U;
}

uint16_t BinaryReader::readU16() {
    require(2);
    uint16_t v = (static_cast<uint16_t>(data_[pos_]) << 8U) |
                 static_cast<uint16_t>(data_[pos_ + 1]);
    pos_ += 2;
    return v;
}

uint32_t BinaryReader::readU32() {
    require(4);
    uint32_t v = 0;
    for (size_t i = 0; i < 4; ++i) {
        v = (v << 8U) | static_cast<uint32_t>(data_[pos_ + i]);
    }
    pos_ += 4;
    return v;
}

uint64_t BinaryReader::readU64() {
    require(8);
    uint64_t v = 0;
    for (size_t i = 0; i < 8; ++i) {
        v = (v << 8U) | static_cast<uint64_t>(data_[pos_ + i]);
    }
    pos_ += 8;
    return v;
}

std::string BinaryReader::readString() {
    uint32_t len = readU32();
    require(len);
    std::string out(reinterpret_cast<const char*>(data_.data() + pos_), len);
    pos_ += len;
    return out;
}

std::vector<uint8_t> ClientRequest::serialize() const {
    BinaryWriter w;
    w.writeU8(static_cast<uint8_t>(cmd));
    w.writeString(key);
    w.writeString(value);
    return w.takeBuffer();
}

ClientRequest ClientRequest::deserialize(std::span<const uint8_t> data) {
    BinaryReader r(data);
    ClientRequest req;
    req.cmd = static_cast<ClientCmdType>(r.readU8());
    req.key = r.readString();
    req.value = r.readString();
    return req;
}

std::vector<uint8_t> ServerResponse::serialize() const {
    BinaryWriter w;
    w.writeU8(static_cast<uint8_t>(status));
    w.writeString(value);
    w.writeString(message);
    w.writeString(leader_hint);
    return w.takeBuffer();
}

ServerResponse ServerResponse::deserialize(std::span<const uint8_t> data) {
    BinaryReader r(data);
    ServerResponse resp;
    resp.status = static_cast<StatusCode>(r.readU8());
    resp.value = r.readString();
    resp.message = r.readString();
    resp.leader_hint = r.readString();
    return resp;
}

void LogRecord::writeTo(BinaryWriter& w) const {
    w.writeU64(index);
    w.writeU64(term);
    w.writeU8(static_cast<uint8_t>(op));
    w.writeString(key);
    w.writeString(value);
}

LogRecord LogRecord::readFrom(BinaryReader& r) {
    LogRecord rec;
    rec.index = r.readU64();
    rec.term = r.readU64();
    rec.op = static_cast<OpType>(r.readU8());
    rec.key = r.readString();
    rec.value = r.readString();
    return rec;
}

std::vector<uint8_t> ReplAppendMsg::serialize() const {
    BinaryWriter w;
    w.writeU32(primary_id);
    w.writeU64(commit_seq);
    w.writeU32(static_cast<uint32_t>(entries.size()));
    for (const auto& e : entries) {
        e.writeTo(w);
    }
    return w.takeBuffer();
}

ReplAppendMsg ReplAppendMsg::deserialize(std::span<const uint8_t> data) {
    BinaryReader r(data);
    ReplAppendMsg msg;
    msg.primary_id = r.readU32();
    msg.commit_seq = r.readU64();
    uint32_t count = r.readU32();
    msg.entries.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        msg.entries.push_back(LogRecord::readFrom(r));
    }
    return msg;
}

std::vector<uint8_t> ReplAckMsg::serialize() const {
    BinaryWriter w;
    w.writeU32(replica_id);
    w.writeU64(acked_seq);
    w.writeBool(success);
    return w.takeBuffer();
}

ReplAckMsg ReplAckMsg::deserialize(std::span<const uint8_t> data) {
    BinaryReader r(data);
    ReplAckMsg msg;
    msg.replica_id = r.readU32();
    msg.acked_seq = r.readU64();
    msg.success = r.readBool();
    return msg;
}

std::vector<uint8_t> HeartbeatMsg::serialize() const {
    BinaryWriter w;
    w.writeU32(sender_id);
    w.writeU64(term);
    w.writeU64(timestamp_ms);
    w.writeBool(is_primary);
    return w.takeBuffer();
}

HeartbeatMsg HeartbeatMsg::deserialize(std::span<const uint8_t> data) {
    BinaryReader r(data);
    HeartbeatMsg msg;
    msg.sender_id = r.readU32();
    msg.term = r.readU64();
    msg.timestamp_ms = r.readU64();
    msg.is_primary = r.readBool();
    return msg;
}

std::vector<uint8_t> RequestVoteArgs::serialize() const {
    BinaryWriter w;
    w.writeU64(term);
    w.writeU32(candidate_id);
    w.writeU64(last_log_index);
    w.writeU64(last_log_term);
    return w.takeBuffer();
}

RequestVoteArgs RequestVoteArgs::deserialize(std::span<const uint8_t> data) {
    BinaryReader r(data);
    RequestVoteArgs a;
    a.term = r.readU64();
    a.candidate_id = r.readU32();
    a.last_log_index = r.readU64();
    a.last_log_term = r.readU64();
    return a;
}

std::vector<uint8_t> RequestVoteReply::serialize() const {
    BinaryWriter w;
    w.writeU64(term);
    w.writeBool(vote_granted);
    return w.takeBuffer();
}

RequestVoteReply RequestVoteReply::deserialize(std::span<const uint8_t> data) {
    BinaryReader r(data);
    RequestVoteReply rep;
    rep.term = r.readU64();
    rep.vote_granted = r.readBool();
    return rep;
}

std::vector<uint8_t> AppendEntriesArgs::serialize() const {
    BinaryWriter w;
    w.writeU64(term);
    w.writeU32(leader_id);
    w.writeU64(prev_log_index);
    w.writeU64(prev_log_term);
    w.writeU32(static_cast<uint32_t>(entries.size()));
    for (const auto& e : entries) {
        e.writeTo(w);
    }
    w.writeU64(leader_commit);
    return w.takeBuffer();
}

AppendEntriesArgs AppendEntriesArgs::deserialize(std::span<const uint8_t> data) {
    BinaryReader r(data);
    AppendEntriesArgs a;
    a.term = r.readU64();
    a.leader_id = r.readU32();
    a.prev_log_index = r.readU64();
    a.prev_log_term = r.readU64();
    uint32_t count = r.readU32();
    a.entries.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        a.entries.push_back(LogRecord::readFrom(r));
    }
    a.leader_commit = r.readU64();
    return a;
}

std::vector<uint8_t> AppendEntriesReply::serialize() const {
    BinaryWriter w;
    w.writeU64(term);
    w.writeBool(success);
    w.writeU64(match_index);
    w.writeU64(conflict_index);
    w.writeU64(conflict_term);
    return w.takeBuffer();
}

AppendEntriesReply AppendEntriesReply::deserialize(std::span<const uint8_t> data) {
    BinaryReader r(data);
    AppendEntriesReply rep;
    rep.term = r.readU64();
    rep.success = r.readBool();
    rep.match_index = r.readU64();
    rep.conflict_index = r.readU64();
    rep.conflict_term = r.readU64();
    return rep;
}

std::vector<uint8_t> encodeFrame(MessageType type, std::span<const uint8_t> payload, uint16_t flags) {
    if (payload.size() > MAX_FRAME_PAYLOAD) {
        throw ProtocolError("Frame payload exceeds maximum permissible size");
    }
    BinaryWriter w;
    w.writeU32(PROTOCOL_MAGIC);
    w.writeU8(PROTOCOL_VERSION);
    w.writeU8(static_cast<uint8_t>(type));
    w.writeU16(flags);
    w.writeU32(static_cast<uint32_t>(payload.size()));
    w.writeU32(computeCrc32(payload));
    auto out = w.takeBuffer();
    out.insert(out.end(), payload.begin(), payload.end());
    return out;
}

FrameHeader decodeHeader(std::span<const uint8_t, FRAME_HEADER_SIZE> raw) {
    BinaryReader r(raw);
    FrameHeader hdr;
    hdr.magic = r.readU32();
    if (hdr.magic != PROTOCOL_MAGIC) {
        throw ProtocolError("Invalid protocol magic header");
    }
    hdr.version = r.readU8();
    if (hdr.version != PROTOCOL_VERSION) {
        throw ProtocolError("Unsupported protocol version");
    }
    hdr.type = static_cast<MessageType>(r.readU8());
    hdr.flags = r.readU16();
    hdr.payload_len = r.readU32();
    if (hdr.payload_len > MAX_FRAME_PAYLOAD) {
        throw ProtocolError("Frame payload length exceeds safety limit");
    }
    hdr.crc32 = r.readU32();
    return hdr;
}

} // namespace dkv
