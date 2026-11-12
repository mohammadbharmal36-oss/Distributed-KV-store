#include "replication.h"

#include <chrono>

namespace dkv {

FailureDetector::FailureDetector(uint32_t suspect_threshold, uint32_t dead_threshold)
    : suspect_threshold_(suspect_threshold), dead_threshold_(dead_threshold) {}

void FailureDetector::registerPeer(uint32_t peer_id) {
    std::lock_guard lock(mutex_);
    peers_.try_emplace(peer_id, Entry{});
}

void FailureDetector::recordSuccess(uint32_t peer_id, double rtt_ms) {
    std::lock_guard lock(mutex_);
    auto& e = peers_[peer_id];
    e.missed = 0;
    e.srtt_ms = (e.srtt_ms == 0.0) ? rtt_ms : (0.8 * e.srtt_ms + 0.2 * rtt_ms);
    e.state = PeerHealth::HEALTHY;
}

void FailureDetector::recordMiss(uint32_t peer_id) {
    std::lock_guard lock(mutex_);
    auto& e = peers_[peer_id];
    ++e.missed;
    if (e.missed >= dead_threshold_) {
        e.state = PeerHealth::UNREACHABLE;
    } else if (e.missed >= suspect_threshold_) {
        e.state = PeerHealth::SUSPECT;
    } else {
        e.state = PeerHealth::HEALTHY;
    }
}

PeerHealth FailureDetector::healthOf(uint32_t peer_id) const {
    std::lock_guard lock(mutex_);
    auto it = peers_.find(peer_id);
    return (it == peers_.end()) ? PeerHealth::UNREACHABLE : it->second.state;
}

uint32_t FailureDetector::missedCount(uint32_t peer_id) const {
    std::lock_guard lock(mutex_);
    auto it = peers_.find(peer_id);
    return (it == peers_.end()) ? 0 : it->second.missed;
}

double FailureDetector::rttOf(uint32_t peer_id) const {
    std::lock_guard lock(mutex_);
    auto it = peers_.find(peer_id);
    return (it == peers_.end()) ? 0.0 : it->second.srtt_ms;
}

std::optional<Frame> ReplicationManager::defaultTcpTransport(const PeerEndpoint& peer,
                                                             MessageType type,
                                                             const std::vector<uint8_t>& payload) {
    try {
        TcpSocket sock = TcpSocket::connectTo(peer.host, peer.peer_port, 600);
        sock.sendFrame(type, payload);
        return sock.recvFrame();
    } catch (...) {
        return std::nullopt;
    }
}

ReplicationManager::ReplicationManager(uint32_t self_id,
                                       bool is_primary,
                                       SyncMode sync_mode,
                                       std::vector<PeerEndpoint> peers,
                                       StorageEngine& storage,
                                       WriteAheadLog& wal,
                                       RpcTransport transport)
    : self_id_(self_id),
      is_primary_(is_primary),
      sync_mode_(sync_mode),
      peers_(std::move(peers)),
      storage_(storage),
      wal_(wal),
      transport_(transport ? std::move(transport) : defaultTcpTransport) {
    for (const auto& p : peers_) {
        detector_.registerPeer(p.id);
        peer_acked_seq_[p.id] = 0;
    }
}

ReplicationManager::~ReplicationManager() {
    stop();
}

void ReplicationManager::start(int heartbeat_interval_ms) {
    if (running_.exchange(true)) return;
    bg_thread_ = std::thread([this, heartbeat_interval_ms] { backgroundLoop(heartbeat_interval_ms); });
}

void ReplicationManager::stop() {
    if (!running_.exchange(false)) return;
    ack_cv_.notify_all();
    if (bg_thread_.joinable()) {
        bg_thread_.join();
    }
}

bool ReplicationManager::replicateWrite(OpType op,
                                        const std::string& key,
                                        const std::string& value,
                                        int quorum_timeout_ms) {
    if (!is_primary_.load()) return false;

    uint64_t seq = 0;
    {
        std::lock_guard lock(mutex_);
        seq = next_seq_++;
        LogRecord rec{seq, 1, op, key, value};
        wal_.append(rec);
        history_.push_back(rec);

        if (sync_mode_ == SyncMode::ASYNC || peers_.empty()) {
            storage_.apply(op, key, value);
            applied_seq_ = seq;
        }
    }

    // Trigger immediate fanout to peers
    for (const auto& peer : peers_) {
        shipPendingToPeer(peer);
    }

    if (sync_mode_ == SyncMode::ASYNC || peers_.empty()) {
        return true;
    }

    const size_t total_nodes = peers_.size() + 1;
    const size_t required_acks =
        (sync_mode_ == SyncMode::SYNC_ALL) ? total_nodes : ((total_nodes / 2) + 1);

    std::unique_lock lock(mutex_);
    bool met = ack_cv_.wait_for(lock, std::chrono::milliseconds(quorum_timeout_ms), [&] {
        size_t count = 1; // Primary already persisted to local WAL
        for (const auto& p : peers_) {
            if (peer_acked_seq_[p.id] >= seq) {
                ++count;
            }
        }
        return count >= required_acks || !running_.load();
    });

    if (!met) {
        return false;
    }

    while (applied_seq_ < seq) {
        const auto& r = history_[applied_seq_];
        storage_.apply(r.op, r.key, r.value);
        ++applied_seq_;
    }
    return true;
}

void ReplicationManager::shipPendingToPeer(const PeerEndpoint& peer) {
    ReplAppendMsg msg;
    {
        std::lock_guard lock(mutex_);
        msg.primary_id = self_id_;
        msg.commit_seq = applied_seq_;
        uint64_t acked = peer_acked_seq_[peer.id];
        for (const auto& rec : history_) {
            if (rec.index > acked) {
                msg.entries.push_back(rec);
            }
        }
    }
    if (msg.entries.empty()) return;

    const auto t0 = std::chrono::steady_clock::now();
    auto maybe_reply = transport_(peer, MessageType::REPL_APPEND, msg.serialize());
    const auto t1 = std::chrono::steady_clock::now();
    double rtt_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    if (!maybe_reply.has_value()) {
        detector_.recordMiss(peer.id);
        return;
    }
    try {
        auto ack = ReplAckMsg::deserialize(maybe_reply->payload);
        detector_.recordSuccess(peer.id, rtt_ms);
        if (ack.success) {
            {
                std::lock_guard lock(mutex_);
                if (ack.acked_seq > peer_acked_seq_[peer.id]) {
                    peer_acked_seq_[peer.id] = ack.acked_seq;
                }
            }
            ack_cv_.notify_all();
        }
    } catch (...) {
        detector_.recordMiss(peer.id);
    }
}

ReplAckMsg ReplicationManager::onReplicaAppend(const ReplAppendMsg& msg) {
    std::lock_guard lock(mutex_);
    primary_missed_ticks_ = 0;
    for (const auto& entry : msg.entries) {
        if (entry.index == applied_seq_ + 1) {
            wal_.append(entry);
            storage_.apply(entry.op, entry.key, entry.value);
            history_.push_back(entry);
            applied_seq_ = entry.index;
            if (next_seq_ <= applied_seq_) next_seq_ = applied_seq_ + 1;
        }
    }
    return ReplAckMsg{self_id_, applied_seq_, true};
}

HeartbeatMsg ReplicationManager::onHeartbeatPing(const HeartbeatMsg& ping) {
    if (ping.is_primary) {
        std::lock_guard lock(mutex_);
        primary_missed_ticks_ = 0;
    }
    return HeartbeatMsg{self_id_, ping.term, ping.timestamp_ms, is_primary_.load()};
}

bool ReplicationManager::primarySuspectedDead() const {
    std::lock_guard lock(mutex_);
    return !is_primary_.load() && primary_missed_ticks_ >= 6;
}

uint64_t ReplicationManager::localLastSeq() const {
    std::lock_guard lock(mutex_);
    return applied_seq_;
}

std::vector<PeerStatus> ReplicationManager::peerStatuses() const {
    std::lock_guard lock(mutex_);
    std::vector<PeerStatus> out;
    out.reserve(peers_.size());
    for (const auto& p : peers_) {
        PeerStatus st;
        st.id = p.id;
        st.health = detector_.healthOf(p.id);
        auto it = peer_acked_seq_.find(p.id);
        st.acked_seq = (it != peer_acked_seq_.end()) ? it->second : 0;
        st.missed_heartbeats = detector_.missedCount(p.id);
        st.smoothed_rtt_ms = detector_.rttOf(p.id);
        out.push_back(st);
    }
    return out;
}

void ReplicationManager::backgroundLoop(int interval_ms) {
    while (running_.load()) {
        if (is_primary_.load()) {
            auto now_ms = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now().time_since_epoch())
                    .count());
            HeartbeatMsg hb{self_id_, 1, now_ms, true};
            auto hb_bytes = hb.serialize();

            for (const auto& peer : peers_) {
                const auto t0 = std::chrono::steady_clock::now();
                auto reply = transport_(peer, MessageType::HEARTBEAT_PING, hb_bytes);
                const auto t1 = std::chrono::steady_clock::now();
                double rtt = std::chrono::duration<double, std::milli>(t1 - t0).count();
                if (reply.has_value()) {
                    detector_.recordSuccess(peer.id, rtt);
                    shipPendingToPeer(peer); // Automatically catch up recovered replica
                } else {
                    detector_.recordMiss(peer.id);
                }
            }
        } else {
            std::lock_guard lock(mutex_);
            ++primary_missed_ticks_;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
    }
}

} // namespace dkv
