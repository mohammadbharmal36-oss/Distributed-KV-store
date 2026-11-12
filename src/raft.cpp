#include "raft.h"

#include <algorithm>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace dkv {

namespace {
constexpr uint32_t RAFT_META_MAGIC = 0x524D4554; // "RMET"
} // namespace

std::optional<Frame> RaftNode::defaultTcpRpc(const PeerEndpoint& peer,
                                             MessageType type,
                                             const std::vector<uint8_t>& payload) {
    try {
        TcpSocket sock = TcpSocket::connectTo(peer.host, peer.peer_port, 250);
        sock.sendFrame(type, payload);
        return sock.recvFrame();
    } catch (...) {
        return std::nullopt;
    }
}

RaftNode::RaftNode(uint32_t node_id,
                   std::vector<PeerEndpoint> peers,
                   std::filesystem::path data_dir,
                   StorageEngine& storage,
                   RpcFunc rpc_sender)
    : id_(node_id),
      peers_(std::move(peers)),
      data_dir_(std::move(data_dir)),
      meta_path_(data_dir_ / "raft_meta.dat"),
      snap_path_(data_dir_ / "snapshot.dat"),
      storage_(storage),
      wal_(data_dir_ / "raft_wal.log", true),
      rpc_sender_(rpc_sender ? std::move(rpc_sender) : defaultTcpRpc),
      rng_(std::random_device{}() ^ (node_id * 0x9E3779B9U)) {
    std::filesystem::create_directories(data_dir_);
    loadPersistentState();
    std::lock_guard lock(mutex_);
    resetElectionDeadlineLocked();
}

RaftNode::~RaftNode() {
    stop();
}

void RaftNode::loadPersistentState() {
    std::lock_guard lock(mutex_);
    // 1. Load Raft term and votedFor metadata
    if (std::filesystem::exists(meta_path_)) {
        int fd = ::open(meta_path_.c_str(), O_RDONLY);
        if (fd >= 0) {
            std::vector<uint8_t> buf(25);
            ssize_t n = ::read(fd, buf.data(), buf.size());
            ::close(fd);
            if (n == 25) {
                BinaryReader r(buf);
                uint32_t magic = r.readU32();
                uint64_t term = r.readU64();
                bool has_vote = r.readBool();
                uint32_t voted = r.readU32();
                uint64_t crc_stored = r.readU64();
                uint32_t calc = computeCrc32(std::span<const uint8_t>(buf.data(), 17));
                if (magic == RAFT_META_MAGIC && calc == static_cast<uint32_t>(crc_stored)) {
                    current_term_ = term;
                    voted_for_ = has_vote ? std::optional<uint32_t>(voted) : std::nullopt;
                }
            }
        }
    }

    // 2. Load latest compacted snapshot if available
    if (auto snap = WriteAheadLog::loadSnapshot(snap_path_)) {
        base_snap_index_ = snap->last_included_index;
        base_snap_term_ = snap->last_included_term;
        commit_index_ = base_snap_index_;
        last_applied_ = base_snap_index_;
        storage_.restoreSnapshot(std::move(*snap));
    }

    // 3. Replay WAL tail beyond base_snap_index_
    log_.clear();
    wal_.replay([&](const LogRecord& rec) {
        if (rec.index > base_snap_index_) {
            log_.push_back(rec);
        }
    });
}

void RaftNode::persistMetadataLocked() {
    BinaryWriter w;
    w.writeU32(RAFT_META_MAGIC);
    w.writeU64(current_term_);
    w.writeBool(voted_for_.has_value());
    w.writeU32(voted_for_.value_or(0));
    uint32_t crc = computeCrc32(w.buffer());
    w.writeU64(static_cast<uint64_t>(crc));

    auto tmp = meta_path_;
    tmp += ".tmp";
    int fd = ::open(tmp.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd >= 0) {
        const auto& b = w.buffer();
        size_t off = 0;
        while (off < b.size()) {
            ssize_t n = ::write(fd, b.data() + off, b.size() - off);
            if (n <= 0) break;
            off += static_cast<size_t>(n);
        }
        ::fsync(fd);
        ::close(fd);
        std::filesystem::rename(tmp, meta_path_);
    }
}

uint64_t RaftNode::lastLogIndexLocked() const {
    if (log_.empty()) return base_snap_index_;
    return log_.back().index;
}

uint64_t RaftNode::lastLogTermLocked() const {
    if (log_.empty()) return base_snap_term_;
    return log_.back().term;
}

uint64_t RaftNode::termAtLocked(uint64_t index) const {
    if (index == 0) return 0;
    if (index == base_snap_index_) return base_snap_term_;
    if (index < base_snap_index_) return 0;
    size_t pos = static_cast<size_t>(index - base_snap_index_ - 1);
    if (pos >= log_.size()) return 0;
    return log_[pos].term;
}

const LogRecord* RaftNode::recordAtLocked(uint64_t index) const {
    if (index <= base_snap_index_) return nullptr;
    size_t pos = static_cast<size_t>(index - base_snap_index_ - 1);
    if (pos >= log_.size()) return nullptr;
    return &log_[pos];
}

void RaftNode::resetElectionDeadlineLocked() {
    std::uniform_int_distribution<int> dist(160, 320);
    election_deadline_ = std::chrono::steady_clock::now() + std::chrono::milliseconds(dist(rng_));
}

void RaftNode::becomeFollowerLocked(uint64_t new_term, uint32_t leader_hint) {
    bool changed = (new_term != current_term_);
    role_ = RaftRole::FOLLOWER;
    if (new_term > current_term_) {
        current_term_ = new_term;
        voted_for_ = std::nullopt;
    }
    if (leader_hint != 0) {
        leader_id_ = leader_hint;
    }
    if (changed) {
        persistMetadataLocked();
    }
    resetElectionDeadlineLocked();
    commit_cv_.notify_all();
}

void RaftNode::start() {
    if (running_.exchange(true)) return;
    {
        std::lock_guard lock(mutex_);
        resetElectionDeadlineLocked();
    }
    ticker_thread_ = std::thread([this] { tickerLoop(); });
}

void RaftNode::stop() {
    if (!running_.exchange(false)) return;
    commit_cv_.notify_all();
    if (ticker_thread_.joinable()) {
        ticker_thread_.join();
    }
}

void RaftNode::tickerLoop() {
    while (running_.load()) {
        bool do_election = false;
        bool do_heartbeat = false;
        {
            std::lock_guard lock(mutex_);
            const auto now = std::chrono::steady_clock::now();
            if (role_ == RaftRole::LEADER) {
                if (now - last_heartbeat_sent_ >= std::chrono::milliseconds(55)) {
                    do_heartbeat = true;
                    last_heartbeat_sent_ = now;
                }
            } else if (now >= election_deadline_) {
                do_election = true;
            }
        }
        if (do_election) {
            triggerElection();
        } else if (do_heartbeat) {
            broadcastAppendEntries();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(15));
    }
}

void RaftNode::triggerElection() {
    RequestVoteArgs args{};
    std::vector<PeerEndpoint> peers_copy;
    uint64_t election_term = 0;

    {
        std::lock_guard lock(mutex_);
        role_ = RaftRole::CANDIDATE;
        ++current_term_;
        voted_for_ = id_;
        leader_id_ = 0;
        persistMetadataLocked();
        resetElectionDeadlineLocked();

        election_term = current_term_;
        args.term = current_term_;
        args.candidate_id = id_;
        args.last_log_index = lastLogIndexLocked();
        args.last_log_term = lastLogTermLocked();
        peers_copy = peers_;

        // Single-node cluster wins immediately
        if (peers_copy.empty()) {
            role_ = RaftRole::LEADER;
            leader_id_ = id_;
            last_heartbeat_sent_ = std::chrono::steady_clock::now();
            return;
        }
    }

    auto payload = args.serialize();
    std::atomic<size_t> votes{1}; // Self vote
    const size_t quorum = ((peers_copy.size() + 1) / 2) + 1;

    std::vector<std::thread> rpc_threads;
    rpc_threads.reserve(peers_copy.size());
    for (const auto& peer : peers_copy) {
        rpc_threads.emplace_back([&, peer] {
            auto maybe_frame = rpc_sender_(peer, MessageType::RAFT_REQUEST_VOTE, payload);
            if (!maybe_frame.has_value()) return;
            try {
                auto reply = RequestVoteReply::deserialize(maybe_frame->payload);
                std::lock_guard lock(mutex_);
                if (reply.term > current_term_) {
                    becomeFollowerLocked(reply.term);
                    return;
                }
                if (role_ != RaftRole::CANDIDATE || current_term_ != election_term) {
                    return;
                }
                if (reply.vote_granted) {
                    size_t total = votes.fetch_add(1) + 1;
                    if (total >= quorum && role_ == RaftRole::CANDIDATE) {
                        role_ = RaftRole::LEADER;
                        leader_id_ = id_;
                        uint64_t next_idx = lastLogIndexLocked() + 1;
                        for (const auto& p : peers_) {
                            next_index_[p.id] = next_idx;
                            match_index_[p.id] = 0;
                        }
                        last_heartbeat_sent_ = std::chrono::steady_clock::now();
                    }
                }
            } catch (...) {}
        });
    }
    for (auto& t : rpc_threads) {
        if (t.joinable()) t.join();
    }

    if (role() == RaftRole::LEADER) {
        broadcastAppendEntries();
    }
}

RequestVoteReply RaftNode::handleRequestVote(const RequestVoteArgs& args) {
    std::lock_guard lock(mutex_);
    RequestVoteReply reply{current_term_, false};

    if (args.term < current_term_) {
        return reply;
    }
    if (args.term > current_term_) {
        becomeFollowerLocked(args.term);
    }
    reply.term = current_term_;

    // §5.4.1 Election Restriction: Candidate log must be at least as up-to-date as ours
    const uint64_t my_last_term = lastLogTermLocked();
    const uint64_t my_last_idx = lastLogIndexLocked();
    const bool log_up_to_date =
        (args.last_log_term > my_last_term) ||
        (args.last_log_term == my_last_term && args.last_log_index >= my_last_idx);

    const bool can_vote = (!voted_for_.has_value() || *voted_for_ == args.candidate_id);
    if (can_vote && log_up_to_date) {
        voted_for_ = args.candidate_id;
        persistMetadataLocked();
        resetElectionDeadlineLocked();
        reply.vote_granted = true;
    }
    return reply;
}

void RaftNode::broadcastAppendEntries() {
    std::vector<PeerEndpoint> peers_copy;
    {
        std::lock_guard lock(mutex_);
        if (role_ != RaftRole::LEADER) return;
        peers_copy = peers_;
    }

    std::vector<std::thread> threads;
    threads.reserve(peers_copy.size());
    for (const auto& peer : peers_copy) {
        threads.emplace_back([this, peer] {
            AppendEntriesArgs args{};
            uint64_t sent_term = 0;
            {
                std::lock_guard lock(mutex_);
                if (role_ != RaftRole::LEADER) return;
                uint64_t nxt = next_index_[peer.id];
                if (nxt <= base_snap_index_) {
                    nxt = base_snap_index_ + 1;
                }
                args.term = current_term_;
                sent_term = current_term_;
                args.leader_id = id_;
                args.prev_log_index = nxt - 1;
                args.prev_log_term = termAtLocked(args.prev_log_index);
                args.leader_commit = commit_index_;

                uint64_t last_idx = lastLogIndexLocked();
                for (uint64_t i = nxt; i <= last_idx; ++i) {
                    if (const auto* rec = recordAtLocked(i)) {
                        args.entries.push_back(*rec);
                    }
                }
            }

            auto maybe_frame = rpc_sender_(peer, MessageType::RAFT_APPEND_ENTRIES, args.serialize());
            if (!maybe_frame.has_value()) return;
            try {
                auto rep = AppendEntriesReply::deserialize(maybe_frame->payload);
                std::lock_guard lock(mutex_);
                if (rep.term > current_term_) {
                    becomeFollowerLocked(rep.term);
                    return;
                }
                if (role_ != RaftRole::LEADER || current_term_ != sent_term) {
                    return;
                }
                if (rep.success) {
                    if (rep.match_index > match_index_[peer.id]) {
                        match_index_[peer.id] = rep.match_index;
                    }
                    next_index_[peer.id] = match_index_[peer.id] + 1;
                    advanceCommitIndexLocked();
                } else {
                    // Fast log backtracking using conflict_index
                    uint64_t fallback = (rep.conflict_index > 0) ? rep.conflict_index : 1;
                    next_index_[peer.id] = std::max<uint64_t>(base_snap_index_ + 1, fallback);
                }
            } catch (...) {}
        });
    }
    for (auto& t : threads) {
        if (t.joinable()) t.join();
    }
}

AppendEntriesReply RaftNode::handleAppendEntries(const AppendEntriesArgs& args) {
    std::lock_guard lock(mutex_);
    AppendEntriesReply rep{current_term_, false, 0, 0, 0};

    if (args.term < current_term_) {
        return rep;
    }
    if (args.term > current_term_ || role_ != RaftRole::FOLLOWER) {
        becomeFollowerLocked(args.term, args.leader_id);
    } else {
        leader_id_ = args.leader_id;
        resetElectionDeadlineLocked();
    }
    rep.term = current_term_;

    // §5.3 Log Matching Check
    if (args.prev_log_index > lastLogIndexLocked()) {
        rep.conflict_index = lastLogIndexLocked() + 1;
        return rep;
    }
    if (args.prev_log_index > base_snap_index_) {
        uint64_t local_prev_term = termAtLocked(args.prev_log_index);
        if (local_prev_term != args.prev_log_term) {
            rep.conflict_term = local_prev_term;
            uint64_t idx = args.prev_log_index;
            while (idx > base_snap_index_ + 1 && termAtLocked(idx - 1) == local_prev_term) {
                --idx;
            }
            rep.conflict_index = idx;
            return rep;
        }
    }

    // Reconcile new entries & truncate conflicting uncommitted suffix if present
    std::vector<LogRecord> to_persist;
    for (const auto& entry : args.entries) {
        if (entry.index <= base_snap_index_) continue;
        uint64_t existing_term = termAtLocked(entry.index);
        if (existing_term != 0 && existing_term != entry.term) {
            size_t keep_len = static_cast<size_t>(entry.index - base_snap_index_ - 1);
            log_.resize(keep_len);
            wal_.truncateSuffixFrom(entry.index);
            existing_term = 0;
        }
        if (existing_term == 0) {
            log_.push_back(entry);
            to_persist.push_back(entry);
        }
    }
    if (!to_persist.empty()) {
        wal_.appendBatch(to_persist);
    }

    if (args.leader_commit > commit_index_) {
        commit_index_ = std::min(args.leader_commit, lastLogIndexLocked());
        applyCommittedLocked();
    }

    rep.success = true;
    rep.match_index = args.prev_log_index + args.entries.size();
    return rep;
}

void RaftNode::advanceCommitIndexLocked() {
    const uint64_t last_idx = lastLogIndexLocked();
    const size_t total_nodes = peers_.size() + 1;
    const size_t quorum = (total_nodes / 2) + 1;

    for (uint64_t n = last_idx; n > commit_index_; --n) {
        // §5.4.2 Safety: Only commit entries from the leader's current term by counting replicas
        if (termAtLocked(n) != current_term_) {
            continue;
        }
        size_t replicated = 1; // Leader itself
        for (const auto& p : peers_) {
            if (match_index_[p.id] >= n) {
                ++replicated;
            }
        }
        if (replicated >= quorum) {
            commit_index_ = n;
            applyCommittedLocked();
            break;
        }
    }
}

void RaftNode::applyCommittedLocked() {
    while (last_applied_ < commit_index_) {
        ++last_applied_;
        if (const auto* rec = recordAtLocked(last_applied_)) {
            storage_.apply(rec->op, rec->key, rec->value);
        }
    }
    commit_cv_.notify_all();
}

ProposeResult RaftNode::propose(OpType op, const std::string& key, const std::string& value, int timeout_ms) {
    ProposeResult res{};
    uint64_t entry_index = 0;
    uint64_t entry_term = 0;

    {
        std::lock_guard lock(mutex_);
        res.leader_id = leader_id_;
        if (role_ != RaftRole::LEADER) {
            return res;
        }
        entry_index = lastLogIndexLocked() + 1;
        entry_term = current_term_;
        LogRecord rec{entry_index, entry_term, op, key, value};
        wal_.append(rec);
        log_.push_back(rec);

        res.accepted = true;
        res.index = entry_index;
        res.term = entry_term;

        if (peers_.empty()) {
            commit_index_ = entry_index;
            applyCommittedLocked();
            res.committed = true;
            return res;
        }
    }

    broadcastAppendEntries();

    std::unique_lock lock(mutex_);
    bool ok = commit_cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms), [&] {
        return last_applied_ >= entry_index || role_ != RaftRole::LEADER || current_term_ != entry_term;
    });
    res.leader_id = leader_id_;
    if (ok && last_applied_ >= entry_index && termAtLocked(entry_index) == entry_term) {
        res.committed = true;
    }
    return res;
}

bool RaftNode::createSnapshot() {
    std::lock_guard lock(mutex_);
    if (last_applied_ <= base_snap_index_) return false;

    uint64_t snap_idx = last_applied_;
    uint64_t snap_term = termAtLocked(snap_idx);
    auto snap = storage_.takeSnapshot(snap_idx, snap_term);

    std::vector<LogRecord> tail;
    for (const auto& rec : log_) {
        if (rec.index > snap_idx) {
            tail.push_back(rec);
        }
    }
    wal_.compactWithSnapshot(snap, snap_path_, tail);
    storage_.restoreSnapshot(std::move(snap));
    log_ = std::move(tail);
    base_snap_index_ = snap_idx;
    base_snap_term_ = snap_term;
    return true;
}

RaftRole RaftNode::role() const {
    std::lock_guard lock(mutex_);
    return role_;
}

uint64_t RaftNode::currentTerm() const {
    std::lock_guard lock(mutex_);
    return current_term_;
}

uint64_t RaftNode::commitIndex() const {
    std::lock_guard lock(mutex_);
    return commit_index_;
}

uint64_t RaftNode::lastApplied() const {
    std::lock_guard lock(mutex_);
    return last_applied_;
}

uint32_t RaftNode::currentLeaderId() const {
    std::lock_guard lock(mutex_);
    return leader_id_;
}

size_t RaftNode::logSize() const {
    std::lock_guard lock(mutex_);
    return log_.size();
}

} // namespace dkv
