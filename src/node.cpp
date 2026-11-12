#include "node.h"

#include <sstream>

namespace dkv {

ClusterNode::ClusterNode(NodeConfig cfg) : cfg_(std::move(cfg)) {
    std::filesystem::create_directories(cfg_.data_dir);

    if (cfg_.mode == ClusterMode::STANDALONE || cfg_.mode == ClusterMode::PRIMARY_REPLICA) {
        auto snap_file = cfg_.data_dir / "snapshot.dat";
        if (auto snap = WriteAheadLog::loadSnapshot(snap_file)) {
            standalone_seq_ = snap->last_included_index;
            storage_.restoreSnapshot(std::move(*snap));
        }
        standalone_wal_ = std::make_unique<WriteAheadLog>(cfg_.data_dir / "wal.log", true);
        standalone_wal_->replay([&](const LogRecord& rec) {
            if (rec.index > standalone_seq_) {
                storage_.apply(rec.op, rec.key, rec.value);
                standalone_seq_ = rec.index;
            }
        });
    }

    if (cfg_.mode == ClusterMode::PRIMARY_REPLICA) {
        repl_ = std::make_unique<ReplicationManager>(
            cfg_.id, cfg_.is_primary, cfg_.sync_mode, cfg_.peers, storage_, *standalone_wal_);
    } else if (cfg_.mode == ClusterMode::RAFT) {
        raft_ = std::make_unique<RaftNode>(cfg_.id, cfg_.peers, cfg_.data_dir, storage_);
    }

    ServerConfig client_scfg{cfg_.host, cfg_.client_port, cfg_.worker_threads, false, 30000};
    client_server_ = std::make_unique<KvServer>(
        client_scfg, [this](const Frame& f) { return handleClientFrame(f); });

    if (cfg_.mode != ClusterMode::STANDALONE) {
        ServerConfig peer_scfg{cfg_.host, cfg_.peer_port, 4, false, 5000};
        peer_server_ = std::make_unique<KvServer>(
            peer_scfg, [this](const Frame& f) { return handlePeerFrame(f); });
    }
}

ClusterNode::~ClusterNode() {
    stop();
}

void ClusterNode::start() {
    if (peer_server_) peer_server_->start();
    if (repl_) repl_->start();
    if (raft_) raft_->start();
    client_server_->start();
}

void ClusterNode::stop() {
    if (client_server_) client_server_->stop();
    if (raft_) raft_->stop();
    if (repl_) repl_->stop();
    if (peer_server_) peer_server_->stop();
    if (standalone_wal_) standalone_wal_->close();
}

uint16_t ClusterNode::clientPort() const noexcept {
    return client_server_ ? client_server_->boundPort() : 0;
}

uint16_t ClusterNode::peerPort() const noexcept {
    return peer_server_ ? peer_server_->boundPort() : 0;
}

std::string ClusterNode::lookupLeaderEndpoint(uint32_t leader_id) const {
    if (leader_id == cfg_.id) {
        return cfg_.host + ":" + std::to_string(clientPort());
    }
    for (const auto& p : cfg_.peers) {
        if (p.id == leader_id) {
            return p.host + ":" + std::to_string(p.client_port);
        }
    }
    return "";
}

Frame ClusterNode::handleClientFrame(const Frame& frame) {
    if (frame.type != MessageType::CLIENT_CMD) {
        throw ProtocolError("Expected CLIENT_CMD frame on client port");
    }
    auto req = ClientRequest::deserialize(frame.payload);
    auto resp = executeClientRequest(req);
    return Frame{MessageType::SERVER_RESP, 0, resp.serialize()};
}

ServerResponse ClusterNode::executeClientRequest(const ClientRequest& req) {
    ServerResponse resp{};

    // Read-only / Diagnostic Commands
    if (req.cmd == ClientCmdType::PING) {
        resp.status = StatusCode::OK;
        resp.message = "PONG";
        return resp;
    }
    if (req.cmd == ClientCmdType::GET) {
        auto val = storage_.get(req.key);
        if (!val.has_value()) {
            resp.status = StatusCode::NOT_FOUND;
            resp.message = "Key not found";
        } else {
            resp.status = StatusCode::OK;
            resp.value = *val;
        }
        return resp;
    }
    if (req.cmd == ClientCmdType::EXISTS) {
        resp.status = storage_.exists(req.key) ? StatusCode::EXISTS_TRUE : StatusCode::EXISTS_FALSE;
        return resp;
    }
    if (req.cmd == ClientCmdType::STATS) {
        auto m = client_server_->metrics();
        std::ostringstream oss;
        oss << "node_id=" << cfg_.id << " keys=" << storage_.size()
            << " reqs=" << m.total_requests << " avg_us=" << m.avg_us
            << " p95_us=" << m.p95_us << " p99_us=" << m.p99_us;
        if (raft_) {
            oss << " raft_term=" << raft_->currentTerm()
                << " commit_idx=" << raft_->commitIndex()
                << " role=" << (raft_->role() == RaftRole::LEADER ? "LEADER" : "FOLLOWER");
        }
        resp.status = StatusCode::OK;
        resp.message = oss.str();
        return resp;
    }
    if (req.cmd == ClientCmdType::SNAPSHOT) {
        if (raft_) {
            raft_->createSnapshot();
        } else if (standalone_wal_) {
            std::lock_guard lock(standalone_mutex_);
            auto snap = storage_.takeSnapshot(standalone_seq_, 1);
            standalone_wal_->compactWithSnapshot(snap, cfg_.data_dir / "snapshot.dat");
        }
        resp.status = StatusCode::OK;
        resp.message = "Snapshot persisted and WAL compacted";
        return resp;
    }

    // Mutating Commands (SET, UPDATE, DELETE)
    OpType op = OpType::SET;
    if (req.cmd == ClientCmdType::DELETE_CMD) op = OpType::DELETE_OP;
    else if (req.cmd == ClientCmdType::UPDATE) op = OpType::UPDATE;

    if (cfg_.mode == ClusterMode::STANDALONE) {
        std::lock_guard lock(standalone_mutex_);
        if (op == OpType::UPDATE && !storage_.exists(req.key)) {
            resp.status = StatusCode::NOT_FOUND;
            resp.message = "Cannot UPDATE nonexistent key";
            return resp;
        }
        if (op == OpType::DELETE_OP && !storage_.exists(req.key)) {
            resp.status = StatusCode::NOT_FOUND;
            resp.message = "Key not found";
            return resp;
        }
        uint64_t seq = ++standalone_seq_;
        standalone_wal_->append(LogRecord{seq, 1, op, req.key, req.value});
        storage_.apply(op, req.key, req.value);
        resp.status = StatusCode::OK;
        resp.message = "OK";
        return resp;
    }

    if (cfg_.mode == ClusterMode::PRIMARY_REPLICA) {
        if (!repl_->isPrimary()) {
            resp.status = StatusCode::NOT_LEADER;
            resp.message = "Node is a read-only replica";
            if (!cfg_.peers.empty()) {
                resp.leader_hint = cfg_.peers.front().host + ":" + std::to_string(cfg_.peers.front().client_port);
            }
            return resp;
        }
        if ((op == OpType::UPDATE || op == OpType::DELETE_OP) && !storage_.exists(req.key)) {
            resp.status = StatusCode::NOT_FOUND;
            resp.message = "Key not found";
            return resp;
        }
        bool ok = repl_->replicateWrite(op, req.key, req.value);
        resp.status = ok ? StatusCode::OK : StatusCode::QUORUM_TIMEOUT;
        resp.message = ok ? "OK" : "Replication quorum timed out";
        return resp;
    }

    // ClusterMode::RAFT
    if (raft_->role() != RaftRole::LEADER) {
        resp.status = StatusCode::NOT_LEADER;
        resp.message = "Redirect to Raft Leader";
        resp.leader_hint = lookupLeaderEndpoint(raft_->currentLeaderId());
        return resp;
    }
    if ((op == OpType::UPDATE || op == OpType::DELETE_OP) && !storage_.exists(req.key)) {
        resp.status = StatusCode::NOT_FOUND;
        resp.message = "Key not found";
        return resp;
    }
    auto pres = raft_->propose(op, req.key, req.value);
    if (!pres.accepted) {
        resp.status = StatusCode::NOT_LEADER;
        resp.leader_hint = lookupLeaderEndpoint(pres.leader_id);
        return resp;
    }
    if (!pres.committed) {
        resp.status = StatusCode::QUORUM_TIMEOUT;
        resp.message = "Raft majority commit timed out";
        return resp;
    }
    resp.status = StatusCode::OK;
    resp.message = "OK";
    return resp;
}

Frame ClusterNode::handlePeerFrame(const Frame& frame) {
    switch (frame.type) {
        case MessageType::REPL_APPEND: {
            auto msg = ReplAppendMsg::deserialize(frame.payload);
            auto ack = repl_->onReplicaAppend(msg);
            return Frame{MessageType::REPL_ACK, 0, ack.serialize()};
        }
        case MessageType::HEARTBEAT_PING: {
            auto ping = HeartbeatMsg::deserialize(frame.payload);
            auto pong = repl_->onHeartbeatPing(ping);
            return Frame{MessageType::HEARTBEAT_PONG, 0, pong.serialize()};
        }
        case MessageType::RAFT_REQUEST_VOTE: {
            auto args = RequestVoteArgs::deserialize(frame.payload);
            auto rep = raft_->handleRequestVote(args);
            return Frame{MessageType::RAFT_REQUEST_VOTE_RESP, 0, rep.serialize()};
        }
        case MessageType::RAFT_APPEND_ENTRIES: {
            auto args = AppendEntriesArgs::deserialize(frame.payload);
            auto rep = raft_->handleAppendEntries(args);
            return Frame{MessageType::RAFT_APPEND_ENTRIES_RESP, 0, rep.serialize()};
        }
        default:
            throw ProtocolError("Unexpected peer RPC message type");
    }
}

} // namespace dkv
