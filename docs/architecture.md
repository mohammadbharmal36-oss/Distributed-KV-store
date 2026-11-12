# System Architecture & Synchronization Specification

## 1. Separation of Concerns

`distributed-kv-store` enforces strict module boundaries so that storage, durability, wire framing, networking, and distributed consensus evolve and test independently:

1. **`StorageEngine` (`storage.h/.cpp`)**: Pure in-memory state machine (`std::unordered_map<std::string, std::string>`). Knows nothing about sockets, files, or consensus.
2. **`WriteAheadLog` (`wal.h/.cpp`)**:Sequential durable log and atomic snapshot engine using POSIX file descriptors.
3. **`Protocol` (`protocol.h/.cpp`)**: Deterministic big-endian binary codecs and IEEE 802.3 CRC32 checksums.
4. **`Network` (`network.h/.cpp`)**: RAII `TcpSocket`, `TcpListener`, and `ThreadPool`. Converts raw TCP byte streams into validated `Frame` objects.
5. **`ReplicationManager` (`replication.h/.cpp`)**: Primary-replica log shipping (`ASYNC`, `SYNC_MAJORITY`, `SYNC_ALL`) + multi-miss `FailureDetector`.
6. **`RaftNode` (`raft.h/.cpp`)**: State-machine replication consensus engine implementing leader election, log reconciliation, and persistent term/vote metadata.
7. **`ClusterNode` (`node.h/.cpp`)**: Top-level composition root binding client & peer `KvServer` listeners to the configured coordination mode.

---

## 2. Concurrency & Lock Hierarchy (Deadlock Prevention)

To eliminate deadlocks across concurrent client workers, heartbeat threads, and Raft RPC handlers, locks strictly obey a **directed acyclic lock ordering**:

```text
Level 1 (Outer):  ClusterNode::standalone_mutex_  OR  ReplicationManager::mutex_  OR  RaftNode::mutex_
        |
        v
Level 2 (Inner):  WriteAheadLog::mutex_   (Disk append / fdatasync / replay)
        |
        v
Level 3 (Leaf):   StorageEngine::mutex_   (std::shared_mutex — never acquires any external lock)
```

### Critical Concurrency Rules
- **No Network I/O Under Consensus Locks**: In both `RaftNode::triggerElection()` and `RaftNode::broadcastAppendEntries()`, RPC request arguments are snapshotted under `RaftNode::mutex_`, the mutex is **released** prior to invoking `rpc_sender_()` over TCP sockets, and `RaftNode::mutex_` is re-acquired upon reply arrival while re-validating `(role_ == LEADER && current_term_ == sent_term)`.
- **Reader/Writer Parallelism**: `StorageEngine::get()`, `exists()`, `size()`, and `takeSnapshot()` acquire `std::shared_lock<std::shared_mutex>`, allowing dozens of concurrent `GET` client threads to execute without blocking one another.
