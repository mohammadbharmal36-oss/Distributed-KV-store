# Engineering Design Decisions & Trade-offs

## 1. Custom Binary Protocol vs Text Protocol (RESP)
- **Decision**: Implemented a length-prefixed binary protocol (`KV20`) with per-frame CRC32 checksums.
- **Why**: Length-prefixed binary strings handle arbitrary binary values (including `\r\n` and `\0` bytes) with zero delimiter-scanning overhead, prevent buffer overruns via a 64 MiB frame cap, and catch bit flips or malformed client frames deterministically.

## 2. WAL + Periodic Snapshots vs Full Map Serialization
- **Decision**: Append-only WAL (`O(mutation size)`) paired with periodic atomic snapshots (`snapshot.dat.tmp` + `fsync` + `rename`).
- **Why**: Serializing an entire `std::unordered_map` of 1M keys on every `SET` turns an `O(1)` operation into `O(N)` disk amplification (~60 MB per write). WAL appends only ~40–200 bytes sequentially, while periodic snapshots bound crash-recovery replay time.

## 3. Multi-Miss Heartbeat Window vs Single-Timeout Eviction
- **Decision**: `FailureDetector` uses `suspect_threshold = 3` and `dead_threshold = 6` consecutive missed heartbeats.
- **Why**: Linux loopback/LAN networks experience occasional OS scheduling stalls or TCP retransmissions. Treating 1 missed heartbeat as permanent death causes unnecessary primary failovers and replication churn.

## 4. Why Primary-Replica Replication Is Not Consensus
Simple primary-replica replication lacks **numbered terms**, **quorum leader election**, and **up-to-date log voting restrictions**. If a network partition isolates an old Primary from its replicas and an operator promotes a replica, both nodes accept divergent writes (**split-brain**). Our Phase 10–11 `RaftNode` eliminates this by requiring majority votes per term and enforcing §5.4.1 & §5.4.2 safety invariants.
