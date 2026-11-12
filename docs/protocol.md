# Binary Wire Protocol (`KV20`) & Disk Format Specification

## 1. Why Application Framing Is Required Over TCP

TCP (`SOCK_STREAM`) is a **byte-stream** transport, not a message transport:
- A single `send()` of 120 bytes may arrive across three `recv()` calls of 10, 50, and 60 bytes (**fragmentation**).
- Back-to-back `send()` calls may arrive in a single `recv()` buffer (**coalescing**).

`TcpSocket::recvFrame()` solves both by calling `recvExact(16)` for the fixed header, validating magic/version/length bounds, and calling `recvExact(payload_len)` for the exact payload slice.

---

## 2. Wire Frame Layout (Big-Endian / Network Byte Order)

```text
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                     Magic = 0x4B563230 ("KV20")               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|  Version (1)  |  MsgType (1B) |          Flags (2B)           |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                   Payload Length (uint32 BE)                  |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                Payload CRC32 (IEEE 802.3 uint32)              |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                     Payload Bytes (0 .. 64 MiB)               |
|                              ...                              |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### MessageType Identifiers
- `0x01` `CLIENT_CMD` — Client request (`SET`, `GET`, `DELETE`, `EXISTS`, `UPDATE`, `SNAPSHOT`, `STATS`, `PING`)
- `0x02` `SERVER_RESP` — Structured server response (`StatusCode`, `value`, `message`, `leader_hint`)
- `0x0A` `REPL_APPEND` / `0x0B` `REPL_ACK` — Primary-to-replica WAL shipping
- `0x14` `HEARTBEAT_PING` / `0x15` `HEARTBEAT_PONG` — Node health probes
- `0x1E` `RAFT_REQUEST_VOTE` / `0x1F` `RAFT_REQUEST_VOTE_RESP` — Raft election RPC
- `0x20` `RAFT_APPEND_ENTRIES` / `0x21` `RAFT_APPEND_ENTRIES_RESP` — Raft log replication & heartbeat RPC

---

## 3. On-Disk WAL Record Layout (`wal.log` / `raft_wal.log`)

```text
+--------------------+--------------------+------------------------------------------+
| PayloadLen (4B BE) |   CRC32 (4B BE)    | Serialized LogRecord (PayloadLen Bytes)  |
+--------------------+--------------------+------------------------------------------+
                                           \
                                            +--> [Index:8B][Term:8B][Op:1B]
                                                 [KeyLen:4B][Key][ValLen:4B][Value]
```

If power is lost mid-write, `WriteAheadLog::replay()` detects either `offset + 8 + len > file_size` or `CRC32 != expected_crc` and repairs the file in-place via `ftruncate(fd, last_valid_offset)`.
