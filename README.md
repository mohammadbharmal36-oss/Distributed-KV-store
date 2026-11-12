# Distributed Key-Value Store

A C++20 key-value store demonstrating durable storage and distributed coordination. It includes a TCP client/server, a write-ahead log (WAL), snapshots, primary-replica replication, and a Raft-based cluster mode.

> **Project status:** This is an educational project, not a production-ready database. It targets Linux and WSL2 and uses POSIX sockets and file APIs.

## Features

- Thread-safe in-memory key-value storage.
- Custom framed binary TCP protocol (`KV20`) with CRC32 checksums.
- WAL-backed persistence and snapshots.
- Standalone, primary-replica, and Raft operating modes.
- Interactive command-line client and a simple multi-threaded benchmark mode.
- GoogleTest unit and integration test suites.

## Requirements

- Linux or WSL2
- C++20 compiler (GCC or Clang)
- CMake 3.20 or newer
- Git

The test configuration downloads GoogleTest through CMake `FetchContent`, so building tests requires an internet connection. To build without tests, configure with `-DBUILD_TESTS=OFF`.

## Build and test

From the repository root:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

## Quick start

Start a standalone node:

```bash
./build/kv_node --mode standalone --id 1 \
  --host 127.0.0.1 --client-port 7001 --peer-port 7101 \
  --data-dir ./data/node1
```

In another terminal, connect the CLI:

```bash
./build/kv_cli --host 127.0.0.1 --port 7001
```

Example session:

```text
kv> SET greeting hello
OK
kv> GET greeting
hello
kv> EXISTS greeting
(integer) 1
kv> QUIT
```

The node accepts `--help` for its options. The client supports `--host` and `--port`; use `./build/kv_cli` without options to see its interactive prompt.

Run the client's multi-threaded benchmark against a running node:

```bash
./build/kv_cli --host 127.0.0.1 --port 7001 \
  --benchmark --threads 8 --requests 80000
```

## Run a three-node Raft cluster

Start each command in its own terminal from the repository root. Each node needs a unique ID, client and peer ports, and data directory. The `--peers` value lists the other nodes as `id@host:peerPort:clientPort`, separated by commas.

```bash
./build/kv_node --id 1 --host 127.0.0.1 --client-port 7001 --peer-port 7101 \
  --data-dir ./data/n1 --mode raft \
  --peers 2@127.0.0.1:7102:7002,3@127.0.0.1:7103:7003
```

```bash
./build/kv_node --id 2 --host 127.0.0.1 --client-port 7002 --peer-port 7102 \
  --data-dir ./data/n2 --mode raft \
  --peers 1@127.0.0.1:7101:7001,3@127.0.0.1:7103:7003
```

```bash
./build/kv_node --id 3 --host 127.0.0.1 --client-port 7003 --peer-port 7103 \
  --data-dir ./data/n3 --mode raft \
  --peers 1@127.0.0.1:7101:7001,2@127.0.0.1:7102:7002
```

Then connect the CLI to a node:

```bash
./build/kv_cli --host 127.0.0.1 --port 7002
```

The CLI follows a leader hint when a request is redirected by a Raft follower.

## Client commands

| Command | Description |
|---|---|
| `SET <key> <value>` | Insert or replace a value |
| `GET <key>` | Read a value |
| `UPDATE <key> <value>` | Update an existing key |
| `DELETE <key>` | Delete a key (`DEL` is also accepted) |
| `EXISTS <key>` | Check whether a key exists |
| `SNAPSHOT` | Request a snapshot |
| `STATS` | Show node statistics |
| `PING` | Check that the server responds |
| `QUIT` / `EXIT` | Exit the CLI |

Values may contain spaces. This command-line interface is intended for simple text input; use the C++ client API or implement the wire protocol for arbitrary binary values.

## Replication modes

The node supports three modes via `--mode`:

- `standalone` — one node with local persistence.
- `replica` — primary-replica replication; use `--role primary` or `--role replica` and configure peers with `--peers`. The `--sync` option accepts `async`, `majority`, or `all`.
- `raft` — Raft-based leader election and log replication.

For all modes, configure each node with its own data directory and non-conflicting ports. Cluster operation depends on the network being able to reach the configured peer endpoints.

## Repository layout

```text
include/    Public C++ headers
src/        Server, client, storage, protocol, and replication implementations
tests/      GoogleTest suites
docs/       Architecture, protocol, and design notes
```

## Documentation

- [Architecture and synchronization](docs/architecture.md)
- [Wire protocol and disk format](docs/protocol.md)
- [Design decisions and trade-offs](docs/design-decisions.md)

## Limitations

- The dataset is held in memory; WAL and snapshots provide persistence, not an on-disk query engine.
- Cluster membership is configured at startup.
- Raft follower reads may be stale; linearizable read coordination is not implemented.
- This project is not production-hardened. Evaluate durability, failure behavior, security, and performance for your workload before relying on it.
