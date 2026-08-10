# dist-key-value-store

A minimal distributed key-value store in Go, built from scratch as an interview
project. It is intentionally scoped to be a **simplified Raft-style system I deeply
understand**, not a partial copy of the full Raft paper. See
[Raft guarantees: provided vs. not provided](#raft-guarantees-provided-vs-not-provided)
for exactly where the line is drawn.

Four things this project demonstrates:

1. **Storage engine** ([`internal/store`](internal/store)) -- in-memory map + write-ahead
   log (WAL). Every write hits the WAL (with `fsync`) before memory. Startup replays
   the WAL; a torn or corrupted final record is detected via checksum and safely
   truncated, not replayed.
2. **Concurrency correctness** -- `sync.RWMutex`-guarded store, verified with a
   100-goroutine mixed get/put/delete test under `go test -race`.
3. **Leader election + replication** ([`internal/raft`](internal/raft)) -- a
   simplified Raft: terms, randomized election timeouts, `RequestVote`, heartbeat
   `AppendEntries`, majority-committed writes replicated from leader to followers,
   and rejoin/catch-up for a node that comes back after being down.
4. **Node communication** ([`internal/transport`](internal/transport)) -- plain TCP
   with length-prefixed JSON framing (no gRPC/protobuf -- see
   [Design Decisions](#design-decisions)), timeouts on every RPC, and clean handling
   of a peer disappearing mid-connection.

Everything runs natively on Windows via PowerShell -- no bash, no Makefiles.

## Requirements

- Go 1.21+
- gcc (mingw-w64), **only** if you want to run `go test -race` -- not required for
  normal build/test/run

Run `.\scripts\check-env.ps1` to verify both; it prints exact install commands
(`winget`) for anything missing.

## Building & testing

```powershell
go build ./...
go vet ./...
go test ./...
go test ./internal/store/... -race -v   # requires gcc, see check-env.ps1
go test ./internal/raft/...  -race -v
```

## Running a single node

```powershell
go run ./cmd/node -id node1 -config .\scripts\cluster.json -data-dir .\data
```

Each node opens two TCP ports (from `scripts\cluster.json`): a **cluster port**
for Raft RPCs and a **client port** for Get/Put/Delete. The node's own stdin REPL
(`put`/`get`/`del`/`status`/`quit`) goes through the same leader-only path a
remote client would.

```powershell
go run ./cmd/kvctl -addr 127.0.0.1:8001 put color blue
go run ./cmd/kvctl -addr 127.0.0.1:8001 get color
```

## Running the full demo

```powershell
.\scripts\demo.ps1
```

Builds `node.exe`/`kvctl.exe`, starts 3 nodes in separate PowerShell windows, waits
for a leader, writes a key through it, shows a follower redirecting instead of
answering, hard-kills the leader with `taskkill /F /T`, shows the survivors elect a
new leader and keep serving reads and writes, restarts the killed node, and shows it
rejoin and catch up on what it missed. Runs start-to-finish in well under a minute
(typically ~10-15s). `scripts\run-3-nodes.ps1` is the simpler, non-scripted version
for just watching 3 interactive nodes and driving them by hand.

## Architecture

```
                     +-------------------+
                     |   cmd/kvctl (CLI) |
                     +---------+---------+
                               | TCP, client port (e.g. 8001)
                               v
   +----------------------------------------------------------+
   |                        cmd/node                          |
   |                                                            |
   |   internal/api.Server  <--- client Put/Get/Delete          |
   |         |  (leader-only; redirects via LeaderHint)         |
   |         v                                                  |
   |   internal/raft.Node  <---- RequestVote / AppendEntries    |
   |         |     ^                    (cluster port, e.g. 7001)
   |         v     |                                            |
   |   internal/store.Store (in-memory map + WAL)                |
   +----------------------------------------------------------+
                               ^
                               | internal/transport (TCP, length-prefixed JSON)
                               v
                     (same wiring on node2, node3)
```

The Raft log and the storage WAL are the same file: every WAL record carries
`Index`/`Term` in addition to `Op`/`Key`/`Value`, so replaying it on startup
rebuilds both the KV map and Raft's view of its own log in one pass. See
[Design Decisions](#design-decisions).

## Wire protocol

All node-to-node and client-to-node traffic uses the same framing
(`internal/transport/protocol.go`):

```
[4 bytes: uint32 big-endian length][payload: JSON]
```

```go
type Message struct {
    Type    string          `json:"type"`
    ID      string          `json:"id"`
    Payload json.RawMessage `json:"payload"`
}
```

Message types: `RequestVoteReq/Resp`, `AppendEntriesReq/Resp` (cluster port,
`internal/raft/rpc.go`); `ClientPutReq`, `ClientGetReq`, `ClientDeleteReq`,
`ClientResp` (client port, `internal/api/server.go`).

Each RPC is one request/response over a fresh, short-lived connection (dial, write,
read, close) bounded by a single timeout -- no connection pooling/multiplexing, so
failure modes stay easy to reason about.

## Node state machine

`Follower` -> (election timeout) -> `Candidate` -> (majority votes) -> `Leader`, or
back to `Follower` the moment any RPC or reply carries a higher term. See
`internal/raft/raft.go`'s package doc for the exact rules.

| Parameter | Value |
|---|---|
| Heartbeat interval (leader -> followers) | 100ms |
| Election timeout | random(300ms, 600ms), re-rolled on every reset |
| RPC dial+read timeout (per attempt) | 200ms |

These are roughly 2x the Raft paper's reference values (150-300ms / 50ms), to leave
margin for Windows scheduling/logging overhead during a live, narrated demo, while
still failing over in well under a second in practice.

`currentTerm`/`votedFor` are persisted to `data\<id>-raftstate.json` via
write-temp-then-`os.Rename` on every change, so a restarted node never double-votes
in a term it already saw.

## Milestones

Each of these was independently runnable and demoable on its own:

1. **Single-node store + WAL + crash recovery** (`internal/store`) -- in-memory map,
   WAL with checksums, torn-write/corruption detection and safe truncation.
2. **Concurrency + race tests** -- 100-goroutine mixed-op hammer test, clean under
   `go test -race`.
3. **TCP transport** (`internal/transport`) -- framing, timeouts, peer-disappears
   handling, plus a fake in-memory network for deterministic tests.
4. **Leader election** (`internal/raft`) -- terms, randomized timeouts,
   RequestVote, heartbeat AppendEntries, persisted term/vote.
5. **Replication + rejoin/catch-up** -- real log entries, majority commit, apply to
   state machine, leader-only client API with redirect, a restarted node catching up.
6. **Demo scripts + README** -- this file, `scripts\demo.ps1`.

## Test strategy

| Area | How, deterministically, on Windows |
|---|---|
| WAL durability/corruption | In-process fault injection: truncate/corrupt bytes in a closed file, then reopen -- no subprocess, no timing dependency |
| Concurrent access | `go test -race` with many goroutines -- races are caught by instrumentation, not by observing a bad outcome |
| Transport framing/disconnects | Real TCP over loopback with short deadlines; disconnects triggered explicitly in-test |
| Election/partition/replication | A fake in-memory `Transport` (`internal/transport/fake.go`) with directional `Cut`/`Heal` instead of real network manipulation -- Windows firewall-rule partitioning needs admin rights and is slow/flaky. Real TCP is still exercised end-to-end by `demo.ps1` (that's what `taskkill` is for) |

## Design decisions

**1. Raft log vs. storage WAL: one file or two?** Unified -- the WAL record carries
`Term`/`Index`, so replaying it rebuilds both KV state and Raft's log view.
Alternative: separate log and WAL (cleaner separation of concerns) at the cost of
double the code and two fsyncs per write.

**2. Read consistency: leader-only vs. stale local reads.** Leader-only (followers
redirect via `LeaderHint`) -- simple to reason about and defend, no staleness
caveats. Alternative: any node answers reads from its own applied state (lower
latency, but explicitly non-linearizable).

**3. One TCP port vs. two per node.** Two -- a cluster port for Raft RPCs, a client
port for the KV API. Keeps logs legible and mirrors how real systems separate
internal and data-path protocols, at the cost of twice the listeners.

**4. Timer aggressiveness.** ~2x the Raft paper's reference values (see table
above) -- safer margin against Windows scheduling/logging jitter during a narrated
demo, while still failing over in well under a second.

## Raft guarantees: provided vs. not provided

**Provided:**
- Leader election with terms, randomized election timeouts, majority voting
- Heartbeat-based leader liveness (`AppendEntries` with no entries)
- Log replication with a consistency check (`PrevLogIndex`/`PrevLogTerm`) before a
  follower accepts new entries
- Majority-acknowledged commits (leader only acks a client write once a majority of
  nodes -- leader included -- have it durably in their WAL)
- Persisted `currentTerm`/`votedFor` across restarts, so a crashed-and-restarted
  node cannot double-vote within a term it already saw
- Catch-up for a rejoining/stale follower via a simplified nextIndex backoff

**Not provided (explicitly out of scope):**
- Log compaction/snapshotting -- the log grows unbounded; fine for a demo, not
  production
- Cluster membership changes -- the 3-node set is fixed at startup
- The Raft paper's full log-matching backtracking optimization -- we use simple
  decrement-and-retry, and (see `internal/raft`'s package doc) don't handle a
  follower whose log has diverged to be *ahead* of the leader's (e.g. a former
  leader rejoining with an entry it never got to replicate before crashing) --
  the scripted demo never hits this, because `Propose` only returns after majority
  commit, so by the time an operator kills a node, everything in its log was
  already durable on at least one other node too
- Linearizable read guarantees -- leader-local reads are safe in the common case,
  but there's no read-index/lease mechanism to formally prove it under all
  leader-doubt scenarios
- Pre-vote extension -- a partitioned minority node can still increment its term
  and disrupt the cluster when the partition heals
- Byzantine fault tolerance / split-brain beyond term+majority logic

## Windows-specific notes

- **WAL file locking**: one long-lived `os.File` handle per node, all writes
  serialized through the store's mutex. Tests explicitly `Close()` before
  `os.Remove`, or Windows refuses with "process cannot access the file."
- **fsync**: `(*os.File).Sync()` maps to `FlushFileBuffers` on Windows -- genuinely
  durable, no special handling needed.
- **Ports**: bind to `127.0.0.1`, not `0.0.0.0` -- avoids an interactive Windows
  Defender Firewall prompt that would interrupt a live demo.
- **Process kill**: `taskkill /F /T /PID <pid>` is the Windows equivalent of
  `kill -9` -- ungraceful and uninterceptable, which is exactly what's needed to
  prove WAL recovery + Raft failover survive a real hard kill.
- `.ps1` scripts are kept plain-ASCII -- Windows PowerShell 5.1 misreads UTF-8
  em-dashes etc. in files without a BOM, which corrupts the parser in confusing,
  seemingly-unrelated ways.
