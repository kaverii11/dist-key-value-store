# dist-key-value-store

A minimal distributed key-value store in Go, built from scratch as an interview
project. It is intentionally scoped to be a **simplified Raft-style system I deeply
understand**, not a partial copy of the full Raft paper. See
[Raft guarantees provided vs. not provided](#raft-guarantees-provided-vs-not-provided)
for exactly where the line is drawn.

Four things this project demonstrates:

1. **Storage engine** — in-memory map + write-ahead log (WAL). Every write hits the
   WAL (with `fsync`) before memory. Startup replays the WAL; a torn or corrupted
   final record is detected via checksum and safely truncated, not replayed.
2. **Concurrency correctness** — `sync.RWMutex`-guarded store, verified with a
   multi-goroutine mixed get/put/delete test under `go test -race`.
3. **Leader election + replication** — a simplified Raft: terms, randomized
   election timeouts, `RequestVote`, heartbeat `AppendEntries`, majority-committed
   writes replicated from leader to followers.
4. **Node communication** — plain TCP with length-prefixed JSON framing (no
   gRPC/protobuf — see [Design Decisions](#design-decisions)), timeouts, and clean
   handling of a peer disappearing mid-connection.

Everything here runs natively on Windows via PowerShell — no bash, no Makefiles.

## Requirements

- Go 1.21+
- gcc (mingw-w64), **only** if you want to run `go test -race` — not required for
  normal build/test/run

Run `.\scripts\check-env.ps1` to verify both.

## Building & testing

```powershell
go build ./...
go vet ./...
go test ./...
go test ./internal/store/... -race -v   # requires gcc, see check-env.ps1
```

## Running a single node

```powershell
go run ./cmd/node -config .\scripts\cluster.json -id node1
```

## Running the 3-node demo

```powershell
.\scripts\demo.ps1
```

This opens 3 PowerShell windows (one per node), waits for a leader to be elected,
writes a key through `kvctl`, hard-kills the leader with `taskkill /F`, shows the
remaining two nodes elect a new leader and keep serving writes, then restarts the
killed node and shows it rejoin and catch up. Runs start-to-finish in well under a
minute.

<!-- Architecture, wire protocol, milestone-by-milestone notes, design decisions,
     and the full Raft guarantees disclosure are filled in as each milestone lands. -->

## Raft guarantees: provided vs. not provided

**Provided:** leader election with terms + randomized timeouts + majority voting;
heartbeat-based leader liveness; log replication with a `PrevLogIndex`/`PrevLogTerm`
consistency check; majority-acknowledged commits; persisted `currentTerm`/`votedFor`
across restarts; catch-up for a rejoining/stale follower.

**Not provided:** log compaction/snapshotting; cluster membership changes; the
paper's full log-matching backtracking optimization (we do simple decrement-and-
retry); linearizable read guarantees (no read-index/lease mechanism); the pre-vote
extension; Byzantine fault tolerance.

## Design decisions

See the project plan for the full writeup of tradeoffs (Raft log vs. storage WAL,
read consistency, one vs. two TCP ports per node, timer aggressiveness).
