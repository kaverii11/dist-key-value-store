package raft

// RequestVoteArgs is sent by a Candidate to every peer when starting an
// election.
type RequestVoteArgs struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidateId"`
	LastLogIndex uint64 `json:"lastLogIndex"`
	LastLogTerm  uint64 `json:"lastLogTerm"`
}

type RequestVoteReply struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"voteGranted"`
}

// LogEntry is one committed (or pending) operation in a node's replicated
// log. The log doubles as the storage WAL — see store.Record, whose
// Index/Term fields mirror these — so replaying the WAL on startup rebuilds
// both KV state and Raft's view of its own log in one pass.
type LogEntry struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Op    string `json:"op"` // "PUT" | "DELETE"
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// AppendEntriesArgs is sent by the Leader both as a heartbeat (Entries
// empty) and to replicate writes (Entries non-empty).
type AppendEntriesArgs struct {
	Term         uint64     `json:"term"`
	LeaderID     string     `json:"leaderId"`
	PrevLogIndex uint64     `json:"prevLogIndex"`
	PrevLogTerm  uint64     `json:"prevLogTerm"`
	Entries      []LogEntry `json:"entries,omitempty"`
	LeaderCommit uint64     `json:"leaderCommit"`
}

type AppendEntriesReply struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
	// LastLogIndex is the follower's own last log index after processing
	// this request, sent on both success and failure. On failure, it lets
	// the leader jump nextIndex directly to where the follower actually
	// is instead of decrementing one entry at a time — this is the
	// "simplified decrement-and-retry" mentioned in the package doc; we
	// don't implement the paper's binary-search-style conflict
	// optimization, and (see package doc) we don't handle a follower
	// whose log has diverged to be AHEAD of the leader's (e.g. a former
	// leader rejoining with an entry it never got to replicate before
	// crashing) — that case is explicitly out of scope.
	LastLogIndex uint64 `json:"lastLogIndex"`
}
