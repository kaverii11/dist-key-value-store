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
	// MatchIndex lets the leader advance nextIndex/matchIndex for this
	// follower in one round trip on success, instead of inferring it.
	MatchIndex uint64 `json:"matchIndex"`
}
