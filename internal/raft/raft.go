// Package raft implements a deliberately compact, Raft-style leader
// election and replication protocol. It is not full Raft (no snapshots,
// no membership changes, no pre-vote): see the README's "Raft guarantees"
// section for the exact list. The goal is a version small enough to fully
// understand and defend.
//
// How this file is laid out:
//
//  1. The replicated log lives in memory (Node.entries), mirrored one-for-one
//     against the storage WAL (store.WAL) for durability. On restart the
//     WAL's records rebuild the log, but are NOT treated as committed: an
//     entry may never have reached a majority (a leader killed mid-write).
//     commitIndex starts at 0 and entries are applied only once a leader
//     confirms them.
//  2. AppendEntries does full log matching: a follower checks the leader's
//     entry at PrevLogIndex, and if its own log has a conflicting suffix
//     (same index, different term), it deletes that uncommitted suffix
//     (rewriting the WAL) and takes the leader's entries. A new leader
//     appends a no-op entry so earlier-term entries get committed, and only
//     serves reads once that no-op commits.
//  3. Writes are group-committed: proposalLoop gives a batch of waiting
//     writes consecutive indexes and fsyncs them once. Each peer has one
//     replicator goroutine with at most one AppendEntries in flight.
//
// Points 1 and 2 were found missing by the chaos test (scripts/chaos.sh):
// killing the leader under load left it with an uncommitted entry that it
// applied on restart and could never reconcile.
package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/kaverii11/dist-key-value-store/internal/store"
	"github.com/kaverii11/dist-key-value-store/internal/transport"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// Timer values. See the project plan's Design Decisions for why these are
// inflated versus the Raft paper's reference values (150-300ms election /
// 50ms heartbeat): margin against Windows scheduling/logging overhead
// during a live, narrated demo, while still failing over in well under a
// second in practice.
const (
	DefaultHeartbeatInterval  = 100 * time.Millisecond
	DefaultElectionTimeoutMin = 300 * time.Millisecond
	DefaultElectionTimeoutMax = 600 * time.Millisecond
)

// DefaultProposeTimeout bounds how long Propose waits for a write to
// commit before giving up.
const DefaultProposeTimeout = 2 * time.Second

var (
	// ErrNotLeader is returned by Propose when this node isn't currently
	// the leader (including if it loses leadership while a proposal is
	// in flight).
	ErrNotLeader = errors.New("raft: not the leader")
	// ErrProposeTimeout is returned by Propose if the entry didn't
	// commit within the timeout.
	ErrProposeTimeout = errors.New("raft: proposal timed out waiting for commit")
)

// Params configures a new Node.
type Params struct {
	ID         string
	PeerAddrs  map[string]string // peer node ID -> cluster address
	Transport  transport.Transport
	StateStore *StateStore

	// Store backs the replicated log (its WAL doubles as the raft log —
	// see the package doc) and is where committed entries get applied.
	// ReplayedRecords must be exactly what store.OpenForRaft returned
	// alongside Store, so the in-memory log view matches what's durably
	// on disk.
	Store           *store.Store
	ReplayedRecords []store.Record

	HeartbeatInterval  time.Duration // 0 = DefaultHeartbeatInterval
	ElectionTimeoutMin time.Duration // 0 = DefaultElectionTimeoutMin
	ElectionTimeoutMax time.Duration // 0 = DefaultElectionTimeoutMax
	ProposeTimeout     time.Duration // 0 = DefaultProposeTimeout
}

// Node is one member of the cluster's Raft state machine.
type Node struct {
	id         string
	peerAddrs  map[string]string
	peerIDs    []string
	transport  transport.Transport
	stateStore *StateStore
	wal        *store.WAL
	fsm        *store.Store

	heartbeatInterval  time.Duration
	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration
	proposeTimeout     time.Duration
	rng                *rand.Rand // only touched from electionLoop's goroutine

	mu          sync.Mutex
	state       State
	currentTerm uint64
	votedFor    string
	leaderID    string

	entries      []LogEntry // in-memory log, 1-indexed: entries[i-1] has Index i
	lastLogIndex uint64
	lastLogTerm  uint64
	commitIndex  uint64
	lastApplied  uint64

	// readyIndex is the index of the no-op entry this node appended when it
	// became leader. Reads are only served once it is committed, so a new
	// leader never answers from a state machine that is missing entries
	// committed by an earlier leader.
	readyIndex uint64
	// lastContact is when this node last finished handling an
	// AppendEntries from the current leader (see startElection).
	lastContact time.Time

	nextIndex     map[string]uint64 // leader-only, reset on becoming leader
	matchIndex    map[string]uint64 // leader-only, reset on becoming leader
	pendingWrites map[uint64]chan error

	// replCh has one 1-slot channel per peer. A send means "this peer may
	// need an AppendEntries"; the peer's replicator goroutine coalesces
	// sends, so each peer has at most one AppendEntries in flight.
	replCh map[string]chan struct{}
	// proposeCh feeds client writes to proposalLoop, which appends whole
	// batches with one fsync (group commit).
	proposeCh chan proposal

	resetElectionCh chan struct{}
	stopCh          chan struct{}
	stopOnce        sync.Once
}

// NewNode constructs a Node. Call Start to begin its timers and Stop to
// shut it down cleanly.
func NewNode(p Params) (*Node, error) {
	if p.ID == "" {
		return nil, fmt.Errorf("raft: Params.ID is required")
	}
	if p.Transport == nil {
		return nil, fmt.Errorf("raft: Params.Transport is required")
	}
	if p.StateStore == nil {
		return nil, fmt.Errorf("raft: Params.StateStore is required")
	}
	if p.Store == nil {
		return nil, fmt.Errorf("raft: Params.Store is required")
	}

	heartbeat := p.HeartbeatInterval
	if heartbeat == 0 {
		heartbeat = DefaultHeartbeatInterval
	}
	minTO := p.ElectionTimeoutMin
	if minTO == 0 {
		minTO = DefaultElectionTimeoutMin
	}
	maxTO := p.ElectionTimeoutMax
	if maxTO == 0 {
		maxTO = DefaultElectionTimeoutMax
	}
	if maxTO <= minTO {
		return nil, fmt.Errorf("raft: ElectionTimeoutMax must be > ElectionTimeoutMin")
	}
	proposeTimeout := p.ProposeTimeout
	if proposeTimeout == 0 {
		proposeTimeout = DefaultProposeTimeout
	}

	currentTerm, votedFor, err := p.StateStore.Load()
	if err != nil {
		return nil, err
	}

	peerIDs := make([]string, 0, len(p.PeerAddrs))
	for id := range p.PeerAddrs {
		peerIDs = append(peerIDs, id)
	}

	seed := time.Now().UnixNano()
	for _, c := range p.ID {
		seed += int64(c)
	}

	entries := make([]LogEntry, len(p.ReplayedRecords))
	for i, rec := range p.ReplayedRecords {
		entries[i] = LogEntry{Index: rec.Index, Term: rec.Term, Op: rec.Op, Key: rec.Key, Value: rec.Value}
	}
	var lastLogIndex, lastLogTerm uint64
	if n := len(entries); n > 0 {
		lastLogIndex, lastLogTerm = entries[n-1].Index, entries[n-1].Term
	}

	n := &Node{
		id:                 p.ID,
		peerAddrs:          p.PeerAddrs,
		peerIDs:            peerIDs,
		transport:          p.Transport,
		stateStore:         p.StateStore,
		wal:                p.Store.WAL(),
		fsm:                p.Store,
		heartbeatInterval:  heartbeat,
		electionTimeoutMin: minTO,
		electionTimeoutMax: maxTO,
		proposeTimeout:     proposeTimeout,
		rng:                rand.New(rand.NewSource(seed)),
		state:              Follower,
		currentTerm:        currentTerm,
		votedFor:           votedFor,
		entries:            entries,
		lastLogIndex:       lastLogIndex,
		lastLogTerm:        lastLogTerm,
		// Replayed entries are NOT assumed committed: the last few may
		// never have reached a majority (e.g. this node was a leader that
		// was killed mid-write). commitIndex starts at 0 and is learned
		// from the leader (or, if this node becomes leader, from its own
		// no-op entry committing); entries are applied only then.
		commitIndex:   0,
		lastApplied:   0,
		nextIndex:     make(map[string]uint64),
		matchIndex:    make(map[string]uint64),
		pendingWrites: make(map[uint64]chan error),

		resetElectionCh: make(chan struct{}, 1),
		stopCh:          make(chan struct{}),
		replCh:          make(map[string]chan struct{}, len(peerIDs)),
		proposeCh:       make(chan proposal, 4096),
	}
	for _, id := range peerIDs {
		n.replCh[id] = make(chan struct{}, 1)
	}
	return n, nil
}

// Start begins the election timer and heartbeat/replication loops in the
// background.
func (n *Node) Start() {
	go n.electionLoop()
	go n.heartbeatLoop()
	go n.proposalLoop()
	for _, id := range n.peerIDs {
		go n.peerReplicator(id)
	}
}

// Stop halts the node's background loops. Safe to call more than once.
func (n *Node) Stop() {
	n.stopOnce.Do(func() { close(n.stopCh) })
}

// --- accessors (safe for concurrent use) ---

func (n *Node) ID() string { return n.id }

func (n *Node) State() State {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

func (n *Node) IsLeader() bool { return n.State() == Leader }

// IsReadyLeader reports whether this node is leader AND has committed its
// no-op entry, i.e. its state machine includes every committed write, so
// it is safe to serve reads.
func (n *Node) IsReadyLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state == Leader && n.readyIndex > 0 && n.commitIndex >= n.readyIndex
}

func (n *Node) CurrentTerm() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

// LeaderID returns who this node currently believes is leader, or "" if
// unknown (e.g. mid-election).
func (n *Node) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// LeaderAddr returns the current leader's client address, for redirect
// hints, or "" if unknown or the leader isn't a known peer.
func (n *Node) LeaderAddr(clientAddrs map[string]string) string {
	id := n.LeaderID()
	if id == n.id {
		return clientAddrs[n.id]
	}
	return clientAddrs[id]
}

// LastLogIndex returns the index of the last entry in this node's log
// (committed or not).
func (n *Node) LastLogIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastLogIndex
}

// CommitIndex returns the highest committed log index, for status/demo output.
func (n *Node) CommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

// --- election timer loop ---

func (n *Node) randomElectionTimeout() time.Duration {
	span := n.electionTimeoutMax - n.electionTimeoutMin
	return n.electionTimeoutMin + time.Duration(n.rng.Int63n(int64(span)))
}

func (n *Node) resetElectionTimer() {
	select {
	case n.resetElectionCh <- struct{}{}:
	default:
	}
}

func (n *Node) electionLoop() {
	timer := time.NewTimer(n.randomElectionTimeout())
	defer timer.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.resetElectionCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(n.randomElectionTimeout())
		case <-timer.C:
			n.startElection()
			timer.Reset(n.randomElectionTimeout())
		}
	}
}

// --- heartbeat / replication loop (Leader only; a no-op tick otherwise) ---
//
// A single code path serves both purposes: on each tick, every peer gets
// whatever AppendEntries would naturally send it right now — real entries
// if it's behind, or an empty one (a pure heartbeat) if it's caught up.
// The same call is also triggered immediately after Propose, so a write
// doesn't have to wait for the next tick to start replicating.

func (n *Node) heartbeatLoop() {
	ticker := time.NewTicker(n.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.triggerReplication()
		}
	}
}

// triggerReplication asks every peer's replicator to send an
// AppendEntries (heartbeat or new entries). Non-blocking: if a peer
// already has a request pending, the signals merge into one.
func (n *Node) triggerReplication() {
	for _, id := range n.peerIDs {
		n.nudge(id)
	}
}

func (n *Node) nudge(peerID string) {
	select {
	case n.replCh[peerID] <- struct{}{}:
	default:
	}
}

// peerReplicator owns all AppendEntries traffic to one peer: one request
// at a time, each carrying everything the peer is missing (up to
// maxEntriesPerAppend). If the peer is still behind afterwards, it goes
// again right away instead of waiting for the next heartbeat.
func (n *Node) peerReplicator(peerID string) {
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.replCh[peerID]:
		}
		n.mu.Lock()
		leader, term := n.state == Leader, n.currentTerm
		n.mu.Unlock()
		if !leader {
			continue
		}
		n.replicateTo(peerID, term)
		n.mu.Lock()
		behind := n.state == Leader && n.currentTerm == term && n.nextIndex[peerID] <= n.lastLogIndex
		n.mu.Unlock()
		if behind {
			n.nudge(peerID)
		}
	}
}

// replicateTo sends peerID whatever AppendEntries it needs right now
// (heartbeat or real entries), and reconciles nextIndex/matchIndex and
// commitIndex based on the reply.
func (n *Node) replicateTo(peerID string, term uint64) {
	n.mu.Lock()
	if n.state != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	next := n.nextIndex[peerID]
	if next < 1 {
		next = 1
	}
	prevIndex := next - 1
	var prevTerm uint64
	if prevIndex >= 1 && prevIndex <= uint64(len(n.entries)) {
		prevTerm = n.entries[prevIndex-1].Term
	}
	var toSend []LogEntry
	if next <= uint64(len(n.entries)) {
		end := uint64(len(n.entries))
		if end-(next-1) > maxEntriesPerAppend {
			end = next - 1 + maxEntriesPerAppend
		}
		toSend = append([]LogEntry(nil), n.entries[next-1:end]...)
	}
	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      toSend,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	reply, err := n.sendAppendEntries(peerID, args)
	if err != nil {
		return // peer unreachable this round; next tick (or the next Propose) retries
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.stepDownLocked(reply.Term)
		return
	}
	if n.state != Leader || n.currentTerm != term {
		return // stale reply — no longer leader of this term
	}

	if reply.Success {
		newMatch := prevIndex + uint64(len(toSend))
		if newMatch > n.matchIndex[peerID] {
			n.matchIndex[peerID] = newMatch
		}
		if newMatch+1 > n.nextIndex[peerID] {
			n.nextIndex[peerID] = newMatch + 1
		}
		n.maybeAdvanceCommitLocked()
	} else {
		// The follower is behind or has a conflicting suffix; its reply
		// says where its log can still match ours. Retry from there.
		next := reply.LastLogIndex + 1
		if next > n.lastLogIndex+1 {
			next = n.lastLogIndex + 1
		}
		if next < 1 {
			next = 1
		}
		n.nextIndex[peerID] = next // peerReplicator retries right away if still behind
	}
}

// maybeAdvanceCommitLocked assumes n.mu is held. It looks for the highest
// index present on a majority of nodes whose entry was written in the
// leader's current term (the standard Raft safety rule: never commit an
// entry from an earlier term purely by replica count) and, if found,
// advances commitIndex and applies newly-committed entries.
func (n *Node) maybeAdvanceCommitLocked() {
	majority := (len(n.peerIDs)+1)/2 + 1

	for N := n.lastLogIndex; N > n.commitIndex; N-- {
		if N < 1 || N > uint64(len(n.entries)) {
			continue
		}
		if n.entries[N-1].Term != n.currentTerm {
			continue
		}
		count := 1 // the leader itself
		for _, peerID := range n.peerIDs {
			if n.matchIndex[peerID] >= N {
				count++
			}
		}
		if count >= majority {
			n.commitIndex = N
			n.applyCommittedLocked()
			return
		}
	}
}

// applyCommittedLocked assumes n.mu is held.
func (n *Node) applyCommittedLocked() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		e := n.entries[n.lastApplied-1]
		n.fsm.Apply(store.Record{Index: e.Index, Term: e.Term, Op: e.Op, Key: e.Key, Value: e.Value})
		if done, ok := n.pendingWrites[e.Index]; ok {
			done <- nil
			delete(n.pendingWrites, e.Index)
		}
	}
}

// --- client writes ---

// Propose queues a new entry for (op, key, value) for the leader's log
// (proposalLoop appends it, batched with other waiting writes) and blocks
// until it is replicated to a majority (committed), the timeout elapses,
// or this node stops being leader. Returns ErrNotLeader if this
// node isn't currently the leader (checked both up front and if
// leadership is lost while the write is in flight).
func (n *Node) Propose(op, key, value string, timeout time.Duration) error {
	if !n.IsLeader() {
		return ErrNotLeader
	}
	if timeout <= 0 {
		timeout = n.proposeTimeout
	}
	p := proposal{op: op, key: key, value: value, done: make(chan error, 1)}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case n.proposeCh <- p:
	case <-n.stopCh:
		return ErrNotLeader
	case <-timer.C:
		return ErrProposeTimeout
	}
	select {
	case err := <-p.done:
		return err
	case <-timer.C:
		return ErrProposeTimeout
	}
}

type proposal struct {
	op, key, value string
	done           chan error
}

// maxProposalBatch caps how many client writes share one fsync.
const maxProposalBatch = 256

// proposalLoop is the leader's group commit: it takes every write that is
// waiting (up to maxProposalBatch), gives them consecutive log indexes,
// writes them to the WAL with ONE fsync, then hands them to replication.
// Under load this turns N fsyncs into 1 and keeps the lock free for
// heartbeats between batches.
func (n *Node) proposalLoop() {
	batch := make([]proposal, 0, maxProposalBatch)
	for {
		batch = batch[:0]
		select {
		case <-n.stopCh:
			return
		case p := <-n.proposeCh:
			batch = append(batch, p)
		}
	drain:
		for len(batch) < maxProposalBatch {
			select {
			case p := <-n.proposeCh:
				batch = append(batch, p)
			default:
				break drain
			}
		}
		n.appendBatch(batch)
	}
}

func (n *Node) appendBatch(batch []proposal) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		for _, p := range batch {
			p.done <- ErrNotLeader
		}
		return
	}
	term := n.currentTerm
	recs := make([]store.Record, len(batch))
	entries := make([]LogEntry, len(batch))
	for i, p := range batch {
		index := n.lastLogIndex + 1 + uint64(i)
		recs[i] = store.Record{Index: index, Term: term, Op: p.op, Key: p.key, Value: p.value}
		entries[i] = LogEntry{Index: index, Term: term, Op: p.op, Key: p.key, Value: p.value}
	}
	if err := n.wal.AppendRecords(recs); err != nil {
		n.mu.Unlock()
		for _, p := range batch {
			p.done <- fmt.Errorf("raft: append to log: %w", err)
		}
		return
	}
	n.entries = append(n.entries, entries...)
	last := entries[len(entries)-1]
	n.lastLogIndex, n.lastLogTerm = last.Index, last.Term
	n.matchIndex[n.id] = last.Index // the leader counts as one replica of its own entries
	for i, p := range batch {
		n.pendingWrites[entries[i].Index] = p.done
	}
	n.maybeAdvanceCommitLocked() // a single-node cluster commits right away
	n.mu.Unlock()

	n.triggerReplication()
}

// --- election ---

func (n *Node) startElection() {
	n.mu.Lock()
	if n.state == Leader {
		n.mu.Unlock()
		return
	}
	// The timer can fire while an AppendEntries handler is still busy
	// (e.g. a slow fsync on a big batch). If we heard from a live leader
	// more recently than the minimum election timeout, it isn't dead:
	// don't start an election that would only depose it.
	if n.state == Follower && n.leaderID != "" && time.Since(n.lastContact) < n.electionTimeoutMin {
		n.mu.Unlock()
		n.resetElectionTimer()
		return
	}
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.leaderID = ""
	term := n.currentTerm
	lastLogIndex, lastLogTerm := n.lastLogIndex, n.lastLogTerm
	peers := append([]string(nil), n.peerIDs...)
	if err := n.stateStore.Save(n.currentTerm, n.votedFor); err != nil {
		log.Printf("[%s] WARNING: failed to persist raft state: %v", n.id, err)
	}
	log.Printf("[%s] term=%d became Candidate", n.id, term)
	n.mu.Unlock()

	if len(peers) == 0 {
		// Single-node "cluster" — trivially wins its own election.
		n.mu.Lock()
		if n.state == Candidate && n.currentTerm == term {
			n.becomeLeaderLocked()
		}
		n.mu.Unlock()
		return
	}

	majority := (len(peers)+1)/2 + 1
	votes := 1 // vote for self
	var voteMu sync.Mutex

	for _, peerID := range peers {
		go func(peerID string) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply, err := n.sendRequestVote(peerID, args)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.Term > n.currentTerm {
				n.stepDownLocked(reply.Term)
				return
			}
			if n.state != Candidate || n.currentTerm != term {
				return // stale reply — election has already moved on
			}
			if !reply.VoteGranted {
				return
			}

			voteMu.Lock()
			votes++
			v := votes
			voteMu.Unlock()

			if v >= majority {
				n.becomeLeaderLocked()
			}
		}(peerID)
	}
}

// becomeLeaderLocked assumes n.mu is held.
func (n *Node) becomeLeaderLocked() {
	if n.state == Leader {
		return
	}
	n.state = Leader
	n.leaderID = n.id
	log.Printf("[%s] term=%d won election -> Leader", n.id, n.currentTerm)

	n.nextIndex = make(map[string]uint64, len(n.peerIDs))
	n.matchIndex = make(map[string]uint64, len(n.peerIDs))
	for _, peerID := range n.peerIDs {
		n.nextIndex[peerID] = n.lastLogIndex + 1
		n.matchIndex[peerID] = 0
	}

	// Append a no-op entry in the new term. A leader may only count
	// replicas for entries of its own term (see maybeAdvanceCommitLocked),
	// so committing this entry is what also commits everything before it,
	// including entries a previous leader replicated but never committed.
	index := n.lastLogIndex + 1
	noop := LogEntry{Index: index, Term: n.currentTerm, Op: store.OpNoop}
	if err := n.wal.AppendRecord(store.Record{Index: index, Term: n.currentTerm, Op: store.OpNoop}); err != nil {
		log.Printf("[%s] failed to append no-op entry: %v", n.id, err)
	} else {
		n.entries = append(n.entries, noop)
		n.lastLogIndex = index
		n.lastLogTerm = n.currentTerm
		n.matchIndex[n.id] = index
		n.readyIndex = index
	}

	// Send an immediate round so followers don't sit out a full
	// heartbeat interval before hearing about the new leader.
	go n.triggerReplication()
}

// stepDownLocked assumes n.mu is held.
func (n *Node) stepDownLocked(term uint64) {
	changed := term > n.currentTerm
	wasNonFollower := n.state != Follower
	if changed {
		n.currentTerm = term
		n.votedFor = ""
	}
	n.state = Follower
	if err := n.stateStore.Save(n.currentTerm, n.votedFor); err != nil {
		log.Printf("[%s] WARNING: failed to persist raft state: %v", n.id, err)
	}
	if wasNonFollower {
		log.Printf("[%s] term=%d -> Follower (saw higher term)", n.id, n.currentTerm)
	}
	for index, done := range n.pendingWrites {
		select {
		case done <- ErrNotLeader:
		default:
		}
		delete(n.pendingWrites, index)
	}
	n.resetElectionTimer()
}

// --- RPC handlers (called from the transport server's Handler) ---

func (n *Node) RequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		n.stepDownLocked(args.Term)
	}

	reply := RequestVoteReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		reply.VoteGranted = false
		return reply
	}

	logOK := args.LastLogTerm > n.lastLogTerm ||
		(args.LastLogTerm == n.lastLogTerm && args.LastLogIndex >= n.lastLogIndex)

	if (n.votedFor == "" || n.votedFor == args.CandidateID) && logOK {
		n.votedFor = args.CandidateID
		if err := n.stateStore.Save(n.currentTerm, n.votedFor); err != nil {
			log.Printf("[%s] WARNING: failed to persist raft state: %v", n.id, err)
		}
		log.Printf("[%s] term=%d granted vote to %s", n.id, n.currentTerm, args.CandidateID)
		n.resetElectionTimer()
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	return reply
}

// AppendEntries handles both heartbeats (Entries empty) and log
// replication (Entries non-empty): validates the term, follows leadership,
// runs the (simplified — see package doc) consistency check, appends any
// new entries to the shared WAL, and advances/applies commitIndex.
func (n *Node) AppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		n.stepDownLocked(args.Term)
	}

	reply := AppendEntriesReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		reply.Success = false
		reply.LastLogIndex = n.lastLogIndex
		return reply
	}

	if n.state != Follower {
		n.state = Follower
	}
	if n.leaderID != args.LeaderID {
		log.Printf("[%s] term=%d -> Follower (leader is %s)", n.id, n.currentTerm, args.LeaderID)
	}
	n.leaderID = args.LeaderID
	n.lastContact = time.Now()
	n.resetElectionTimer()

	// Log matching: our log must contain the leader's entry at
	// PrevLogIndex with the same term.
	if args.PrevLogIndex > n.lastLogIndex {
		reply.Success = false
		reply.LastLogIndex = n.lastLogIndex // we're behind: resend from our end
		return reply
	}
	if args.PrevLogIndex >= 1 && n.entries[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		// Conflict: skip back over every entry of the conflicting term in
		// one step, so the leader doesn't walk back one entry per RPC.
		conflictTerm := n.entries[args.PrevLogIndex-1].Term
		i := args.PrevLogIndex
		for i > 1 && n.entries[i-2].Term == conflictTerm {
			i--
		}
		reply.Success = false
		reply.LastLogIndex = i - 1
		return reply
	}

	// Skip entries we already have; at the first entry whose term differs
	// from ours at the same index, our entry (and everything after it) was
	// never committed: drop that suffix and take the leader's entries.
	newEntries := args.Entries
	for len(newEntries) > 0 && newEntries[0].Index <= n.lastLogIndex {
		e := newEntries[0]
		if n.entries[e.Index-1].Term == e.Term {
			newEntries = newEntries[1:] // already have it (a retried or overlapping RPC)
			continue
		}
		if err := n.truncateFromLocked(e.Index); err != nil {
			log.Printf("[%s] failed to truncate log from index %d: %v", n.id, e.Index, err)
			reply.Success = false
			reply.LastLogIndex = n.lastLogIndex
			return reply
		}
		break
	}
	appended := len(newEntries)
	if appended > 0 {
		recs := make([]store.Record, appended)
		for i, e := range newEntries {
			recs[i] = store.Record{Index: e.Index, Term: e.Term, Op: e.Op, Key: e.Key, Value: e.Value}
		}
		if err := n.wal.AppendRecords(recs); err != nil { // one fsync for the whole batch
			log.Printf("[%s] failed to append %d replicated entries: %v", n.id, appended, err)
			reply.Success = false
			reply.LastLogIndex = n.lastLogIndex
			return reply
		}
		n.entries = append(n.entries, newEntries...)
		last := newEntries[appended-1]
		n.lastLogIndex, n.lastLogTerm = last.Index, last.Term
	}
	if appended > 0 {
		log.Printf("[%s] term=%d appended %d log entries from %s (lastLogIndex now %d)",
			n.id, n.currentTerm, appended, args.LeaderID, n.lastLogIndex)
	}

	// Only entries the leader just confirmed match its log may be marked
	// committed; anything past them could still be a stale suffix.
	lastNew := args.PrevLogIndex + uint64(len(args.Entries))
	if args.LeaderCommit > n.commitIndex {
		c := args.LeaderCommit
		if c > lastNew {
			c = lastNew
		}
		if c > n.commitIndex {
			n.commitIndex = c
			n.applyCommittedLocked()
		}
	}

	n.lastContact = time.Now() // after the fsync, so a slow disk doesn't look like a dead leader
	reply.Success = true
	reply.LastLogIndex = n.lastLogIndex
	return reply
}

// --- outbound RPC helpers ---

func (n *Node) sendRequestVote(peerID string, args RequestVoteArgs) (RequestVoteReply, error) {
	payload, err := json.Marshal(args)
	if err != nil {
		return RequestVoteReply{}, err
	}
	resp, err := n.transport.Send(n.peerAddrs[peerID], transport.Message{
		Type:    transport.MsgRequestVoteReq,
		Payload: payload,
	})
	if err != nil {
		return RequestVoteReply{}, err
	}
	var reply RequestVoteReply
	if err := json.Unmarshal(resp.Payload, &reply); err != nil {
		return RequestVoteReply{}, err
	}
	return reply, nil
}

func (n *Node) sendAppendEntries(peerID string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	payload, err := json.Marshal(args)
	if err != nil {
		return AppendEntriesReply{}, err
	}
	resp, err := n.transport.Send(n.peerAddrs[peerID], transport.Message{
		Type:    transport.MsgAppendEntriesReq,
		Payload: payload,
	})
	if err != nil {
		return AppendEntriesReply{}, err
	}
	var reply AppendEntriesReply
	if err := json.Unmarshal(resp.Payload, &reply); err != nil {
		return AppendEntriesReply{}, err
	}
	return reply, nil
}

// HandleMessage dispatches an incoming transport.Message to the right RPC
// handler. Pass this as the Handler for the cluster-port transport.Server.
func (n *Node) HandleMessage(msg transport.Message) (transport.Message, error) {
	switch msg.Type {
	case transport.MsgRequestVoteReq:
		var args RequestVoteArgs
		if err := json.Unmarshal(msg.Payload, &args); err != nil {
			return transport.Message{}, err
		}
		reply := n.RequestVote(args)
		payload, err := json.Marshal(reply)
		if err != nil {
			return transport.Message{}, err
		}
		return transport.Message{Type: transport.MsgRequestVoteResp, ID: msg.ID, Payload: payload}, nil

	case transport.MsgAppendEntriesReq:
		var args AppendEntriesArgs
		if err := json.Unmarshal(msg.Payload, &args); err != nil {
			return transport.Message{}, err
		}
		reply := n.AppendEntries(args)
		payload, err := json.Marshal(reply)
		if err != nil {
			return transport.Message{}, err
		}
		return transport.Message{Type: transport.MsgAppendEntriesResp, ID: msg.ID, Payload: payload}, nil

	default:
		return transport.Message{}, fmt.Errorf("raft: unknown message type %q", msg.Type)
	}
}

// maxEntriesPerAppend caps one AppendEntries batch, so a follower that was
// down for a long time catches up in bounded-size steps.
const maxEntriesPerAppend = 512

// truncateFromLocked assumes n.mu is held. It deletes log entries from
// index onward, in memory and on disk. Only uncommitted entries may be
// removed; Raft guarantees a committed entry never conflicts with the
// leader, so hitting a committed one means a bug, and we refuse.
func (n *Node) truncateFromLocked(index uint64) error {
	if index <= n.commitIndex {
		return fmt.Errorf("raft: refusing to truncate committed entry %d (commitIndex %d)", index, n.commitIndex)
	}
	keep := n.entries[:index-1]
	recs := make([]store.Record, len(keep))
	for i, e := range keep {
		recs[i] = store.Record{Index: e.Index, Term: e.Term, Op: e.Op, Key: e.Key, Value: e.Value}
	}
	if err := n.wal.Rewrite(recs); err != nil {
		return err
	}
	log.Printf("[%s] term=%d dropped %d uncommitted log entries from index %d (conflict with leader)",
		n.id, n.currentTerm, len(n.entries)-len(keep), index)
	n.entries = append([]LogEntry(nil), keep...)
	n.lastLogIndex, n.lastLogTerm = 0, 0
	if k := len(keep); k > 0 {
		n.lastLogIndex, n.lastLogTerm = keep[k-1].Index, keep[k-1].Term
	}
	return nil
}
