// Package raft implements a deliberately simplified, Raft-style leader
// election and replication protocol. It is not full Raft — see the
// project README's "Raft guarantees provided vs. not provided" section for
// the exact, honest list of what's cut. The goal is a version small enough
// to fully understand and defend, not a partial copy of the paper.
//
// Two simplifications worth calling out here specifically, since they
// shape this file:
//
//  1. The replicated log lives entirely in memory (Node.entries), mirrored
//     one-for-one against the storage WAL (store.WAL) for durability. There
//     is no random-access read-back from the WAL; on restart, the WAL's
//     replayed records rebuild the in-memory log view once, and after that
//     the two are kept in lockstep by every AppendRecord call.
//  2. AppendEntries' consistency check only ever compares against a
//     follower's LAST log entry, not an arbitrary index — so it correctly
//     handles a follower whose log is a prefix of the leader's (behind, the
//     common case), but does not handle a follower whose log has diverged
//     to be AHEAD of the leader's (e.g. a former leader rejoining with an
//     entry it appended to its own WAL but never got to replicate before
//     crashing). That case is the paper's log-matching / conflicting-entry
//     truncation machinery, which this project deliberately doesn't
//     implement. In the scripted demo this never arises, because a
//     Propose() call only ever returns after majority commit — so by the
//     time an operator kills a node, everything in that node's log was
//     already durable on at least one other node too.
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

	nextIndex     map[string]uint64 // leader-only, reset on becoming leader
	matchIndex    map[string]uint64 // leader-only, reset on becoming leader
	pendingWrites map[uint64]chan error

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
		id:                  p.ID,
		peerAddrs:           p.PeerAddrs,
		peerIDs:             peerIDs,
		transport:           p.Transport,
		stateStore:          p.StateStore,
		wal:                 p.Store.WAL(),
		fsm:                 p.Store,
		heartbeatInterval:   heartbeat,
		electionTimeoutMin:  minTO,
		electionTimeoutMax:  maxTO,
		proposeTimeout:      proposeTimeout,
		rng:                 rand.New(rand.NewSource(seed)),
		state:               Follower,
		currentTerm:         currentTerm,
		votedFor:            votedFor,
		entries:             entries,
		lastLogIndex:        lastLogIndex,
		lastLogTerm:         lastLogTerm,
		// Everything replayed from the WAL was already applied to the
		// Store's map by OpenForRaft, so treat it as committed+applied.
		// This is the restart-time counterpart of the divergence
		// simplification described in the package doc.
		commitIndex:   lastLogIndex,
		lastApplied:   lastLogIndex,
		nextIndex:     make(map[string]uint64),
		matchIndex:    make(map[string]uint64),
		pendingWrites: make(map[uint64]chan error),

		resetElectionCh: make(chan struct{}, 1),
		stopCh:          make(chan struct{}),
	}
	return n, nil
}

// Start begins the election timer and heartbeat/replication loops in the
// background.
func (n *Node) Start() {
	go n.electionLoop()
	go n.heartbeatLoop()
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

func (n *Node) triggerReplication() {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	peers := append([]string(nil), n.peerIDs...)
	n.mu.Unlock()

	for _, peerID := range peers {
		go n.replicateTo(peerID, term)
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
		toSend = append([]LogEntry(nil), n.entries[next-1:]...)
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
		n.nextIndex[peerID] = reply.LastLogIndex + 1
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

// Propose appends a new entry for (op, key, value) to the leader's log and
// blocks until it is replicated to a majority (committed), the timeout
// elapses, or this node stops being leader. Returns ErrNotLeader if this
// node isn't currently the leader (checked both up front and if
// leadership is lost while the write is in flight).
func (n *Node) Propose(op, key, value string, timeout time.Duration) error {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}

	index := n.lastLogIndex + 1
	term := n.currentTerm
	entry := LogEntry{Index: index, Term: term, Op: op, Key: key, Value: value}

	if err := n.wal.AppendRecord(store.Record{Index: index, Term: term, Op: op, Key: key, Value: value}); err != nil {
		n.mu.Unlock()
		return fmt.Errorf("raft: append to log: %w", err)
	}
	n.entries = append(n.entries, entry)
	n.lastLogIndex = index
	n.lastLogTerm = term
	n.matchIndex[n.id] = index // the leader counts as one replica of its own entry

	done := make(chan error, 1)
	n.pendingWrites[index] = done
	n.mu.Unlock()

	n.triggerReplication()

	if timeout <= 0 {
		timeout = n.proposeTimeout
	}
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		return ErrProposeTimeout
	}
}

// --- election ---

func (n *Node) startElection() {
	n.mu.Lock()
	if n.state == Leader {
		n.mu.Unlock()
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
	n.resetElectionTimer()

	if args.PrevLogIndex != n.lastLogIndex || args.PrevLogTerm != n.lastLogTerm {
		reply.Success = false
		reply.LastLogIndex = n.lastLogIndex
		return reply
	}

	for _, e := range args.Entries {
		rec := store.Record{Index: e.Index, Term: e.Term, Op: e.Op, Key: e.Key, Value: e.Value}
		if err := n.wal.AppendRecord(rec); err != nil {
			log.Printf("[%s] failed to append replicated entry index=%d: %v", n.id, e.Index, err)
			reply.Success = false
			reply.LastLogIndex = n.lastLogIndex
			return reply
		}
		n.entries = append(n.entries, e)
		n.lastLogIndex = e.Index
		n.lastLogTerm = e.Term
	}
	if len(args.Entries) > 0 {
		log.Printf("[%s] term=%d appended %d log entries from %s (lastLogIndex now %d)",
			n.id, n.currentTerm, len(args.Entries), args.LeaderID, n.lastLogIndex)
	}

	if args.LeaderCommit > n.commitIndex {
		if args.LeaderCommit < n.lastLogIndex {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = n.lastLogIndex
		}
		n.applyCommittedLocked()
	}

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
