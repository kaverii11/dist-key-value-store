// Package raft implements a deliberately simplified, Raft-style leader
// election and replication protocol. It is not full Raft — see the
// project README's "Raft guarantees provided vs. not provided" section for
// the exact, honest list of what's cut (no log compaction, no membership
// changes, a simplified conflict-resolution backoff instead of the paper's
// full log-matching optimization, no linearizable-read mechanism, no
// pre-vote). The goal is a version small enough to fully understand and
// defend, not a partial copy of the paper.
package raft

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

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

// Params configures a new Node.
type Params struct {
	ID         string
	PeerAddrs  map[string]string // peer node ID -> cluster address
	Transport  transport.Transport
	StateStore *StateStore

	HeartbeatInterval  time.Duration // 0 = DefaultHeartbeatInterval
	ElectionTimeoutMin time.Duration // 0 = DefaultElectionTimeoutMin
	ElectionTimeoutMax time.Duration // 0 = DefaultElectionTimeoutMax
}

// Node is one member of the cluster's Raft state machine.
type Node struct {
	id         string
	peerAddrs  map[string]string
	peerIDs    []string
	transport  transport.Transport
	stateStore *StateStore

	heartbeatInterval  time.Duration
	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration
	rng                *rand.Rand // only touched from electionLoop's goroutine

	mu          sync.Mutex
	state       State
	currentTerm uint64
	votedFor    string
	leaderID    string

	// Populated by the log/replication layer (wired in M5); a fresh Node
	// with no log reports 0/0, which is correct for an empty log and keeps
	// the RequestVote up-to-date check well-defined even before M5 lands.
	lastLogIndex uint64
	lastLogTerm  uint64

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

	n := &Node{
		id:                  p.ID,
		peerAddrs:           p.PeerAddrs,
		peerIDs:             peerIDs,
		transport:           p.Transport,
		stateStore:          p.StateStore,
		heartbeatInterval:   heartbeat,
		electionTimeoutMin:  minTO,
		electionTimeoutMax:  maxTO,
		rng:                 rand.New(rand.NewSource(seed)),
		state:               Follower,
		currentTerm:         currentTerm,
		votedFor:            votedFor,
		resetElectionCh:     make(chan struct{}, 1),
		stopCh:              make(chan struct{}),
	}
	return n, nil
}

// Start begins the election timer and heartbeat loops in the background.
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

// --- heartbeat loop (Leader only; a no-op tick otherwise) ---

func (n *Node) heartbeatLoop() {
	ticker := time.NewTicker(n.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.broadcastHeartbeatIfLeader()
		}
	}
}

func (n *Node) broadcastHeartbeatIfLeader() {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	peers := append([]string(nil), n.peerIDs...)
	n.mu.Unlock()

	for _, peerID := range peers {
		go n.sendHeartbeat(peerID, term)
	}
}

func (n *Node) sendHeartbeat(peerID string, term uint64) {
	args := AppendEntriesArgs{Term: term, LeaderID: n.id}
	reply, err := n.sendAppendEntries(peerID, args)
	if err != nil {
		return // peer unreachable this round; next heartbeat tick will retry
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if reply.Term > n.currentTerm {
		n.stepDownLocked(reply.Term)
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
	term := n.currentTerm
	peers := append([]string(nil), n.peerIDs...)
	// Send an immediate heartbeat so followers don't sit out a full
	// heartbeat interval before hearing about the new leader.
	go func() {
		for _, peerID := range peers {
			go n.sendHeartbeat(peerID, term)
		}
	}()
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
// replication (Entries non-empty). M4 only wires the term/leadership
// bookkeeping; the log consistency check and entry application are added
// in M5 once the WAL-backed log is plugged in.
func (n *Node) AppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		n.stepDownLocked(args.Term)
	}

	reply := AppendEntriesReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		reply.Success = false
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

	reply.Success = true
	reply.MatchIndex = n.lastLogIndex
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
// handler. Pass this (or a wrapper around it) as the Handler for the
// cluster-port transport.Server.
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
