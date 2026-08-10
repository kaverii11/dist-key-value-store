package raft

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/kaverii11/dist-key-value-store/internal/store"
	"github.com/kaverii11/dist-key-value-store/internal/transport"
)

// testTimers are much tighter than the production defaults (see raft.go)
// because these tests run over the in-memory FakeNetwork, which has
// effectively zero latency — there's no Windows-scheduling/logging-I/O
// margin to protect here, only test wall-clock time.
const (
	testHeartbeat   = 15 * time.Millisecond
	testElectionMin = 60 * time.Millisecond
	testElectionMax = 120 * time.Millisecond
)

// newTestCluster wires up n Nodes on a shared FakeNetwork, using node IDs
// as their fake "addresses" directly (FakeNetwork addresses are opaque
// strings, not real host:ports). Returns the nodes, their backing stores
// (same index order), and the network.
func newTestCluster(t *testing.T, n int) ([]*Node, []*store.Store, *transport.FakeNetwork) {
	t.Helper()

	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("node%d", i+1)
	}

	fakeNet := transport.NewFakeNetwork()
	nodes := make([]*Node, n)
	stores := make([]*store.Store, n)

	for i, id := range ids {
		peerAddrs := make(map[string]string, n-1)
		for _, other := range ids {
			if other != id {
				peerAddrs[other] = other
			}
		}

		walPath := filepath.Join(t.TempDir(), id+".wal")
		s, records, err := store.OpenForRaft(walPath)
		if err != nil {
			t.Fatalf("OpenForRaft(%s): %v", id, err)
		}
		stores[i] = s

		statePath := filepath.Join(t.TempDir(), id+"-state.json")
		node, err := NewNode(Params{
			ID:                 id,
			PeerAddrs:          peerAddrs,
			Transport:          fakeNet.Transport(id),
			StateStore:         NewStateStore(statePath),
			Store:              s,
			ReplayedRecords:    records,
			HeartbeatInterval:  testHeartbeat,
			ElectionTimeoutMin: testElectionMin,
			ElectionTimeoutMax: testElectionMax,
		})
		if err != nil {
			t.Fatalf("NewNode(%s): %v", id, err)
		}
		fakeNet.Register(id, node.HandleMessage)
		nodes[i] = node
	}

	t.Cleanup(func() {
		for _, node := range nodes {
			node.Stop()
		}
		for _, s := range stores {
			s.Close()
		}
	})

	for _, node := range nodes {
		node.Start()
	}
	return nodes, stores, fakeNet
}

// waitForApplied polls s.Get(key) until it equals want or timeout elapses.
func waitForApplied(t *testing.T, s *store.Store, key, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if v, ok := s.Get(key); ok && v == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	v, ok := s.Get(key)
	t.Fatalf("Get(%q) = (%q, %v); want (%q, true) within %s", key, v, ok, want, timeout)
}

// waitForLeader polls until exactly one node reports itself Leader and
// every other node agrees (LeaderID matches, no stale second leader), or
// fails the test after timeout.
func waitForLeader(t *testing.T, nodes []*Node, timeout time.Duration) *Node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leader *Node
		agree := true
		for _, n := range nodes {
			if n.State() == Leader {
				if leader != nil {
					agree = false // two simultaneous leaders would be a real bug
					break
				}
				leader = n
			}
		}
		if leader != nil && agree {
			consistent := true
			for _, n := range nodes {
				if n.LeaderID() != leader.ID() {
					consistent = false
					break
				}
			}
			if consistent {
				return leader
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no leader elected (or cluster didn't converge) within %s", timeout)
	return nil
}

func TestSingleClusterElectsOneLeader(t *testing.T) {
	nodes, _, _ := newTestCluster(t, 3)

	leader := waitForLeader(t, nodes, 2*time.Second)
	t.Logf("elected leader: %s (term %d)", leader.ID(), leader.CurrentTerm())

	term := leader.CurrentTerm()
	for _, n := range nodes {
		if n.CurrentTerm() != term {
			t.Errorf("node %s term = %d; want %d (all nodes should converge on the same term)", n.ID(), n.CurrentTerm(), term)
		}
	}
}

func TestThreeWaySplitVoteEventuallyResolves(t *testing.T) {
	nodes, _, _ := newTestCluster(t, 3)

	// Force all three nodes to become candidates for the same term at
	// (as close to) the same instant, before any RequestVote RPCs can be
	// exchanged — a genuine 3-way split vote, since a 3-node majority is 2
	// and each node can only ever vote for itself here. Every node's
	// election timer will keep retrying with a fresh random timeout until
	// one of them gets ahead, exactly like a real split-vote recovery.
	for _, n := range nodes {
		go n.startElection()
	}

	leader := waitForLeader(t, nodes, 3*time.Second)
	t.Logf("split vote resolved: leader=%s term=%d", leader.ID(), leader.CurrentTerm())
}

func TestFollowerStepsDownOnHigherTerm(t *testing.T) {
	nodes, _, _ := newTestCluster(t, 3)
	leader := waitForLeader(t, nodes, 2*time.Second)

	// Simulate a message from a legitimate future term (e.g. this node was
	// partitioned and another election already happened without it).
	reply := leader.AppendEntries(AppendEntriesArgs{
		Term:     leader.CurrentTerm() + 5,
		LeaderID: "some-other-node",
	})

	if leader.State() != Follower {
		t.Fatalf("leader state after higher-term AppendEntries = %s; want Follower", leader.State())
	}
	if got, want := leader.CurrentTerm(), reply.Term; got != want {
		t.Fatalf("term after step-down = %d; want %d", got, want)
	}
}

func TestMinorityPartitionCannotElectLeader(t *testing.T) {
	nodes, _, fakeNet := newTestCluster(t, 3)
	n1, n2, n3 := nodes[0], nodes[1], nodes[2]

	// Isolate n3 from the other two in both directions — a clean minority
	// partition. n1/n2 still form a majority (2 of 3) and should elect a
	// leader normally; n3 alone can never reach majority.
	fakeNet.Cut(n3.ID(), n1.ID())
	fakeNet.Cut(n1.ID(), n3.ID())
	fakeNet.Cut(n3.ID(), n2.ID())
	fakeNet.Cut(n2.ID(), n3.ID())

	leader := waitForLeader(t, []*Node{n1, n2}, 2*time.Second)
	t.Logf("majority side elected leader: %s", leader.ID())

	// Give n3 ample time to try (and fail) to become leader on its own.
	time.Sleep(500 * time.Millisecond)
	if n3.State() == Leader {
		t.Fatalf("minority-partitioned node %s became Leader; should be impossible without a majority", n3.ID())
	}
}

func TestProposeReplicatesToAllFollowers(t *testing.T) {
	nodes, stores, _ := newTestCluster(t, 3)
	leader := waitForLeader(t, nodes, 2*time.Second)

	if err := leader.Propose(store.OpPut, "foo", "bar", 2*time.Second); err != nil {
		t.Fatalf("Propose: %v", err)
	}

	for i, n := range nodes {
		waitForApplied(t, stores[i], "foo", "bar", time.Second)
		t.Logf("%s applied foo=bar (commitIndex=%d)", n.ID(), n.CommitIndex())
	}
}

func TestProposeOnFollowerFailsWithLeaderHint(t *testing.T) {
	nodes, _, _ := newTestCluster(t, 3)
	leader := waitForLeader(t, nodes, 2*time.Second)

	for _, n := range nodes {
		if n.ID() == leader.ID() {
			continue
		}
		err := n.Propose(store.OpPut, "foo", "bar", 200*time.Millisecond)
		if !errors.Is(err, ErrNotLeader) {
			t.Fatalf("Propose on follower %s = %v; want ErrNotLeader", n.ID(), err)
		}
	}
}

// TestLeaderFailoverContinuesServingWrites is the core of the M5 demo
// story: kill the leader (here, simulated by stopping it and cutting it
// off the fake network — the closest a fake network gets to taskkill),
// confirm the remaining majority elects a new leader, and confirm writes
// keep working through the new leader.
func TestLeaderFailoverContinuesServingWrites(t *testing.T) {
	nodes, stores, fakeNet := newTestCluster(t, 3)
	leader := waitForLeader(t, nodes, 2*time.Second)

	if err := leader.Propose(store.OpPut, "before", "1", 2*time.Second); err != nil {
		t.Fatalf("Propose before failover: %v", err)
	}
	for i := range nodes {
		waitForApplied(t, stores[i], "before", "1", time.Second)
	}

	killedID := leader.ID()
	leader.Stop()
	fakeNet.Register(killedID, func(transport.Message) (transport.Message, error) {
		return transport.Message{}, fmt.Errorf("node %s is down", killedID)
	})

	var survivors []*Node
	var survivorStores []*store.Store
	for i, n := range nodes {
		if n.ID() != killedID {
			survivors = append(survivors, n)
			survivorStores = append(survivorStores, stores[i])
		}
	}

	newLeader := waitForLeader(t, survivors, 2*time.Second)
	if newLeader.ID() == killedID {
		t.Fatalf("new leader %s is the node we just killed", newLeader.ID())
	}
	t.Logf("failover: killed=%s new leader=%s", killedID, newLeader.ID())

	if err := newLeader.Propose(store.OpPut, "after", "2", 2*time.Second); err != nil {
		t.Fatalf("Propose after failover: %v", err)
	}
	for i := range survivors {
		waitForApplied(t, survivorStores[i], "after", "2", time.Second)
	}
}

// TestRestartedNodeCatchesUp is the other half of the M5 demo story: a
// node goes down, the cluster keeps taking writes without it, and when it
// comes back — with a fresh Store and a fresh Node reading the same
// on-disk WAL and raft-state files, exactly like a real process restart —
// it rejoins and catches up to the latest committed value.
func TestRestartedNodeCatchesUp(t *testing.T) {
	const n = 3
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("node%d", i+1)
	}

	dir := t.TempDir()
	fakeNet := transport.NewFakeNetwork()
	nodes := make([]*Node, n)
	stores := make([]*store.Store, n)
	walPaths := make([]string, n)
	statePaths := make([]string, n)

	peerAddrsFor := func(self string) map[string]string {
		m := make(map[string]string, n-1)
		for _, other := range ids {
			if other != self {
				m[other] = other
			}
		}
		return m
	}

	for i, id := range ids {
		walPaths[i] = filepath.Join(dir, id+".wal")
		statePaths[i] = filepath.Join(dir, id+"-state.json")

		s, records, err := store.OpenForRaft(walPaths[i])
		if err != nil {
			t.Fatalf("OpenForRaft(%s): %v", id, err)
		}
		stores[i] = s

		node, err := NewNode(Params{
			ID:                 id,
			PeerAddrs:          peerAddrsFor(id),
			Transport:          fakeNet.Transport(id),
			StateStore:         NewStateStore(statePaths[i]),
			Store:              s,
			ReplayedRecords:    records,
			HeartbeatInterval:  testHeartbeat,
			ElectionTimeoutMin: testElectionMin,
			ElectionTimeoutMax: testElectionMax,
		})
		if err != nil {
			t.Fatalf("NewNode(%s): %v", id, err)
		}
		fakeNet.Register(id, node.HandleMessage)
		nodes[i] = node
	}
	t.Cleanup(func() {
		for _, node := range nodes {
			node.Stop()
		}
		for _, s := range stores {
			s.Close() // double-Close is harmless; the victim is closed explicitly mid-test
		}
	})
	for _, node := range nodes {
		node.Start()
	}

	leader := waitForLeader(t, nodes, 2*time.Second)
	if err := leader.Propose(store.OpPut, "k1", "v1", 2*time.Second); err != nil {
		t.Fatalf("Propose k1: %v", err)
	}
	for i := range nodes {
		waitForApplied(t, stores[i], "k1", "v1", time.Second)
	}

	// Pick a follower to kill and restart (the leader-failover path is
	// covered separately by TestLeaderFailoverContinuesServingWrites).
	victim := -1
	for i, node := range nodes {
		if node.ID() != leader.ID() {
			victim = i
			break
		}
	}
	victimID := nodes[victim].ID()

	// Hard "kill": stop its loops, close its store (releases the WAL file
	// handle — required before anything can reopen it, especially on
	// Windows), and cut it off the network.
	nodes[victim].Stop()
	stores[victim].Close()
	fakeNet.Register(victimID, func(transport.Message) (transport.Message, error) {
		return transport.Message{}, fmt.Errorf("node %s is down", victimID)
	})

	// The cluster keeps taking writes while it's down — leader + the
	// other follower is still a majority of 3.
	if err := leader.Propose(store.OpPut, "k2", "v2", 2*time.Second); err != nil {
		t.Fatalf("Propose k2 while %s is down: %v", victimID, err)
	}

	// "Restart": brand new Store + Node reading the same on-disk files.
	s2, records2, err := store.OpenForRaft(walPaths[victim])
	if err != nil {
		t.Fatalf("reopen store for %s: %v", victimID, err)
	}
	t.Cleanup(func() { s2.Close() })

	if got, ok := s2.Get("k1"); !ok || got != "v1" {
		t.Fatalf("restarted %s's replayed WAL missing k1: got %q, ok=%v", victimID, got, ok)
	}
	if _, ok := s2.Get("k2"); ok {
		t.Fatalf("restarted %s's replayed WAL already has k2, which it should have missed while down", victimID)
	}

	restarted, err := NewNode(Params{
		ID:                 victimID,
		PeerAddrs:          peerAddrsFor(victimID),
		Transport:          fakeNet.Transport(victimID),
		StateStore:         NewStateStore(statePaths[victim]),
		Store:              s2,
		ReplayedRecords:    records2,
		HeartbeatInterval:  testHeartbeat,
		ElectionTimeoutMin: testElectionMin,
		ElectionTimeoutMax: testElectionMax,
	})
	if err != nil {
		t.Fatalf("NewNode restart: %v", err)
	}
	fakeNet.Register(victimID, restarted.HandleMessage)
	restarted.Start()
	t.Cleanup(restarted.Stop)

	waitForApplied(t, s2, "k2", "v2", 2*time.Second)
	t.Logf("restarted node %s caught up on k2 after rejoining", victimID)
}
