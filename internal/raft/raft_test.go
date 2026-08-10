package raft

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

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
// strings, not real host:ports). Returns the nodes and a cleanup func.
func newTestCluster(t *testing.T, n int) ([]*Node, *transport.FakeNetwork) {
	t.Helper()

	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("node%d", i+1)
	}

	fakeNet := transport.NewFakeNetwork()
	nodes := make([]*Node, n)

	for i, id := range ids {
		peerAddrs := make(map[string]string, n-1)
		for _, other := range ids {
			if other != id {
				peerAddrs[other] = other
			}
		}

		statePath := filepath.Join(t.TempDir(), id+"-state.json")
		node, err := NewNode(Params{
			ID:                 id,
			PeerAddrs:          peerAddrs,
			Transport:          fakeNet.Transport(id),
			StateStore:         NewStateStore(statePath),
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
	})

	for _, node := range nodes {
		node.Start()
	}
	return nodes, fakeNet
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
	nodes, _ := newTestCluster(t, 3)

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
	nodes, _ := newTestCluster(t, 3)

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
	nodes, _ := newTestCluster(t, 3)
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
	nodes, fakeNet := newTestCluster(t, 3)
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
