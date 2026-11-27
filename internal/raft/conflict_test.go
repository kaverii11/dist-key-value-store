package raft

import (
	"errors"
	"testing"
	"time"

	"github.com/kaverii11/dist-key-value-store/internal/store"
)

// TestIsolatedLeaderDropsUncommittedEntry covers the case the chaos test
// (scripts/chaos.sh) found: a leader appends a write to its own log, is
// cut off before replicating it, a new leader takes over and commits other
// writes at the same index, and the old leader then rejoins. The old
// leader must drop its uncommitted entry (never apply it) and end up with
// exactly the new leader's log.
func TestIsolatedLeaderDropsUncommittedEntry(t *testing.T) {
	nodes, stores, net := newTestCluster(t, 3)
	old := waitForLeader(t, nodes, 2*time.Second)
	if err := old.Propose(store.OpPut, "k-base", "b", time.Second); err != nil {
		t.Fatalf("Propose k-base: %v", err)
	}

	var others []*Node
	var oldStore *store.Store
	for i, n := range nodes {
		if n == old {
			oldStore = stores[i]
			continue
		}
		others = append(others, n)
	}

	// Cut the leader off from both followers, in both directions.
	for _, o := range others {
		net.Cut(old.ID(), o.ID())
		net.Cut(o.ID(), old.ID())
	}

	// It still believes it is leader, so it appends this write to its own
	// log, but can't reach a majority: the write must NOT be acknowledged.
	if err := old.Propose(store.OpPut, "k-lost", "x", 200*time.Millisecond); !errors.Is(err, ErrProposeTimeout) && !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Propose on isolated leader = %v; want a timeout or not-leader", err)
	}

	newLeader := waitForLeader(t, others, 2*time.Second)
	for i := 0; i < 3; i++ {
		if err := newLeader.Propose(store.OpPut, "k-new", "y", time.Second); err != nil {
			t.Fatalf("Propose k-new on new leader: %v", err)
		}
	}

	net.HealAll()

	waitForApplied(t, oldStore, "k-new", "y", 3*time.Second)
	waitForApplied(t, oldStore, "k-base", "b", time.Second)

	// Logs must converge to the new leader's, with k-lost gone everywhere.
	deadline := time.Now().Add(3 * time.Second)
	for {
		if old.LastLogIndex() == newLeader.LastLogIndex() && old.CommitIndex() == newLeader.CommitIndex() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("old leader did not converge: lastLog %d vs %d, commit %d vs %d",
				old.LastLogIndex(), newLeader.LastLogIndex(), old.CommitIndex(), newLeader.CommitIndex())
		}
		time.Sleep(10 * time.Millisecond)
	}
	for i, s := range stores {
		if _, ok := s.Get("k-lost"); ok {
			t.Fatalf("node%d applied k-lost, a write that was never committed", i+1)
		}
	}
	old.mu.Lock()
	defer old.mu.Unlock()
	for _, e := range old.entries {
		if e.Key == "k-lost" {
			t.Fatalf("old leader still has uncommitted k-lost in its log at index %d", e.Index)
		}
	}
}

// TestNewLeaderServesReadsOnlyAfterNoopCommits checks the read-safety rule:
// a leader is "ready" only once the no-op entry from its own term commits.
func TestNewLeaderServesReadsOnlyAfterNoopCommits(t *testing.T) {
	nodes, _, _ := newTestCluster(t, 3)
	leader := waitForLeader(t, nodes, 2*time.Second)
	deadline := time.Now().Add(time.Second)
	for !leader.IsReadyLeader() {
		if time.Now().After(deadline) {
			t.Fatal("leader never became ready (no-op entry not committed)")
		}
		time.Sleep(5 * time.Millisecond)
	}
	for _, n := range nodes {
		if n != leader && n.IsReadyLeader() {
			t.Fatalf("follower %s reports ready-leader", n.ID())
		}
	}
}
