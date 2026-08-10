package transport

import (
	"fmt"
	"sync"
)

// FakeNetwork is an in-memory Transport implementation for deterministic
// tests: no real sockets, and Cut/Heal let a test simulate a network
// partition between specific nodes instantly, without OS-level firewall
// rules (which need admin rights on Windows and are slow/flaky). Real TCP
// is still exercised end-to-end via the live demo's taskkill.
type FakeNetwork struct {
	mu    sync.Mutex
	nodes map[string]Handler
	cut   map[[2]string]bool // (from, to) pairs currently partitioned; directional
}

func NewFakeNetwork() *FakeNetwork {
	return &FakeNetwork{
		nodes: make(map[string]Handler),
		cut:   make(map[[2]string]bool),
	}
}

// Register attaches a node's handler under addr so other nodes on this
// fake network can send to it.
func (n *FakeNetwork) Register(addr string, handler Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[addr] = handler
}

// Cut makes messages sent from `from` to `to` fail as if `to` were
// unreachable, until Heal is called. Directional, so asymmetric partitions
// (A can reach B, B can't reach A) can be simulated too.
func (n *FakeNetwork) Cut(from, to string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cut[[2]string{from, to}] = true
}

// Heal reverses a single Cut.
func (n *FakeNetwork) Heal(from, to string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.cut, [2]string{from, to})
}

// HealAll clears every partition on this network.
func (n *FakeNetwork) HealAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cut = make(map[[2]string]bool)
}

// Transport returns the Transport as seen by the node at selfAddr, so
// Cut/Heal directionality is applied from that node's perspective.
func (n *FakeNetwork) Transport(selfAddr string) Transport {
	return &fakeTransport{network: n, self: selfAddr}
}

type fakeTransport struct {
	network *FakeNetwork
	self    string
}

func (t *fakeTransport) Send(addr string, req Message) (Message, error) {
	t.network.mu.Lock()
	cut := t.network.cut[[2]string{t.self, addr}]
	handler, ok := t.network.nodes[addr]
	t.network.mu.Unlock()

	if cut {
		return Message{}, fmt.Errorf("transport(fake): %s unreachable from %s (partitioned)", addr, t.self)
	}
	if !ok {
		return Message{}, fmt.Errorf("transport(fake): %s not registered", addr)
	}
	return handler(req)
}
