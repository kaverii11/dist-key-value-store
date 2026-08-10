// Package config loads the static cluster peer list (node id -> addresses).
// Peer discovery is deliberately static for this project — no gossip or
// discovery protocol — so a cluster is just a JSON file every node reads.
package config

import (
	"encoding/json"
	"fmt"
	"os"
)

// NodeConfig describes one cluster member. ClusterAddr carries Raft RPCs
// (RequestVote/AppendEntries); ClientAddr carries the Get/Put/Delete API.
// Kept as two ports (see project plan's Design Decisions) so demo logs
// aren't a mix of election chatter and client traffic.
type NodeConfig struct {
	ID          string `json:"id"`
	ClusterAddr string `json:"clusterAddr"`
	ClientAddr  string `json:"clientAddr"`
}

type ClusterConfig struct {
	Nodes []NodeConfig `json:"nodes"`
}

// Load reads and parses a cluster config file.
func Load(path string) (ClusterConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return ClusterConfig{}, fmt.Errorf("config: read %s: %w", path, err)
	}
	var c ClusterConfig
	if err := json.Unmarshal(data, &c); err != nil {
		return ClusterConfig{}, fmt.Errorf("config: parse %s: %w", path, err)
	}
	if len(c.Nodes) == 0 {
		return ClusterConfig{}, fmt.Errorf("config: %s defines no nodes", path)
	}
	return c, nil
}

// Node looks up a single node's config by ID.
func (c ClusterConfig) Node(id string) (NodeConfig, bool) {
	for _, n := range c.Nodes {
		if n.ID == id {
			return n, true
		}
	}
	return NodeConfig{}, false
}

// PeerClusterAddrs returns id -> clusterAddr for every node except selfID —
// the map a raft.Node needs to know who to send RequestVote/AppendEntries to.
func (c ClusterConfig) PeerClusterAddrs(selfID string) map[string]string {
	peers := make(map[string]string, len(c.Nodes)-1)
	for _, n := range c.Nodes {
		if n.ID != selfID {
			peers[n.ID] = n.ClusterAddr
		}
	}
	return peers
}

// ClientAddrs returns id -> clientAddr for every node, including self —
// used to resolve a LeaderHint to an address a client can redirect to.
func (c ClusterConfig) ClientAddrs() map[string]string {
	m := make(map[string]string, len(c.Nodes))
	for _, n := range c.Nodes {
		m[n.ID] = n.ClientAddr
	}
	return m
}
