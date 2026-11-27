// Package api implements the client-facing Get/Put/Delete protocol,
// served on each node's client port (separate from the cluster port used
// for Raft RPCs — see the project plan's Design Decisions). Writes (and,
// in this project's simplified model, reads too — see the plan's read-
// consistency tradeoff) are only accepted on the leader; a non-leader
// replies with an error and a LeaderHint so a client can redirect.
package api

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/kaverii11/dist-key-value-store/internal/raft"
	"github.com/kaverii11/dist-key-value-store/internal/store"
	"github.com/kaverii11/dist-key-value-store/internal/transport"
)

type ClientPutReq struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type ClientGetReq struct {
	Key string `json:"key"`
}

type ClientDeleteReq struct {
	Key string `json:"key"`
}

type ClientResp struct {
	OK         bool   `json:"ok"`
	Value      string `json:"value,omitempty"`
	Error      string `json:"error,omitempty"`
	LeaderHint string `json:"leaderHint,omitempty"`
}

// Server handles client requests for one node.
type Server struct {
	node           *raft.Node
	fsm            *store.Store
	clientAddrs    map[string]string // node ID -> client address, for LeaderHint
	proposeTimeout time.Duration
}

// NewServer builds a Server. clientAddrs must map every node ID in the
// cluster (including this one) to its client address.
func NewServer(node *raft.Node, fsm *store.Store, clientAddrs map[string]string) *Server {
	return &Server{node: node, fsm: fsm, clientAddrs: clientAddrs, proposeTimeout: 2 * time.Second}
}

// Put proposes a write through raft. Returns raft.ErrNotLeader (check with
// errors.Is) if this node isn't leader.
func (s *Server) Put(key, value string) error {
	if !s.node.IsLeader() {
		return raft.ErrNotLeader
	}
	return s.node.Propose(store.OpPut, key, value, s.proposeTimeout)
}

// Delete proposes a delete through raft. Returns raft.ErrNotLeader if this
// node isn't leader.
func (s *Server) Delete(key string) error {
	if !s.node.IsLeader() {
		return raft.ErrNotLeader
	}
	return s.node.Propose(store.OpDelete, key, "", s.proposeTimeout)
}

// Get reads from the leader's local state. Returns raft.ErrNotLeader if
// this node isn't leader (see the project plan: reads, like writes, only
// go to the leader in this project's simplified consistency model).
func (s *Server) Get(key string) (value string, found bool, err error) {
	if !s.node.IsLeader() {
		return "", false, raft.ErrNotLeader
	}
	// A brand-new leader may not have applied every committed entry yet;
	// wait (briefly) until its no-op entry commits before answering.
	deadline := time.Now().Add(s.proposeTimeout)
	for !s.node.IsReadyLeader() {
		if !s.node.IsLeader() {
			return "", false, raft.ErrNotLeader
		}
		if time.Now().After(deadline) {
			return "", false, raft.ErrProposeTimeout
		}
		time.Sleep(5 * time.Millisecond)
	}
	v, ok := s.fsm.Get(key)
	return v, ok, nil
}

func (s *Server) notLeaderResp() ClientResp {
	return ClientResp{OK: false, Error: "not leader", LeaderHint: s.node.LeaderAddr(s.clientAddrs)}
}

// Handle is the transport.Handler for this node's client-port server.
func (s *Server) Handle(msg transport.Message) (transport.Message, error) {
	switch msg.Type {
	case transport.MsgClientPutReq:
		var req ClientPutReq
		if err := json.Unmarshal(msg.Payload, &req); err != nil {
			return transport.Message{}, err
		}
		return s.reply(msg, s.handlePut(req))

	case transport.MsgClientGetReq:
		var req ClientGetReq
		if err := json.Unmarshal(msg.Payload, &req); err != nil {
			return transport.Message{}, err
		}
		return s.reply(msg, s.handleGet(req))

	case transport.MsgClientDeleteReq:
		var req ClientDeleteReq
		if err := json.Unmarshal(msg.Payload, &req); err != nil {
			return transport.Message{}, err
		}
		return s.reply(msg, s.handleDelete(req))

	default:
		return transport.Message{}, fmt.Errorf("api: unknown message type %q", msg.Type)
	}
}

func (s *Server) reply(req transport.Message, resp ClientResp) (transport.Message, error) {
	payload, err := json.Marshal(resp)
	if err != nil {
		return transport.Message{}, err
	}
	return transport.Message{Type: transport.MsgClientResp, ID: req.ID, Payload: payload}, nil
}

func (s *Server) handlePut(req ClientPutReq) ClientResp {
	if err := s.Put(req.Key, req.Value); err != nil {
		if err == raft.ErrNotLeader {
			return s.notLeaderResp()
		}
		return ClientResp{OK: false, Error: err.Error()}
	}
	return ClientResp{OK: true}
}

func (s *Server) handleDelete(req ClientDeleteReq) ClientResp {
	if err := s.Delete(req.Key); err != nil {
		if err == raft.ErrNotLeader {
			return s.notLeaderResp()
		}
		return ClientResp{OK: false, Error: err.Error()}
	}
	return ClientResp{OK: true}
}

func (s *Server) handleGet(req ClientGetReq) ClientResp {
	v, ok, err := s.Get(req.Key)
	if err != nil {
		if err == raft.ErrNotLeader {
			return s.notLeaderResp()
		}
		return ClientResp{OK: false, Error: err.Error()}
	}
	if !ok {
		return ClientResp{OK: false, Error: "key not found"}
	}
	return ClientResp{OK: true, Value: v}
}
