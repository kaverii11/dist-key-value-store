package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// StateStore persists currentTerm and votedFor across restarts, so a node
// that crashes and comes back never votes twice in a term it already
// participated in — without this, a restarted node would default to
// votedFor="" and could grant a second, conflicting vote in the same term.
//
// Writes are atomic: write to a temp file, then os.Rename over the real
// path. On Windows, os.Rename replaces the destination (MoveFileEx with
// MOVEFILE_REPLACE_EXISTING under the hood), so a reader never observes a
// half-written file.
type StateStore struct {
	path string
}

func NewStateStore(path string) *StateStore {
	return &StateStore{path: path}
}

type persistedState struct {
	CurrentTerm uint64 `json:"currentTerm"`
	VotedFor    string `json:"votedFor"`
}

// Load returns the persisted (currentTerm, votedFor), or (0, "", nil) if
// nothing has been persisted yet (a brand-new node).
func (s *StateStore) Load() (currentTerm uint64, votedFor string, err error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, "", nil
	}
	if err != nil {
		return 0, "", fmt.Errorf("raft: read state file %s: %w", s.path, err)
	}
	var ps persistedState
	if err := json.Unmarshal(data, &ps); err != nil {
		return 0, "", fmt.Errorf("raft: parse state file %s: %w", s.path, err)
	}
	return ps.CurrentTerm, ps.VotedFor, nil
}

func (s *StateStore) Save(currentTerm uint64, votedFor string) error {
	data, err := json.Marshal(persistedState{CurrentTerm: currentTerm, VotedFor: votedFor})
	if err != nil {
		return fmt.Errorf("raft: marshal state: %w", err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("raft: write temp state file: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("raft: rename temp state file: %w", err)
	}
	return nil
}
