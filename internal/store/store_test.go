package store

import (
	"path/filepath"
	"testing"
)

func TestPutGetDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	if _, ok := s.Get("missing"); ok {
		t.Fatalf("Get(missing) returned ok=true, want false")
	}

	if err := s.Put("k1", "v1"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if v, ok := s.Get("k1"); !ok || v != "v1" {
		t.Fatalf("Get(k1) = %q, %v; want v1, true", v, ok)
	}

	if err := s.Put("k1", "v2"); err != nil {
		t.Fatalf("Put overwrite: %v", err)
	}
	if v, _ := s.Get("k1"); v != "v2" {
		t.Fatalf("Get(k1) after overwrite = %q; want v2", v)
	}

	if err := s.Delete("k1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok := s.Get("k1"); ok {
		t.Fatalf("Get(k1) after delete returned ok=true, want false")
	}
}

func TestReplayAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")

	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	writes := map[string]string{"alpha": "1", "beta": "2", "gamma": "3"}
	for k, v := range writes {
		if err := s.Put(k, v); err != nil {
			t.Fatalf("Put(%s): %v", k, err)
		}
	}
	if err := s.Delete("beta"); err != nil {
		t.Fatalf("Delete(beta): %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen — simulates a clean restart. Replay must reproduce exact state.
	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	if v, ok := s2.Get("alpha"); !ok || v != "1" {
		t.Fatalf("Get(alpha) after replay = %q, %v; want 1, true", v, ok)
	}
	if v, ok := s2.Get("gamma"); !ok || v != "3" {
		t.Fatalf("Get(gamma) after replay = %q, %v; want 3, true", v, ok)
	}
	if _, ok := s2.Get("beta"); ok {
		t.Fatalf("Get(beta) after replay returned ok=true; want deleted")
	}
	if got, want := s2.Len(), 2; got != want {
		t.Fatalf("Len() after replay = %d; want %d", got, want)
	}

	// New writes after replay must continue from the correct WAL offset.
	if err := s2.Put("delta", "4"); err != nil {
		t.Fatalf("Put(delta) after replay: %v", err)
	}
	if v, ok := s2.Get("delta"); !ok || v != "4" {
		t.Fatalf("Get(delta) = %q, %v; want 4, true", v, ok)
	}
}
