package store

import "sync"

// Store is an in-memory key-value map backed by a WAL for durability. All
// access goes through mu: writers take the write lock for the whole
// WAL-append-then-mutate sequence (so a write is never visible in memory
// before it is durable on disk), readers take the read lock.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
	wal  *WAL
}

// Open opens (or creates) the WAL at path, replays it to rebuild in-memory
// state, and returns a ready-to-use Store.
func Open(path string) (*Store, error) {
	wal, records, err := OpenWAL(path)
	if err != nil {
		return nil, err
	}
	s := &Store{data: make(map[string]string, len(records)), wal: wal}
	for _, rec := range records {
		switch rec.Op {
		case OpPut:
			s.data[rec.Key] = rec.Value
		case OpDelete:
			delete(s.data, rec.Key)
		}
	}
	return s, nil
}

// Get returns the value for key and whether it was present.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// Put durably appends a PUT record to the WAL, then applies it to the
// in-memory map. Returns once the write is fsynced.
func (s *Store) Put(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.wal.Append(OpPut, key, value); err != nil {
		return err
	}
	s.data[key] = value
	return nil
}

// Delete durably appends a DELETE record to the WAL, then removes key from
// the in-memory map. Returns once the write is fsynced.
func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.wal.Append(OpDelete, key, ""); err != nil {
		return err
	}
	delete(s.data, key)
	return nil
}

// Len returns the number of keys currently in the store.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

func (s *Store) Close() error {
	return s.wal.Close()
}
