package store

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentMixedOps hammers a single shared Store with many goroutines
// doing randomized, interleaved Get/Put/Delete for a fixed duration. It
// exists to be run under `go test -race`: any missing lock around the map
// or the WAL would show up as a data race here, not as a wrong answer (the
// race detector instruments memory accesses, so it doesn't depend on
// actually observing a corrupted value).
//
// Run it with:
//
//	go test ./internal/store/... -race -v -run TestConcurrentMixedOps
func TestConcurrentMixedOps(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping race hammer test in -short mode")
	}

	path := filepath.Join(t.TempDir(), "concurrent.wal")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	const (
		numGoroutines = 100
		numKeys       = 20
		duration      = 500 * time.Millisecond
	)

	keys := make([]string, numKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%d", i)
	}

	var puts, gets, deletes, hits, misses int64

	deadline := time.Now().Add(duration)
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	start := time.Now()
	for g := 0; g < numGoroutines; g++ {
		go func(seed int64) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(seed))
			for time.Now().Before(deadline) {
				key := keys[rnd.Intn(numKeys)]
				switch rnd.Intn(3) {
				case 0:
					value := fmt.Sprintf("v-%d-%d", seed, rnd.Int())
					if err := s.Put(key, value); err != nil {
						t.Errorf("Put(%s): %v", key, err)
						return
					}
					atomic.AddInt64(&puts, 1)
				case 1:
					if _, ok := s.Get(key); ok {
						atomic.AddInt64(&hits, 1)
					} else {
						atomic.AddInt64(&misses, 1)
					}
					atomic.AddInt64(&gets, 1)
				case 2:
					if err := s.Delete(key); err != nil {
						t.Errorf("Delete(%s): %v", key, err)
						return
					}
					atomic.AddInt64(&deletes, 1)
				}
			}
		}(int64(g))
	}
	wg.Wait()
	elapsed := time.Since(start)

	total := puts + gets + deletes
	t.Logf("")
	t.Logf("=== concurrency hammer summary ===")
	t.Logf("goroutines:      %d", numGoroutines)
	t.Logf("distinct keys:   %d", numKeys)
	t.Logf("elapsed:         %s", elapsed)
	t.Logf("puts:            %d", puts)
	t.Logf("gets:            %d  (hits=%d misses=%d)", gets, hits, misses)
	t.Logf("deletes:         %d", deletes)
	t.Logf("total ops:       %d  (%.0f ops/sec)", total, float64(total)/elapsed.Seconds())
	t.Logf("final key count: %d", s.Len())
	t.Logf("===================================")

	if total == 0 {
		t.Fatalf("no operations completed — test harness issue")
	}
}
