package store

import (
	"path/filepath"
	"testing"
)

// TestWALRewriteReplacesLog checks that Rewrite swaps the log for exactly
// the given records, keeps the WAL usable for appends, and survives reopen.
func TestWALRewriteReplacesLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "n.wal")
	w, _, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 5; i++ {
		if err := w.AppendRecord(Record{Index: i, Term: 1, Op: OpPut, Key: "k", Value: "v"}); err != nil {
			t.Fatal(err)
		}
	}
	keep := []Record{{Index: 1, Term: 1, Op: OpPut, Key: "a", Value: "1"}, {Index: 2, Term: 1, Op: OpPut, Key: "b", Value: "2"}}
	if err := w.Rewrite(keep); err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	if w.LastIndex() != 2 {
		t.Fatalf("LastIndex after rewrite = %d; want 2", w.LastIndex())
	}
	if err := w.AppendRecord(Record{Index: 3, Term: 2, Op: OpPut, Key: "c", Value: "3"}); err != nil {
		t.Fatalf("append after rewrite: %v", err)
	}
	w.Close()

	_, got, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].Key != "a" || got[1].Key != "b" || got[2].Key != "c" || got[2].Term != 2 {
		t.Fatalf("records after reopen = %+v; want a, b, c(term 2)", got)
	}
}
