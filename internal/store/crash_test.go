package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestCrashRecovery_TornWrite simulates a hard kill mid-write: a length
// prefix is on disk claiming N bytes of payload follow, but only a handful
// of bytes actually made it out before the process died — exactly what an
// interrupted os.File.Write leaves behind. Replay must recover every valid
// record before the tear and discard the torn tail, without erroring.
func TestCrashRecovery_TornWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")

	w, _, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	const validCount = 5
	for i := 0; i < validCount; i++ {
		if _, err := w.Append(OpPut, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open for fault injection: %v", err)
	}
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, 100) // claims 100 bytes of payload follow
	if _, err := f.Write(lenBuf); err != nil {
		t.Fatalf("write length prefix: %v", err)
	}
	if _, err := f.Write([]byte(`{"partial`)); err != nil { // far short of 100 bytes
		t.Fatalf("write partial payload: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close after fault injection: %v", err)
	}

	w2, records, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL after simulated crash: %v", err)
	}

	if len(records) != validCount {
		t.Fatalf("replayed %d records; want %d (torn tail should be discarded, not error)", len(records), validCount)
	}
	for i, rec := range records {
		if want := fmt.Sprintf("k%d", i); rec.Key != want {
			t.Fatalf("record[%d].Key = %q; want %q", i, rec.Key, want)
		}
	}

	// Store must remain fully usable after recovery: new appends continue
	// from the truncation point, not from the torn record's claimed length.
	rec, err := w2.Append(OpPut, "after-crash", "ok")
	if err != nil {
		t.Fatalf("Append after recovery: %v", err)
	}
	if want := uint64(validCount + 1); rec.Index != want {
		t.Fatalf("new record Index = %d; want %d", rec.Index, want)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Re-replay to confirm the truncation was actually persisted to disk
	// (not just skipped in memory) and the post-recovery write is durable.
	w3, records2, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL final replay: %v", err)
	}
	defer w3.Close() // Windows won't let TempDir clean up an open handle
	if want := validCount + 1; len(records2) != want {
		t.Fatalf("final replay = %d records; want %d", len(records2), want)
	}
}

// TestCrashRecovery_CorruptedRecord flips a byte inside an otherwise
// complete, correctly-length-prefixed final record. Length-based parsing
// succeeds and the JSON is valid, so only the CRC32 check catches this —
// proving the checksum does real work beyond just detecting truncation.
func TestCrashRecovery_CorruptedRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")

	w, _, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	const validCount = 3
	for i := 0; i < validCount; i++ {
		if _, err := w.Append(OpPut, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if _, err := w.Append(OpPut, "willbecorrupted", "sentinelvalue"); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	marker := []byte("sentinelvalue")
	idx := bytes.Index(raw, marker)
	if idx < 0 {
		t.Fatalf("sentinel value not found in WAL file")
	}
	// Toggle the case of a letter mid-string: same length, still valid
	// JSON, but the record no longer matches its stored CRC32.
	raw[idx+4] ^= 0x20
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("WriteFile corrupted copy: %v", err)
	}

	w2, records, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL after corruption: %v", err)
	}
	defer w2.Close()

	if len(records) != validCount {
		t.Fatalf("replayed %d records; want %d (corrupted final record must be discarded)", len(records), validCount)
	}
}
