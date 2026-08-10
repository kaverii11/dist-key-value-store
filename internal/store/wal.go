// Package store implements the storage engine: an in-memory map backed by a
// write-ahead log (WAL) for durability.
package store

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"sync"
)

const (
	OpPut    = "PUT"
	OpDelete = "DELETE"
)

// Record is a single WAL entry. Index/Term double as the Raft log
// index/term once the raft package drives writes (see internal/raft); a
// standalone Store not under Raft leaves Term at 0.
type Record struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
	CRC32 uint32 `json:"crc32"`
}

func checksum(r Record) uint32 {
	preimage := fmt.Sprintf("%d|%d|%s|%s|%s", r.Index, r.Term, r.Op, r.Key, r.Value)
	return crc32.ChecksumIEEE([]byte(preimage))
}

// WAL is an append-only, length-prefixed JSON log on disk. Each Append
// fsyncs before returning, so a caller that has received a nil error knows
// the record survives a hard process kill.
//
// On-disk framing per record: [4-byte big-endian uint32 length][JSON bytes].
type WAL struct {
	mu        sync.Mutex
	file      *os.File
	lastIndex uint64
	lastTerm  uint64
}

// OpenWAL opens (creating if necessary) the WAL at path, replays any valid
// records, and returns them along with a WAL ready for further Appends.
//
// If the file ends in a torn write (a record cut off mid-write, e.g. by a
// hard kill) or a corrupted record (checksum mismatch), replay stops at
// that record, the file is truncated to the last known-good byte offset,
// and a warning is logged — startup proceeds normally with whatever was
// validly replayed. Because the WAL is strictly append-only, a bad record
// can only ever be the last one, so "stop at the first bad record" and
// "discard the corrupted tail" are the same operation.
func OpenWAL(path string) (*WAL, []Record, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("wal: open %s: %w", path, err)
	}

	records, validBytes, err := replay(f)
	if err != nil {
		f.Close()
		return nil, nil, err
	}

	if err := f.Truncate(validBytes); err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("wal: truncate %s: %w", path, err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("wal: seek %s: %w", path, err)
	}

	w := &WAL{file: f}
	if n := len(records); n > 0 {
		w.lastIndex = records[n-1].Index
		w.lastTerm = records[n-1].Term
	}
	return w, records, nil
}

// replay reads records sequentially from the start of f and returns the
// valid ones plus the byte offset immediately after the last valid record
// (i.e. where the file should be truncated to discard any torn/corrupt
// tail). It does not mutate f's position permanently in a way callers need
// to care about; the caller re-seeks afterward.
func replay(f *os.File) ([]Record, int64, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, 0, fmt.Errorf("wal: seek start: %w", err)
	}
	r := bufio.NewReader(f)

	var records []Record
	var offset int64

	for {
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(r, lenBuf); err != nil {
			if err == io.EOF {
				break // clean end of file
			}
			log.Printf("wal: torn record header at offset %d (%v); truncating tail", offset, err)
			break
		}
		length := binary.BigEndian.Uint32(lenBuf)

		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			log.Printf("wal: torn record payload at offset %d (%v); truncating tail", offset, err)
			break
		}

		var rec Record
		if err := json.Unmarshal(payload, &rec); err != nil {
			log.Printf("wal: corrupt record at offset %d (unmarshal: %v); truncating tail", offset, err)
			break
		}
		if got, want := checksum(rec), rec.CRC32; got != want {
			log.Printf("wal: checksum mismatch at offset %d (got %08x want %08x); truncating tail", offset, got, want)
			break
		}

		records = append(records, rec)
		offset += 4 + int64(length)
	}

	return records, offset, nil
}

// Append assigns the next sequential index (Term 0) and writes the record.
// Convenience wrapper around AppendRecord for a standalone Store not driven
// by Raft.
func (w *WAL) Append(op, key, value string) (Record, error) {
	w.mu.Lock()
	rec := Record{Index: w.lastIndex + 1, Term: 0, Op: op, Key: key, Value: value}
	w.mu.Unlock()
	if err := w.AppendRecord(rec); err != nil {
		return Record{}, err
	}
	return rec, nil
}

// AppendRecord writes a fully-formed record (Index/Term already assigned by
// the caller — e.g. a Raft leader assigning its own log index/term, or a
// follower writing exactly the index/term it was told to). CRC32 is
// (re)computed here, so callers never need to set it themselves.
func (w *WAL) AppendRecord(rec Record) error {
	rec.CRC32 = checksum(rec)

	payload, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("wal: marshal record: %w", err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(payload)))
	if _, err := w.file.Write(lenBuf); err != nil {
		return fmt.Errorf("wal: write length prefix: %w", err)
	}
	if _, err := w.file.Write(payload); err != nil {
		return fmt.Errorf("wal: write payload: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: fsync: %w", err)
	}

	if rec.Index > w.lastIndex {
		w.lastIndex = rec.Index
		w.lastTerm = rec.Term
	}
	return nil
}

// LastIndex returns the index of the most recently appended (or replayed)
// record, or 0 if the log is empty.
func (w *WAL) LastIndex() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastIndex
}

// LastTerm returns the term of the most recently appended (or replayed)
// record, or 0 if the log is empty.
func (w *WAL) LastTerm() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastTerm
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
