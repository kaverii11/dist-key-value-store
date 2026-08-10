// Package transport implements node-to-node and client-to-node
// communication: a length-prefixed JSON wire protocol over plain TCP (no
// gRPC/protobuf — see the project README for why), plus a Transport
// interface with both a real TCP implementation and an in-memory fake for
// deterministic tests.
package transport

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

// maxMessageSize guards against a garbled or hostile length prefix causing
// an unbounded allocation.
const maxMessageSize = 16 * 1024 * 1024

// Message type constants. Defined here (not in the raft/api packages that
// use them) so both can reference the shared vocabulary without importing
// each other.
const (
	MsgRequestVoteReq    = "RequestVoteReq"
	MsgRequestVoteResp   = "RequestVoteResp"
	MsgAppendEntriesReq  = "AppendEntriesReq"
	MsgAppendEntriesResp = "AppendEntriesResp"
	MsgClientPutReq      = "ClientPutReq"
	MsgClientGetReq      = "ClientGetReq"
	MsgClientDeleteReq   = "ClientDeleteReq"
	MsgClientResp        = "ClientResp"
)

// Message is the envelope for every request and response on the wire.
type Message struct {
	Type    string          `json:"type"`
	ID      string          `json:"id"`
	Payload json.RawMessage `json:"payload"`
}

// WriteMessage frames and writes msg: [4-byte big-endian length][JSON payload].
func WriteMessage(w io.Writer, msg Message) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("transport: marshal message: %w", err)
	}
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(payload)))
	if _, err := w.Write(lenBuf); err != nil {
		return fmt.Errorf("transport: write length prefix: %w", err)
	}
	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("transport: write payload: %w", err)
	}
	return nil
}

// ReadMessage reads one framed message from r. A clean close before any
// bytes are read surfaces as io.EOF; a close mid-message surfaces as
// io.ErrUnexpectedEOF — callers can distinguish "peer was never there" from
// "peer disappeared partway through".
func ReadMessage(r io.Reader) (Message, error) {
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		return Message{}, err
	}
	length := binary.BigEndian.Uint32(lenBuf)
	if length > maxMessageSize {
		return Message{}, fmt.Errorf("transport: message too large (%d bytes)", length)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return Message{}, fmt.Errorf("transport: read payload: %w", err)
	}
	var msg Message
	if err := json.Unmarshal(payload, &msg); err != nil {
		return Message{}, fmt.Errorf("transport: unmarshal message: %w", err)
	}
	return msg, nil
}

// Handler processes one request Message and returns the response to send
// back. An error means "don't respond, just close the connection" — used
// when the request itself was unreadable/invalid.
type Handler func(Message) (Message, error)

// Transport is what the raft and api packages depend on, so tests can
// substitute FakeNetwork instead of real sockets. addr identifies the peer
// to send to (a "host:port" for TCPTransport, an arbitrary node ID string
// for FakeNetwork).
type Transport interface {
	Send(addr string, req Message) (Message, error)
}
