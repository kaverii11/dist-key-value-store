package transport

import (
	"bytes"
	"encoding/json"
	"net"
	"testing"
	"time"
)

func TestWriteReadMessageRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	want := Message{Type: MsgRequestVoteReq, ID: "abc123", Payload: json.RawMessage(`{"term":5}`)}

	if err := WriteMessage(&buf, want); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
	got, err := ReadMessage(&buf)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if got.Type != want.Type || got.ID != want.ID || string(got.Payload) != string(want.Payload) {
		t.Fatalf("round trip mismatch: got %+v, want %+v", got, want)
	}
}

func TestServerCallRoundTrip(t *testing.T) {
	handler := func(req Message) (Message, error) {
		return Message{Type: "pong", ID: req.ID}, nil
	}
	srv, err := Listen("127.0.0.1:0", handler)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer srv.Close()

	resp, err := Call(srv.Addr(), Message{Type: "ping", ID: "42"}, time.Second)
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if resp.Type != "pong" || resp.ID != "42" {
		t.Fatalf("resp = %+v; want Type=pong ID=42", resp)
	}
}

// TestPeerDisappearsMidConnection simulates a peer that dies right after
// accepting a connection, before reading or writing anything. Call must
// return an error promptly (via the TCP-level close), not hang for the
// full RPC timeout.
func TestPeerDisappearsMidConnection(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		conn.Close() // dies immediately, no read or write
	}()

	const timeout = 500 * time.Millisecond
	start := time.Now()
	_, err = Call(ln.Addr().String(), Message{Type: "ping"}, timeout)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("Call succeeded against a peer that closed immediately; want an error")
	}
	if elapsed > timeout {
		t.Fatalf("Call took %s to fail; should fail fast on connection close, not wait out the timeout", elapsed)
	}
}

// TestPeerHangsWithoutResponding covers the other disappearance mode: the
// peer accepts and reads the request but never replies (e.g. wedged, or a
// partition that only breaks the return path). Call must give up at the
// deadline, not hang forever.
func TestPeerHangsWithoutResponding(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		ReadMessage(conn)          // read the request...
		time.Sleep(2 * time.Second) // ...then just sit there
	}()

	const timeout = 150 * time.Millisecond
	start := time.Now()
	_, err = Call(ln.Addr().String(), Message{Type: "ping"}, timeout)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("Call succeeded against a peer that never responds; want a timeout error")
	}
	if elapsed > timeout+300*time.Millisecond {
		t.Fatalf("Call took %s to time out; want close to the %s deadline", elapsed, timeout)
	}
}

func TestFakeNetworkCutHeal(t *testing.T) {
	fakeNet := NewFakeNetwork()
	fakeNet.Register("nodeA", func(req Message) (Message, error) {
		return Message{Type: "ack"}, nil
	})
	fromB := fakeNet.Transport("nodeB")

	if _, err := fromB.Send("nodeA", Message{Type: "ping"}); err != nil {
		t.Fatalf("Send before cut: %v", err)
	}

	fakeNet.Cut("nodeB", "nodeA")
	if _, err := fromB.Send("nodeA", Message{Type: "ping"}); err == nil {
		t.Fatalf("Send after Cut succeeded; want error")
	}

	fakeNet.Heal("nodeB", "nodeA")
	if _, err := fromB.Send("nodeA", Message{Type: "ping"}); err != nil {
		t.Fatalf("Send after Heal: %v", err)
	}
}

func TestFakeNetworkCutIsDirectional(t *testing.T) {
	fakeNet := NewFakeNetwork()
	fakeNet.Register("nodeA", func(req Message) (Message, error) { return Message{Type: "ack"}, nil })
	fakeNet.Register("nodeB", func(req Message) (Message, error) { return Message{Type: "ack"}, nil })

	fakeNet.Cut("nodeB", "nodeA") // B -> A broken, A -> B still fine

	if _, err := fakeNet.Transport("nodeB").Send("nodeA", Message{Type: "ping"}); err == nil {
		t.Fatalf("B->A Send succeeded after cutting that direction; want error")
	}
	if _, err := fakeNet.Transport("nodeA").Send("nodeB", Message{Type: "ping"}); err != nil {
		t.Fatalf("A->B Send failed even though only B->A was cut: %v", err)
	}
}
