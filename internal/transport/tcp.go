package transport

import (
	"fmt"
	"net"
	"time"
)

// DefaultRPCTimeout bounds dial+write+read for a single Call, so a dead or
// unreachable peer fails fast instead of hanging. See the project plan for
// why this is inflated versus the Raft paper's typical values (Windows
// scheduling/logging overhead margin during a live demo).
const DefaultRPCTimeout = 200 * time.Millisecond

// Server accepts TCP connections and dispatches each framed request to
// Handler. Each RPC is a single request/response over a fresh, short-lived
// connection (dial → write request → read response → close) — no
// connection pooling or multiplexing, which keeps the implementation and
// its failure modes easy to reason about at the cost of one new connection
// per RPC (fine at a 100ms heartbeat interval on localhost).
type Server struct {
	listener net.Listener
	handler  Handler
}

// Listen starts a Server on addr (use "127.0.0.1:0" for an ephemeral port
// in tests) and begins accepting connections in the background.
func Listen(addr string, handler Handler) (*Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("transport: listen %s: %w", addr, err)
	}
	s := &Server{listener: ln, handler: handler}
	go s.acceptLoop()
	return s, nil
}

// Addr returns the address the server is actually listening on.
func (s *Server) Addr() string { return s.listener.Addr().String() }

func (s *Server) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return // listener closed
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	if err := conn.SetReadDeadline(time.Now().Add(DefaultRPCTimeout)); err != nil {
		return
	}
	req, err := ReadMessage(conn)
	if err != nil {
		return // peer disappeared mid-request or sent garbage — nothing to reply to
	}

	resp, err := s.handler(req)
	if err != nil {
		return
	}

	if err := conn.SetWriteDeadline(time.Now().Add(DefaultRPCTimeout)); err != nil {
		return
	}
	_ = WriteMessage(conn, resp)
}

// Close stops accepting new connections. In-flight connections finish on
// their own (bounded by DefaultRPCTimeout).
func (s *Server) Close() error {
	return s.listener.Close()
}

// Call dials addr, sends req, and returns the peer's response. Every step
// (dial, write, read) is bounded by timeout.
func Call(addr string, req Message, timeout time.Duration) (Message, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return Message{}, fmt.Errorf("transport: dial %s: %w", addr, err)
	}
	defer conn.Close()

	if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return Message{}, err
	}
	if err := WriteMessage(conn, req); err != nil {
		return Message{}, err
	}

	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return Message{}, err
	}
	resp, err := ReadMessage(conn)
	if err != nil {
		return Message{}, fmt.Errorf("transport: read response from %s: %w", addr, err)
	}
	return resp, nil
}

// TCPTransport is the real, over-the-network Transport implementation.
type TCPTransport struct {
	// Timeout bounds each Call; DefaultRPCTimeout is used if zero.
	Timeout time.Duration
}

func (t TCPTransport) Send(addr string, req Message) (Message, error) {
	timeout := t.Timeout
	if timeout == 0 {
		timeout = DefaultRPCTimeout
	}
	return Call(addr, req, timeout)
}
