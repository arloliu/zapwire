package zapwire

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// rawEncoder passes the record through unchanged.
type rawEncoder struct{}

func (rawEncoder) Encode(dst, record []byte) ([]byte, error) { return append(dst, record...), nil }

// errEncoder always fails (to exercise the encode-error path).
type errEncoder struct{}

func (errEncoder) Encode([]byte, []byte) ([]byte, error) { return nil, fmt.Errorf("boom") }

// lineFramer joins payloads each terminated by '\n' (newline framing).
type lineFramer struct{}

func (lineFramer) Frame(dst []byte, payloads [][]byte) ([]byte, error) {
	for _, p := range payloads {
		dst = append(dst, p...)
		dst = append(dst, '\n')
	}

	return dst, nil
}

func randomSocketPath(t *testing.T) string {
	t.Helper()

	return filepath.Join(os.TempDir(), fmt.Sprintf("zapwire_%d.sock", time.Now().UnixNano()))
}

// readServer accepts one UDS connection and streams everything it reads to recv.
type readServer struct {
	ln   net.Listener
	recv chan []byte
	path string
}

func startReadServer(t *testing.T, path string) *readServer {
	t.Helper()
	ln, err := net.Listen("unix", path)
	require.NoError(t, err)
	s := &readServer{ln: ln, recv: make(chan []byte, 64), path: path}
	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 4096)
		for {
			n, rerr := conn.Read(buf)
			if n > 0 {
				b := make([]byte, n)
				copy(b, buf[:n])
				s.recv <- b
			}
			if rerr != nil {
				return
			}
		}
	}()

	return s
}

func (s *readServer) stop() { _ = s.ln.Close(); _ = os.Remove(s.path) }

// deafServer accepts one UDS connection and then never reads from it, so a writer's socket
// buffer fills and conn.Write stalls until its deadline. Used to prove Close returns within a
// bounded time while the flush goroutine is mid-write to a non-reading peer.
type deafServer struct {
	ln        net.Listener
	path      string
	mu        sync.Mutex
	conn      net.Conn
	connected chan struct{}
}

func startDeafServer(t *testing.T, path string) *deafServer {
	t.Helper()
	ln, err := net.Listen("unix", path)
	require.NoError(t, err)
	s := &deafServer{ln: ln, path: path, connected: make(chan struct{})}
	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		s.mu.Lock()
		s.conn = conn
		s.mu.Unlock()
		close(s.connected)
		// Deliberately never read: hold the conn so the peer's writes stall.
	}()

	return s
}

func (s *deafServer) waitConnected(t *testing.T) {
	t.Helper()
	select {
	case <-s.connected:
	case <-time.After(time.Second):
		t.Fatal("deafServer: timed out waiting for connection")
	}
}

func (s *deafServer) stop() {
	_ = s.ln.Close()
	s.mu.Lock()
	if s.conn != nil {
		_ = s.conn.Close()
	}
	s.mu.Unlock()
	_ = os.Remove(s.path)
}

// rawUDSServer can sever the live connection to drive reconnect tests.
type rawUDSServer struct {
	t         *testing.T
	path      string
	ln        net.Listener
	mu        sync.Mutex
	conns     []net.Conn
	connected chan struct{}
}

func startRawUDSServer(t *testing.T, path string) *rawUDSServer {
	t.Helper()
	ln, err := net.Listen("unix", path)
	require.NoError(t, err)
	s := &rawUDSServer{t: t, path: path, ln: ln, connected: make(chan struct{})}
	go func() {
		first := true
		for {
			conn, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			s.mu.Lock()
			s.conns = append(s.conns, conn)
			s.mu.Unlock()
			if first {
				first = false
				close(s.connected)
			}
			go func(c net.Conn) {
				b := make([]byte, 4096)
				for {
					if _, rerr := c.Read(b); rerr != nil {
						return
					}
				}
			}(conn)
		}
	}()

	return s
}

func (s *rawUDSServer) waitConnected() {
	select {
	case <-s.connected:
	case <-time.After(time.Second):
		s.t.Fatal("rawUDSServer: timed out waiting for first connection")
	}
}

func (s *rawUDSServer) close() {
	_ = s.ln.Close()
	s.mu.Lock()
	for _, c := range s.conns {
		_ = c.Close()
	}
	s.conns = nil
	s.mu.Unlock()
	_ = os.Remove(s.path)
}
