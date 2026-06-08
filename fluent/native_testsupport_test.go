package fluent

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

func randomSocketPath(t testing.TB) string {
	t.Helper()

	return filepath.Join(os.TempDir(), fmt.Sprintf("zapwire_native_%d.sock", time.Now().UnixNano()))
}

// nativeReadServer accepts one UDS connection and streams each read to recv.
type nativeReadServer struct {
	ln     net.Listener
	recv   chan []byte
	path   string
	mu     sync.Mutex
	conn   net.Conn
	closed bool
}

func startReadServer(t testing.TB, path string) *nativeReadServer {
	t.Helper()
	ln, err := net.Listen("unix", path)
	require.NoError(t, err)
	s := &nativeReadServer{ln: ln, recv: make(chan []byte, 64), path: path}
	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = conn.Close()

			return
		}
		s.conn = conn
		s.mu.Unlock()
		defer conn.Close()
		buf := make([]byte, 65536)
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

func (s *nativeReadServer) stop() {
	_ = s.ln.Close()
	s.mu.Lock()
	s.closed = true
	if s.conn != nil {
		_ = s.conn.Close()
	}
	s.mu.Unlock()
	_ = os.Remove(s.path)
}

// drainServer accepts one UDS connection and DISCARDS everything it reads — it never applies
// backpressure, so high-volume concurrency smoke tests (Task 10) cannot stall on a full channel.
type drainServer struct {
	ln     net.Listener
	path   string
	mu     sync.Mutex
	conn   net.Conn
	closed bool
}

func startDrainServer(t testing.TB, path string) *drainServer {
	t.Helper()
	ln, err := net.Listen("unix", path)
	require.NoError(t, err)
	s := &drainServer{ln: ln, path: path}
	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = conn.Close()

			return
		}
		s.conn = conn
		s.mu.Unlock()
		defer conn.Close()
		buf := make([]byte, 65536)
		for {
			if _, rerr := conn.Read(buf); rerr != nil {
				return
			}
		}
	}()

	return s
}

func (s *drainServer) stop() {
	_ = s.ln.Close()
	s.mu.Lock()
	s.closed = true
	if s.conn != nil {
		_ = s.conn.Close()
	}
	s.mu.Unlock()
	_ = os.Remove(s.path)
}
