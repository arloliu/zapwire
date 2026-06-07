package fluent

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	zapwire "github.com/arloliu/zapwire"
)

const benchJSON = `{"level":"info","ts":1692959400123456789,"msg":"request completed","caller":"server/handler.go:142","service":"zapwire","status":200}`

// drainingServer accepts and discards everything (a fast, never-stalling consumer).
func drainingServer(b *testing.B, path string) net.Listener {
	b.Helper()
	ln, err := net.Listen("unix", path)
	require.NoError(b, err)
	go func() {
		for {
			conn, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			go func(c net.Conn) {
				buf := make([]byte, 65536)
				for {
					if _, rerr := c.Read(buf); rerr != nil {
						return
					}
				}
			}(conn)
		}
	}()

	return ln
}

func benchWriter(b *testing.B, opts ...zapwire.Option) (*zapwire.Writer, func()) {
	b.Helper()
	path := filepath.Join(os.TempDir(), fmt.Sprintf("zapwire_bench_%d.sock", time.Now().UnixNano()))
	ln := drainingServer(b, path)
	w, err := NewWriter(zapwire.UDS(path), WithZapwireOptions(opts...))
	require.NoError(b, err)
	require.Eventually(b, w.IsConnected, time.Second, 5*time.Millisecond)

	return w, func() { _ = w.Close(); _ = ln.Close(); _ = os.Remove(path) }
}

func BenchmarkFluentWriter_Sync(b *testing.B) {
	w, cleanup := benchWriter(b)
	defer cleanup()
	msg := []byte(benchJSON)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = w.Write(msg)
	}
}

func BenchmarkFluentWriter_Async(b *testing.B) {
	w, cleanup := benchWriter(b, zapwire.WithAsyncMode())
	defer cleanup()
	msg := []byte(benchJSON)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = w.Write(msg)
	}
}
