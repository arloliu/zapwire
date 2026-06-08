package fluent

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

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

// Native benchmarks below log through a real zap.Logger built on NewNativeCore, so they measure
// the FULL hot path: zap field handling + native msgpack encoding + the Writer/Framer/socket. The
// transcode BenchmarkFluentWriter_* benches above instead send a prebuilt []byte straight to
// w.Write, measuring ONLY the writer path (they exclude JSON encoding). The two are therefore NOT
// a strict apples-to-apples micro-comparison: the native numbers include strictly MORE work (the
// encode step) yet still report far fewer allocs/op — a conservative reading of the native
// encoder's allocation advantage (design §3 / §5.7).
func benchNativeLogger(b *testing.B, opts ...zapwire.Option) (*zap.Logger, func()) {
	b.Helper()
	path := filepath.Join(os.TempDir(), fmt.Sprintf("zapwire_native_bench_%d.sock", time.Now().UnixNano()))
	ln := drainingServer(b, path)
	core, w, err := NewNativeCore(zapwire.UDS(path), zap.InfoLevel, zap.NewProductionEncoderConfig(),
		WithZapwireOptions(opts...))
	require.NoError(b, err)
	require.Eventually(b, w.IsConnected, time.Second, 5*time.Millisecond)

	return zap.New(core), func() { _ = w.Close(); _ = ln.Close(); _ = os.Remove(path) }
}

func benchFields() []zapcore.Field {
	return []zapcore.Field{
		zap.String("service", "zapwire"),
		zap.Int("status", 200),
		zap.String("caller", "server/handler.go:142"),
	}
}

func BenchmarkNativeWriter_Sync(b *testing.B) {
	logger, cleanup := benchNativeLogger(b)
	defer cleanup()
	fields := benchFields()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		logger.Info("request completed", fields...)
	}
}

func BenchmarkNativeWriter_Async(b *testing.B) {
	logger, cleanup := benchNativeLogger(b, zapwire.WithAsyncMode())
	defer cleanup()
	fields := benchFields()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		logger.Info("request completed", fields...)
	}
}

func BenchmarkNativeWriter_SyncNestedObject(b *testing.B) {
	logger, cleanup := benchNativeLogger(b)
	defer cleanup()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		logger.Info("request completed",
			zap.String("service", "zapwire"),
			zap.Object("http", objMarshaler(func(enc zapcore.ObjectEncoder) error {
				enc.AddInt("status", 200)
				enc.AddString("method", "GET")

				return nil
			})),
		)
	}
}
