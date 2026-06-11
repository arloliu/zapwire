package syslog

import (
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/arloliu/zapwire"
)

const benchMsg = "request completed"

func benchCore(b *testing.B, opts ...Option) (*zap.Logger, func()) {
	b.Helper()
	path := filepath.Join(b.TempDir(), "syslog_bench.sock")
	ln, err := net.Listen("unix", path)
	require.NoError(b, err)
	go func() {
		for {
			conn, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			go func(c net.Conn) { _, _ = io.Copy(io.Discard, c); _ = c.Close() }(conn)
		}
	}()

	core, w, err := NewCore(zapwire.UDS(path), zap.InfoLevel, zap.NewProductionEncoderConfig(), opts...)
	require.NoError(b, err)
	require.Eventually(b, w.IsConnected, time.Second, 5*time.Millisecond)

	return zap.New(core), func() { _ = w.Close(); _ = ln.Close() }
}

func BenchmarkSyslogWriter_Sync(b *testing.B) {
	logger, cleanup := benchCore(b)
	defer cleanup()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		logger.Info(benchMsg, zap.String("svc", "zapwire"), zap.Int("status", 200))
	}
}

func BenchmarkSyslogWriter_Async(b *testing.B) {
	logger, cleanup := benchCore(b, WithZapwireOptions(zapwire.WithAsyncMode()))
	defer cleanup()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		logger.Info(benchMsg, zap.String("svc", "zapwire"), zap.Int("status", 200))
	}
}
