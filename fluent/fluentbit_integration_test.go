//go:build fluentbit

// This file holds an opt-in integration test that runs a real Fluent Bit
// process and proves that zapwire's Fluent Forward PackedForward frames are
// accepted and correctly decoded end to end — including that native msgpack
// preserves numeric types across the wire, over both UDS and TCP.
//
// It is gated behind the `fluentbit` build tag, so the default `make test` / CI
// gate never compiles it and never needs a Fluent Bit binary. To run it, point
// ZAPWIRE_FLUENT_BIT_BIN at a real binary:
//
//	ZAPWIRE_FLUENT_BIT_BIN=/opt/fluent-bit/bin/fluent-bit \
//	    go test ./fluent -tags fluentbit -run Integration -count=1 -v
//
// When the env var is unset the test skips, so leaving the tag on is harmless.
// It is written as an external test (package fluent_test) so it exercises the
// public API exactly as a downstream consumer would.
package fluent_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/arloliu/zapwire"
	"github.com/arloliu/zapwire/fluent"
)

// fluentBitBinEnv names the env var that points at the Fluent Bit binary under
// test. Unset means "skip" — the test never installs or downloads anything.
const fluentBitBinEnv = "ZAPWIRE_FLUENT_BIT_BIN"

// TestIntegration_FluentBitForward_NativeUDS ships native-msgpack Fluent Forward
// frames over a Unix socket to a real Fluent Bit and asserts the records arrive
// decoded, with numeric types intact (the native path's whole point).
func TestIntegration_FluentBitForward_NativeUDS(t *testing.T) {
	socketPath, stdout := startFluentBitForwardUDS(t)

	core, writer, err := fluent.NewNativeCore(
		zapwire.UDS(socketPath),
		zap.InfoLevel,
		zap.NewProductionEncoderConfig(),
		fluent.WithTag("zapwire.itest.native-uds"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = writer.Close() })

	requireConnected(t, writer)

	logger := zap.New(core)
	const marker = "zapwire-native-uds-itest"
	for i := range 3 {
		logger.Info(marker,
			zap.String("token", fmt.Sprintf("native-uds-%d", i)),
			zap.Int64("count", 42),
			zap.Float64("ratio", 0.5),
			zap.Bool("ok", true),
		)
	}
	require.NoError(t, logger.Sync())

	got := waitForRecords(t, stdout, marker, "native-uds-0", "native-uds-1", "native-uds-2")

	// Native msgpack preserves numeric types end to end: an int stays an
	// integer (no ".0") and a float keeps its exact value. This is what the
	// native path buys over a JSON round-trip.
	require.Contains(t, got, `"count":42`, "native int64 must round-trip as an integer")
	require.Contains(t, got, `"ratio":0.5`, "native float64 must round-trip exactly")
	require.Contains(t, got, `"ok":true`)
	require.Contains(t, got, `"date":`, "fluent-bit must decode the EventTime into a record timestamp")
	require.Zero(t, writer.DroppedLogs(), "no logs may be dropped against a live consumer")
}

// TestIntegration_FluentBitForward_TranscodeUDS ships JSON-transcoded Fluent
// Forward frames (the fluent.NewCore path) and asserts they arrive decoded —
// proving the transcode encoder also emits frames fluent-bit accepts.
func TestIntegration_FluentBitForward_TranscodeUDS(t *testing.T) {
	socketPath, stdout := startFluentBitForwardUDS(t)

	core, writer, err := fluent.NewCore(
		zapwire.UDS(socketPath),
		zap.InfoLevel,
		zap.NewProductionEncoderConfig(),
		fluent.WithTag("zapwire.itest.transcode-uds"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = writer.Close() })

	requireConnected(t, writer)

	logger := zap.New(core)
	const marker = "zapwire-transcode-uds-itest"
	for i := range 3 {
		logger.Info(marker,
			zap.String("token", fmt.Sprintf("transcode-uds-%d", i)),
			zap.String("kind", "transcode"),
		)
	}
	require.NoError(t, logger.Sync())

	got := waitForRecords(t, stdout, marker, "transcode-uds-0", "transcode-uds-1", "transcode-uds-2")
	require.Contains(t, got, `"kind":"transcode"`)
	require.Contains(t, got, `"date":`, "fluent-bit must decode the EventTime into a record timestamp")
	require.Zero(t, writer.DroppedLogs(), "no logs may be dropped against a live consumer")
}

// TestIntegration_FluentBitForward_NativeTCP exercises the TCP transport: native
// Fluent Forward frames over a loopback TCP port into a real Fluent Bit forward
// input, asserting the same content + numeric-typing guarantees as the UDS path.
func TestIntegration_FluentBitForward_NativeTCP(t *testing.T) {
	addr, stdout := startFluentBitForwardTCP(t)

	core, writer, err := fluent.NewNativeCore(
		zapwire.TCP(addr),
		zap.InfoLevel,
		zap.NewProductionEncoderConfig(),
		fluent.WithTag("zapwire.itest.native-tcp"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = writer.Close() })

	requireConnected(t, writer)

	logger := zap.New(core)
	const marker = "zapwire-native-tcp-itest"
	for i := range 3 {
		logger.Info(marker,
			zap.String("token", fmt.Sprintf("native-tcp-%d", i)),
			zap.Int64("count", 42),
		)
	}
	require.NoError(t, logger.Sync())

	got := waitForRecords(t, stdout, marker, "native-tcp-0", "native-tcp-1", "native-tcp-2")
	require.Contains(t, got, `"count":42`, "native int64 must round-trip as an integer over TCP")
	require.Contains(t, got, `"date":`, "fluent-bit must decode the EventTime into a record timestamp")
	require.Zero(t, writer.DroppedLogs(), "no logs may be dropped against a live consumer")
}

// startFluentBitForwardUDS launches a real Fluent Bit with a forward input on a
// Unix socket and returns the socket path plus a buffer capturing the decoded
// records (the process's stdout).
func startFluentBitForwardUDS(t *testing.T) (socketPath string, stdout *syncBuffer) {
	t.Helper()
	socketPath = filepath.Join(t.TempDir(), "fluent.sock")
	input := fmt.Sprintf("[INPUT]\n    Name         forward\n    Unix_Path    %s\n", socketPath)
	stdout = startFluentBit(t, input, func() bool { return dialOK(t, "unix", socketPath) })

	return socketPath, stdout
}

// startFluentBitForwardTCP launches a real Fluent Bit with a forward input on a
// loopback TCP port and returns the dial address plus the records buffer.
func startFluentBitForwardTCP(t *testing.T) (addr string, stdout *syncBuffer) {
	t.Helper()
	addr = freeLoopbackAddr(t)
	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	input := fmt.Sprintf("[INPUT]\n    Name         forward\n    Listen       %s\n    Port         %s\n", host, port)
	stdout = startFluentBit(t, input, func() bool { return dialOK(t, "tcp", addr) })

	return addr, stdout
}

// startFluentBit writes a config (the given INPUT block plus a stdout output in
// json_lines format), starts Fluent Bit, waits until ready() reports the input
// is accepting connections, and returns a buffer holding the process's stdout
// (the decoded records). Fluent Bit writes its own logs to stderr, so the
// returned buffer holds only records. The process is torn down via t.Cleanup.
func startFluentBit(t *testing.T, inputBlock string, ready func() bool) *syncBuffer {
	t.Helper()
	bin := fluentBitBinary(t)

	configPath := filepath.Join(t.TempDir(), "fluent-bit.conf")
	// The classic config format is indentation-sensitive (4 spaces under a section).
	config := "[SERVICE]\n" +
		"    Flush        0.2\n" +
		"    Grace        1\n" +
		"    Log_Level    error\n\n" +
		inputBlock + "\n" +
		"[OUTPUT]\n" +
		"    Name         stdout\n" +
		"    Match        *\n" +
		"    Format       json_lines\n"
	require.NoError(t, os.WriteFile(configPath, []byte(config), 0o600))

	stdout := &syncBuffer{}
	stderr := &syncBuffer{}

	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, bin, "-c", configPath)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	require.NoError(t, cmd.Start())

	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()

	t.Cleanup(func() {
		cancel()
		select {
		case <-waitCh:
		case <-time.After(3 * time.Second):
			t.Logf("fluent-bit did not exit within 3s of cancel\nstderr:\n%s", stderr.String())
		}
	})

	// Wait until the input accepts connections and the process is still alive.
	// Poll from the test goroutine so t.Fatalf is safe to call here.
	deadline := time.Now().Add(20 * time.Second)
	for {
		if processExited(waitCh) {
			t.Fatalf("fluent-bit exited before it was ready\nstderr:\n%s", stderr.String())
		}
		if ready() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fluent-bit never became ready within 20s\nstderr:\n%s", stderr.String())
		}
		time.Sleep(50 * time.Millisecond)
	}

	return stdout
}

// fluentBitBinary resolves the binary under test, skipping the test when the env
// var is unset and failing fast when it points at something unusable.
func fluentBitBinary(t *testing.T) string {
	t.Helper()
	bin := os.Getenv(fluentBitBinEnv)
	if bin == "" {
		t.Skipf("%s not set; skipping real Fluent Bit integration test", fluentBitBinEnv)
	}
	info, err := os.Stat(bin)
	require.NoErrorf(t, err, "%s=%q is not usable", fluentBitBinEnv, bin)
	require.Falsef(t, info.IsDir(), "%s=%q is a directory, want a binary", fluentBitBinEnv, bin)

	return bin
}

// dialOK reports whether network/address currently accepts a connection.
func dialOK(t *testing.T, network, address string) bool {
	t.Helper()
	conn, err := net.DialTimeout(network, address, 100*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()

	return true
}

// freeLoopbackAddr returns a currently-free 127.0.0.1:port. There is an
// unavoidable bind/reuse window, but the readiness probe detects a failed bind
// (fluent-bit exits) and surfaces it, so a lost race fails loudly, not flakily.
func freeLoopbackAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	return addr
}

// requireConnected blocks until the writer reports a live connection, failing if
// it never connects.
func requireConnected(t *testing.T, w *zapwire.Writer) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !w.IsConnected() {
		if time.Now().After(deadline) {
			t.Fatal("zapwire writer never connected to the fluent-bit forward input")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// waitForRecords polls fluent-bit's captured stdout until every wanted substring
// is present, then returns the full output. It fails (with the output) on
// timeout.
func waitForRecords(t *testing.T, stdout *syncBuffer, want ...string) string {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		out := stdout.String()
		if containsAll(out, want) {
			return out
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for fluent-bit to emit %v\ngot:\n%s", want, out)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func containsAll(s string, subs []string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}

	return true
}

func processExited(waitCh <-chan error) bool {
	select {
	case <-waitCh:
		return true
	default:
		return false
	}
}

// syncBuffer is an io.Writer the os/exec writer goroutine and the test goroutine
// can share safely.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}
