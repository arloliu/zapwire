//go:build fluentbit

package otlp

import (
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
	"go.uber.org/zap/zapcore"
)

// fluentBitBinEnvFB names the env var that points at the Fluent Bit binary under
// test (shared with the fluent module). Unset means "fall back to the CI/install
// default", and a missing binary skips — the test never downloads anything.
const fluentBitBinEnvFB = "ZAPWIRE_FLUENT_BIT_BIN"

// TestFluentBitOTLPEndToEnd boots a real Fluent Bit with an `opentelemetry` input
// (OTLP/HTTP on /v1/logs) feeding a json_lines stdout output, then ships one log
// record through zapwire's OTLP core (POST /v1/logs, binary protobuf) and asserts
// the record survives end to end.
//
// Run via `make integration-fluentbit`. Set ZAPWIRE_FLUENT_BIT_BIN, otherwise the
// test falls back to /opt/fluent-bit/bin/fluent-bit (the path the CI installer and
// the repo's fluent module both use); a missing binary skips.
//
// Trace-ID surfacing — observed against Fluent Bit v5.0.6 (the CI-pinned version):
// the `opentelemetry` input stores the message in the record BODY but keeps
// attributes, service.name, severity, trace_flags, and the trace_id/span_id under
// (group) METADATA. json_lines flattens that metadata into an `__internal__`
// block at serialize time, where trace_id/span_id appear only as raw 16/8-byte
// values (e.g. "trace_id":"[i3?\f") — never lowercase hex. The trace
// context is not reachable as record-body keys at any user-facing stage (verified:
// input-processor Lua, pipeline-filter Lua, and content_modifier all see a
// body-only record; the input exposes no logs_*_message_key promotion — its
// allowed props are just logs_metadata_key / logs_body_key), and the OTLP input
// has no hex-encoding option. So lowercase-hex trace IDs cannot be surfaced in any
// output on this version.
//
// We therefore assert the strongest signals this version DOES serialize and that
// prove the OTLP envelope round-tripped intact:
//   - the message body ("integration") and service.name ("fbtest");
//   - the structured attribute ("k":"v") promoted from LogRecord attributes;
//   - "severity_text":"info" (the mapped LogRecord severity);
//   - "trace_flags":1 — present ONLY because a valid, sampled span context
//     (SpanContext(ctx)) reached the receiver, so it is direct evidence the trace
//     context survived even though the binary will not emit the IDs as hex.
//
// A sample line (trimmed) from v5.0.6:
//
//	{"date":...,"__internal__":{"group_attributes":{"resource":{"attributes":
//	 {"service.name":"fbtest"},...}},"log_metadata":{"otlp":{...,
//	 "severity_text":"info","attributes":{"k":"v"},
//	 "trace_id":"[i3?\f","span_id":"~qt","trace_flags":1}}},
//	 "log":"integration"}
func TestFluentBitOTLPEndToEnd(t *testing.T) {
	bin := os.Getenv(fluentBitBinEnvFB)
	if bin == "" {
		bin = "/opt/fluent-bit/bin/fluent-bit"
	}
	if info, err := os.Stat(bin); err != nil || info.IsDir() {
		t.Skipf("Fluent Bit binary not found at %s (set %s)", bin, fluentBitBinEnvFB)
	}

	dir := t.TempDir()
	port := freePortFB(t)

	// YAML config: an `opentelemetry` input on a free loopback port and a
	// json_lines stdout output we capture and assert on. Kept minimal — no
	// processors — because (see the doc comment) no input-stage transform can
	// reach the trace context, so a richer pipeline buys nothing here.
	cfg := fmt.Sprintf(`service:
  flush: 0.2
  log_level: error
pipeline:
  inputs:
    - name: opentelemetry
      listen: 127.0.0.1
      port: %d
  outputs:
    - name: stdout
      match: '*'
      format: json_lines
`, port)
	cfgFile := filepath.Join(dir, "fluent-bit.yaml")
	require.NoError(t, os.WriteFile(cfgFile, []byte(cfg), 0o600))

	stdout := &syncBufferFB{}
	stderr := &syncBufferFB{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "-c", cfgFile)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	require.NoError(t, cmd.Start())

	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()
	defer func() {
		cancel()
		select {
		case <-waitCh:
		case <-time.After(3 * time.Second):
			t.Logf("fluent-bit did not exit within 3s of cancel\nstderr:\n%s", stderr.String())
		}
	}()

	waitPortFB(t, port, waitCh, stderr)

	core, w, err := NewCore(fmt.Sprintf("http://127.0.0.1:%d", port), zapcore.InfoLevel,
		WithServiceName("fbtest"), WithFlushInterval(50*time.Millisecond))
	require.NoError(t, err)
	logger := zap.New(core)
	_, sctx := testSpanContext(t)
	logger.Info("integration", zap.String("k", "v"), SpanContext(sctx))
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	// Poll fluent-bit's stdout until every signal proving the OTLP envelope
	// round-tripped is present (see the doc comment for why these and not hex IDs).
	require.Eventually(t, func() bool {
		out := stdout.String()
		for _, sub := range []string{
			"integration",            // message body
			"fbtest",                 // service.name (resource attribute)
			`"k":"v"`,                // LogRecord attribute
			`"severity_text":"info"`, // mapped severity
			`"trace_flags":1`,        // proves a valid sampled span context arrived
		} {
			if !strings.Contains(out, sub) {
				return false
			}
		}

		return true
		// Pass the *syncBufferFB Stringers (not snapshots): require.Eventually
		// evaluates these args eagerly at call time, but %s defers String() to
		// when the failure message is built (the deadline), so the dump reflects
		// what fluent-bit actually emitted rather than the empty pre-flush buffer.
	}, 15*time.Second, 200*time.Millisecond,
		"record must reach Fluent Bit with service name, attribute, severity and the sampled span context\nstdout:\n%s\nstderr:\n%s",
		stdout, stderr)
}

// freePortFB returns a currently-free 127.0.0.1 port. There is an unavoidable
// bind/reuse window, but a lost race makes fluent-bit fail to bind and exit, which
// waitPortFB surfaces loudly rather than flakily.
func freePortFB(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())

	return port
}

// waitPortFB blocks until the input accepts connections, failing fast (with
// fluent-bit's stderr) if the process exits before becoming ready.
func waitPortFB(t *testing.T, port int, waitCh <-chan error, stderr *syncBufferFB) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		select {
		case <-waitCh:
			t.Fatalf("fluent-bit exited before it was ready\nstderr:\n%s", stderr.String())
		default:
		}
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 100*time.Millisecond)
		if err == nil {
			_ = c.Close()

			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("fluent-bit never became ready within 20s\nstderr:\n%s", stderr.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// syncBufferFB is an io.Writer the os/exec writer goroutine and the test goroutine
// can share safely.
type syncBufferFB struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncBufferFB) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *syncBufferFB) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}
