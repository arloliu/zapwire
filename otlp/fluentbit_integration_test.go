//go:build fluentbit

package otlp

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
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

	core, w, err := NewHTTPCore(fmt.Sprintf("http://127.0.0.1:%d", port), zapcore.InfoLevel,
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

// TestFluentBitOTLPGRPCEndToEnd is the OTLP/gRPC variant of
// TestFluentBitOTLPEndToEnd (design 2026-06-12 §9.4 follow-up). Fluent Bit's
// `opentelemetry` input serves OTLP/HTTP and OTLP/gRPC on the same port
// (verified against v5.0.6), so the config is identical; the record ships
// through NewGRPCCore (h2c via WithInsecure) instead. The input normalizes
// both protocols into the same record shape, so the assertions are the HTTP
// variant's verbatim — a match proves zapwire's hand-rolled gRPC framing and
// trace-context bytes interoperate with FB's non-Go gRPC server.
func TestFluentBitOTLPGRPCEndToEnd(t *testing.T) {
	bin := os.Getenv(fluentBitBinEnvFB)
	if bin == "" {
		bin = "/opt/fluent-bit/bin/fluent-bit"
	}
	if info, err := os.Stat(bin); err != nil || info.IsDir() {
		t.Skipf("Fluent Bit binary not found at %s (set %s)", bin, fluentBitBinEnvFB)
	}

	dir := t.TempDir()
	port := freePortFB(t)

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

	core, w, err := NewGRPCCore(fmt.Sprintf("127.0.0.1:%d", port), zapcore.InfoLevel,
		WithInsecure(),
		WithServiceName("fbtest-grpc"), WithFlushInterval(50*time.Millisecond))
	require.NoError(t, err)
	logger := zap.New(core)
	_, sctx := testSpanContext(t)
	logger.Info("grpc-integration", zap.String("k", "v"), SpanContext(sctx))
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())
	// Zero drops is the protocol discriminator: NewGRPCCore has no HTTP
	// fallback (h2c-only client, gRPC framing, fixed method path) and its
	// success path REQUIRES a grpc-status trailer — if FB had answered this
	// request as plain HTTP, every attempt would have failed and the batch
	// would be counted dropped. Zero drops + the record landing below proves
	// FB's gRPC server path de-framed and ingested it.
	require.Zero(t, w.DroppedLogs(), "export must succeed over FB's real gRPC path")

	// Same signals as the HTTP variant (see TestFluentBitOTLPEndToEnd's doc
	// comment for why hex trace IDs cannot be asserted on this FB version).
	require.Eventually(t, func() bool {
		out := stdout.String()
		for _, sub := range []string{
			"grpc-integration",       // message body
			"fbtest-grpc",            // service.name (resource attribute)
			`"k":"v"`,                // LogRecord attribute
			`"severity_text":"info"`, // mapped severity
			`"trace_flags":1`,        // proves a valid sampled span context arrived
		} {
			if !strings.Contains(out, sub) {
				return false
			}
		}

		return true
	}, 15*time.Second, 200*time.Millisecond,
		"record must reach Fluent Bit over gRPC with service name, attribute, severity and the sampled span context\nstdout:\n%s\nstderr:\n%s",
		stdout, stderr)
}

// TestFluentBitOTLPRelayFidelity proves the RELAY pipeline shape preserves trace
// context byte-for-byte: zapwire OTLP core → Fluent Bit `opentelemetry` input →
// Fluent Bit `opentelemetry` OUTPUT → an httptest "OTLP backend" that captures the
// raw POST body. Fluent Bit reconstructs the OTLP envelope on the output side, so
// the trace_id/span_id bytes must survive even though FB re-encodes (the whole
// request is NOT byte-identical to what zapwire sent — FB re-frames it). We assert
// the relayed request's LogRecord trace_id (field 9) and span_id (field 10) equal
// the fixture's bytes, decoding with the package's dependency-free findField walker
// (NO protobuf imports — the otlp module never depends on google.golang.org/protobuf).
//
// Observed against Fluent Bit v5.0.6 (CI-pinned): with grpc=off the otel output
// POSTs binary protobuf (Content-Type application/x-protobuf) to logs_uri; the body
// is uncompressed by default (compress unset). The handler tolerates gzip via
// Content-Encoding in case a future/default build compresses.
func TestFluentBitOTLPRelayFidelity(t *testing.T) {
	bin := os.Getenv(fluentBitBinEnvFB)
	if bin == "" {
		bin = "/opt/fluent-bit/bin/fluent-bit"
	}
	if info, err := os.Stat(bin); err != nil || info.IsDir() {
		t.Skipf("Fluent Bit binary not found at %s (set %s)", bin, fluentBitBinEnvFB)
	}

	// The "OTLP backend": capture every relayed POST body (decompressed).
	var mu sync.Mutex
	var bodies [][]byte
	var sawPaths []string
	var sawEnc []string
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		var body []byte
		if r.Header.Get("Content-Encoding") == "gzip" {
			if zr, err := gzip.NewReader(r.Body); err == nil {
				body, _ = io.ReadAll(zr)
				_ = zr.Close()
			}
		} else {
			body, _ = io.ReadAll(r.Body)
		}
		mu.Lock()
		bodies = append(bodies, body)
		sawPaths = append(sawPaths, r.URL.Path)
		sawEnc = append(sawEnc, r.Header.Get("Content-Encoding"))
		mu.Unlock()
		// OTLP success: empty ExportLogsServiceResponse.
		rw.Header().Set("Content-Type", "application/x-protobuf")
		rw.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	backURL, err := url.Parse(srv.URL)
	require.NoError(t, err)
	backHost, backPort, err := net.SplitHostPort(backURL.Host)
	require.NoError(t, err)

	dir := t.TempDir()
	inPort := freePortFB(t)

	// otel input on a free port; otel OUTPUT relays to the httptest backend.
	// grpc=off forces HTTP/protobuf; tls=off because httptest is plaintext;
	// logs_uri matches the backend path zapwire/httptest serve at root.
	cfg := fmt.Sprintf(`service:
  flush: 0.2
  log_level: info
pipeline:
  inputs:
    - name: opentelemetry
      listen: 127.0.0.1
      port: %d
  outputs:
    - name: opentelemetry
      match: '*'
      host: %s
      port: %s
      grpc: off
      tls: off
      logs_uri: /v1/logs
`, inPort, backHost, backPort)
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

	waitPortFB(t, inPort, waitCh, stderr)

	core, w, err := NewHTTPCore(fmt.Sprintf("http://127.0.0.1:%d", inPort), zapcore.InfoLevel,
		WithServiceName("fbrelay"), WithFlushInterval(50*time.Millisecond))
	require.NoError(t, err)
	logger := zap.New(core)
	sc, sctx := testSpanContext(t)
	const msg = "relay-integration"
	logger.Info(msg, zap.String("k", "v"), SpanContext(sctx))
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	wantTID, wantSID := sc.TraceID(), sc.SpanID()

	// Poll until a relayed body carries our record with intact trace IDs. FB may
	// batch/re-frame; we accept any captured body whose first LogRecord matches.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, b := range bodies {
			if len(b) == 0 {
				continue
			}
			rec := firstLogRecordFB(b)
			if rec == nil {
				continue
			}
			tid, err := findField(rec, 9)
			if err != nil || !bytes.Equal(tid, wantTID[:]) {
				continue
			}
			sid, err := findField(rec, 10)
			if err != nil || !bytes.Equal(sid, wantSID[:]) {
				continue
			}
			if !bytes.Contains(b, []byte(msg)) {
				continue
			}

			return true
		}

		return false
	}, 20*time.Second, 200*time.Millisecond,
		"relayed OTLP request must carry the fixture trace_id/span_id and message\npaths=%v enc=%v\nstderr:\n%s",
		&sawPathsFB{&mu, &sawPaths}, &sawPathsFB{&mu, &sawEnc}, stderr)
}

// TestFluentBitOTLPRelayFidelityGRPC is the gRPC-egress variant of
// TestFluentBitOTLPRelayFidelity (design 2026-06-12 §9.4 follow-up): zapwire
// OTLP core → Fluent Bit `opentelemetry` input → Fluent Bit `opentelemetry`
// OUTPUT with `grpc: on` → the package's fakeGRPCServer (stdlib h2c HTTP/2,
// reused from grpc_transport_test.go), which de-frames the Export request and
// records the raw protobuf message. Asserting the relayed LogRecord's
// trace_id/span_id bytes proves FB's gRPC *client* and our stdlib h2c server
// agree on framing — the mirror image of the ingest test, closing the loop on
// both gRPC directions against a non-Go implementation.
func TestFluentBitOTLPRelayFidelityGRPC(t *testing.T) {
	bin := os.Getenv(fluentBitBinEnvFB)
	if bin == "" {
		bin = "/opt/fluent-bit/bin/fluent-bit"
	}
	if info, err := os.Stat(bin); err != nil || info.IsDir() {
		t.Skipf("Fluent Bit binary not found at %s (set %s)", bin, fluentBitBinEnvFB)
	}

	// The "OTLP backend": the fake h2c gRPC server records every de-framed
	// Export message; the handler answers an empty success response.
	fake := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) {
		okResponse(w, nil)
	})

	backURL, err := url.Parse(fake.srv.URL)
	require.NoError(t, err)
	backHost, backPort, err := net.SplitHostPort(backURL.Host)
	require.NoError(t, err)

	dir := t.TempDir()
	inPort := freePortFB(t)

	// otel input on a free port; otel OUTPUT relays over gRPC (prior-knowledge
	// h2c — tls=off) to the fake backend. No logs_uri: gRPC uses the fixed
	// LogsService/Export method path.
	cfg := fmt.Sprintf(`service:
  flush: 0.2
  log_level: info
pipeline:
  inputs:
    - name: opentelemetry
      listen: 127.0.0.1
      port: %d
  outputs:
    - name: opentelemetry
      match: '*'
      host: %s
      port: %s
      grpc: on
      tls: off
`, inPort, backHost, backPort)
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

	waitPortFB(t, inPort, waitCh, stderr)

	core, w, err := NewHTTPCore(fmt.Sprintf("http://127.0.0.1:%d", inPort), zapcore.InfoLevel,
		WithServiceName("fbrelay-grpc"), WithFlushInterval(50*time.Millisecond))
	require.NoError(t, err)
	logger := zap.New(core)
	sc, sctx := testSpanContext(t)
	const msg = "relay-grpc-integration"
	logger.Info(msg, zap.String("k", "v"), SpanContext(sctx))
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	wantTID, wantSID := sc.TraceID(), sc.SpanID()

	// Poll the fake backend until a relayed Export message carries our record
	// with intact trace IDs. FB may batch/re-frame; accept any captured
	// message whose LogRecord matches.
	require.Eventually(t, func() bool {
		for _, b := range fake.received() {
			if len(b) == 0 {
				continue
			}
			rec := firstLogRecordFB(b)
			if rec == nil {
				continue
			}
			tid, err := findField(rec, 9)
			if err != nil || !bytes.Equal(tid, wantTID[:]) {
				continue
			}
			sid, err := findField(rec, 10)
			if err != nil || !bytes.Equal(sid, wantSID[:]) {
				continue
			}
			if !bytes.Contains(b, []byte(msg)) {
				continue
			}

			return true
		}

		return false
	}, 20*time.Second, 200*time.Millisecond,
		"relayed OTLP/gRPC request must carry the fixture trace_id/span_id and message\nstderr:\n%s",
		stderr)
}

// firstLogRecordFB walks an ExportLogsServiceRequest payload to the first
// LogRecord using only the package's dependency-free findField walker:
// resource_logs(1) → scope_logs(2) → log_records(2). Returns nil if any level
// is absent. findField returns the LAST occurrence at each level, which is fine
// here: the relay test ships exactly one record, so there is a single chain.
func firstLogRecordFB(req []byte) []byte {
	rl, err := findField(req, 1) // ExportLogsServiceRequest.resource_logs
	if err != nil || rl == nil {
		return nil
	}
	sl, err := findField(rl, 2) // ResourceLogs.scope_logs
	if err != nil || sl == nil {
		return nil
	}
	rec, err := findField(sl, 2) // ScopeLogs.log_records
	if err != nil {
		return nil
	}

	return rec
}

// sawPathsFB defers the lock-guarded join of a captured slice to failure-message
// build time (require.Eventually evaluates format args eagerly, but %s calls
// String() lazily), so the dump reflects what FB actually sent.
type sawPathsFB struct {
	mu *sync.Mutex
	s  *[]string
}

func (p *sawPathsFB) String() string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return strings.Join(*p.s, ",")
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
