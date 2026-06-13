//go:build vector

package otlp

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Requires a real Vector (vector.dev) binary; set VECTOR_BIN, otherwise the
// test looks up `vector` on PATH and falls back to /usr/local/bin/vector. Run
// via `make integration-vector`.
//
// Flow: boot Vector with an `opentelemetry` source (gRPC + HTTP on free ports —
// many Vector versions require both sub-sections even for an HTTP-only setup)
// feeding a `file` sink with the JSON codec, reading from the source's `.logs`
// output stream. Log one record through NewCore (OTLP/HTTP → POST /v1/logs) and
// assert it survives into Vector's emitted event with intact lowercase-hex
// trace/span IDs and the service name.
//
// Vector 0.56.0 renders the event as (trimmed):
//
//	{"attributes":{"k":"v"},"flags":1,"message":"integration",
//	 "resources":{"service.name":"vtest"},
//	 "span_id":"eee19b7ec3c1b174",
//	 "trace_id":"5b8efff798038103d269b633813fc60c", ...}
//
// so trace_id/span_id are lowercase hex and service.name lives under resources.
// Whole-file substring checks survive whatever nesting Vector applies.
func TestVectorEndToEnd(t *testing.T) {
	bin := os.Getenv("VECTOR_BIN")
	if bin == "" {
		if p, err := exec.LookPath("vector"); err == nil {
			bin = p
		} else {
			bin = "/usr/local/bin/vector"
		}
	}
	if _, err := os.Stat(bin); err != nil {
		t.Skipf("vector binary not found at %s (set VECTOR_BIN)", bin)
	}

	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	require.NoError(t, os.MkdirAll(dataDir, 0o755))
	outFile := filepath.Join(dir, "out.json")
	grpcPort := freePortVec(t)
	httpPort := freePortVec(t)

	// The sink input must reference the `.logs` output of the source, not the
	// bare source id (the opentelemetry source fans out logs/metrics/traces).
	cfg := fmt.Sprintf(`data_dir: %s
sources:
  otlp:
    type: opentelemetry
    grpc:
      address: 127.0.0.1:%d
    http:
      address: 127.0.0.1:%d
sinks:
  out:
    type: file
    inputs:
      - otlp.logs
    path: %s
    encoding:
      codec: json
`, dataDir, grpcPort, httpPort, outFile)
	cfgFile := filepath.Join(dir, "vector.yaml")
	require.NoError(t, os.WriteFile(cfgFile, []byte(cfg), 0o644))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "--config", cfgFile)
	require.NoError(t, cmd.Start())
	defer func() { cancel(); _ = cmd.Wait() }()
	waitPortVec(t, httpPort)

	core, w, err := NewCore(fmt.Sprintf("http://127.0.0.1:%d", httpPort), zapcore.InfoLevel,
		WithServiceName("vtest"), WithFlushInterval(50*time.Millisecond))
	require.NoError(t, err)
	logger := zap.New(core)
	sc, sctx := testSpanContext(t)
	logger.Info("integration", zap.String("k", "v"), SpanContext(sctx))
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	// Poll the file sink output for our record with intact trace IDs and the
	// service name flowing through as a resource attribute.
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(outFile)
		if err != nil {
			return false
		}
		s := strings.ToLower(string(data))

		return strings.Contains(s, "integration") &&
			strings.Contains(s, strings.ToLower(sc.TraceID().String())) &&
			strings.Contains(s, strings.ToLower(sc.SpanID().String())) &&
			strings.Contains(s, "vtest")
	}, 15*time.Second, 200*time.Millisecond, "record with trace IDs and service name must reach Vector")
}

// TestVectorEndToEndGRPC is the OTLP/gRPC variant of TestVectorEndToEnd
// (design 2026-06-12 §9.4 follow-up): same Vector topology, but the record
// ships through NewGRPCCore to the source's gRPC address (h2c via
// WithInsecure). Assertions are identical — Vector normalizes both protocols
// into the same event shape, so a matching event proves the gRPC framing,
// method path, and trace-context bytes interoperate with a real non-Go
// OTLP/gRPC receiver.
func TestVectorEndToEndGRPC(t *testing.T) {
	bin := os.Getenv("VECTOR_BIN")
	if bin == "" {
		if p, err := exec.LookPath("vector"); err == nil {
			bin = p
		} else {
			bin = "/usr/local/bin/vector"
		}
	}
	if _, err := os.Stat(bin); err != nil {
		t.Skipf("vector binary not found at %s (set VECTOR_BIN)", bin)
	}

	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	require.NoError(t, os.MkdirAll(dataDir, 0o755))
	outFile := filepath.Join(dir, "out.json")
	grpcPort := freePortVec(t)
	httpPort := freePortVec(t)

	cfg := fmt.Sprintf(`data_dir: %s
sources:
  otlp:
    type: opentelemetry
    grpc:
      address: 127.0.0.1:%d
    http:
      address: 127.0.0.1:%d
sinks:
  out:
    type: file
    inputs:
      - otlp.logs
    path: %s
    encoding:
      codec: json
`, dataDir, grpcPort, httpPort, outFile)
	cfgFile := filepath.Join(dir, "vector.yaml")
	require.NoError(t, os.WriteFile(cfgFile, []byte(cfg), 0o644))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "--config", cfgFile)
	require.NoError(t, cmd.Start())
	defer func() { cancel(); _ = cmd.Wait() }()
	waitPortVec(t, grpcPort)

	core, w, err := NewGRPCCore(fmt.Sprintf("127.0.0.1:%d", grpcPort), zapcore.InfoLevel,
		WithInsecure(),
		WithServiceName("vtest-grpc"), WithFlushInterval(50*time.Millisecond))
	require.NoError(t, err)
	logger := zap.New(core)
	sc, sctx := testSpanContext(t)
	logger.Info("grpc-integration", zap.String("k", "v"), SpanContext(sctx))
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	// Poll the file sink output for our record with intact trace IDs and the
	// service name flowing through as a resource attribute.
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(outFile)
		if err != nil {
			return false
		}
		s := strings.ToLower(string(data))

		return strings.Contains(s, "grpc-integration") &&
			strings.Contains(s, strings.ToLower(sc.TraceID().String())) &&
			strings.Contains(s, strings.ToLower(sc.SpanID().String())) &&
			strings.Contains(s, "vtest-grpc")
	}, 15*time.Second, 200*time.Millisecond, "record with trace IDs and service name must reach Vector over gRPC")
}

func freePortVec(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port
}

func waitPortVec(t *testing.T, port int) {
	t.Helper()
	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 100*time.Millisecond)
		if err == nil {
			c.Close()
		}

		return err == nil
	}, 15*time.Second, 100*time.Millisecond)
}
