//go:build otelcollector

package otlp

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
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

// Requires a real otel-collector binary; set OTELCOL_BIN (default
// /usr/local/bin/otelcol). Run via `make integration-otel`.
func TestCollectorEndToEnd(t *testing.T) {
	bin := os.Getenv("OTELCOL_BIN")
	if bin == "" {
		bin = "/usr/local/bin/otelcol"
	}
	if _, err := os.Stat(bin); err != nil {
		t.Skipf("otel-collector binary not found at %s", bin)
	}

	dir := t.TempDir()
	outFile := filepath.Join(dir, "out.json")
	port := freePort(t)
	cfg := fmt.Sprintf(`
receivers:
  otlp:
    protocols:
      http:
        endpoint: 127.0.0.1:%d
exporters:
  file:
    path: %s
service:
  pipelines:
    logs:
      receivers: [otlp]
      exporters: [file]
`, port, outFile)
	cfgFile := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(cfgFile, []byte(cfg), 0o644))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "--config", cfgFile)
	require.NoError(t, cmd.Start())
	defer func() { cancel(); _ = cmd.Wait() }()
	waitPort(t, port)

	core, w, err := NewCore(fmt.Sprintf("http://127.0.0.1:%d", port), zapcore.InfoLevel,
		WithServiceName("itest"), WithFlushInterval(50*time.Millisecond))
	require.NoError(t, err)
	logger := zap.New(core)
	sc, sctx := testSpanContext(t)
	logger.Info("integration", zap.String("k", "v"), SpanContext(sctx))
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	// Poll the file exporter output for our record with intact trace IDs.
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(outFile)
		if err != nil {
			return false
		}
		for _, line := range strings.Split(string(data), "\n") {
			if line == "" {
				continue
			}
			var doc map[string]any
			if json.Unmarshal([]byte(line), &doc) != nil {
				continue
			}
			s := string(data)
			_ = doc
			if strings.Contains(s, "integration") &&
				strings.Contains(strings.ToLower(s), strings.ToLower(sc.TraceID().String())) &&
				strings.Contains(strings.ToLower(s), strings.ToLower(sc.SpanID().String())) {
				return true
			}
		}

		return false
	}, 15*time.Second, 200*time.Millisecond, "record with trace IDs must reach the collector")
}

// TestCollectorEndToEndGRPC ships through NewGRPCCore to a real otel-collector
// OTLP/gRPC receiver (h2c) and asserts the file-exporter output — the same
// oracle as the HTTP variant.
func TestCollectorEndToEndGRPC(t *testing.T) {
	bin := os.Getenv("OTELCOL_BIN")
	if bin == "" {
		bin = "/usr/local/bin/otelcol"
	}
	if _, err := os.Stat(bin); err != nil {
		t.Skipf("otel-collector binary not found at %s", bin)
	}

	dir := t.TempDir()
	outFile := filepath.Join(dir, "out.json")
	port := freePort(t)
	cfg := fmt.Sprintf(`
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 127.0.0.1:%d
exporters:
  file:
    path: %s
service:
  pipelines:
    logs:
      receivers: [otlp]
      exporters: [file]
`, port, outFile)
	cfgFile := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(cfgFile, []byte(cfg), 0o644))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "--config", cfgFile)
	require.NoError(t, cmd.Start())
	defer func() { cancel(); _ = cmd.Wait() }()
	waitPort(t, port)

	core, w, err := NewGRPCCore(fmt.Sprintf("127.0.0.1:%d", port), zapcore.InfoLevel,
		WithInsecure(),
		WithServiceName("itest-grpc"), WithFlushInterval(50*time.Millisecond),
	)
	require.NoError(t, err)
	logger := zap.New(core)
	sc, sctx := testSpanContext(t)
	logger.Info("grpc end to end", zap.String("transport", "grpc"), SpanContext(sctx))
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())
	// Zero drops is the protocol discriminator: the gRPC client's success
	// path REQUIRES a grpc-status trailer (grpc_transport.go attempt) — a
	// receiver answering as plain HTTP would have failed every attempt and
	// counted the batch dropped.
	require.Zero(t, w.DroppedLogs(), "export must succeed over real gRPC")

	// Poll the file exporter output for our record with intact trace IDs —
	// the same oracle as the HTTP variant (design §9.4: body/attrs/trace
	// must land identically).
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(outFile)
		if err != nil {
			return false
		}
		s := strings.ToLower(string(data))

		return strings.Contains(s, "grpc end to end") &&
			strings.Contains(s, "transport") &&
			strings.Contains(s, strings.ToLower(sc.TraceID().String())) &&
			strings.Contains(s, strings.ToLower(sc.SpanID().String())) &&
			strings.Contains(s, "itest-grpc")
	}, 15*time.Second, 200*time.Millisecond, "record with trace IDs must reach the collector over gRPC")
}

// TestCollectorEndToEndJSON ships WithEncoding(JSON) output to a real
// otel-collector OTLP/HTTP receiver — proving a real receiver ingests the
// OTLP/JSON encoding (Content-Type application/json) with intact trace IDs,
// the same file-exporter oracle as the protobuf variant.
func TestCollectorEndToEndJSON(t *testing.T) {
	bin := os.Getenv("OTELCOL_BIN")
	if bin == "" {
		bin = "/usr/local/bin/otelcol"
	}
	if _, err := os.Stat(bin); err != nil {
		t.Skipf("otel-collector binary not found at %s", bin)
	}

	dir := t.TempDir()
	outFile := filepath.Join(dir, "out.json")
	port := freePort(t)
	cfg := fmt.Sprintf(`
receivers:
  otlp:
    protocols:
      http:
        endpoint: 127.0.0.1:%d
exporters:
  file:
    path: %s
service:
  pipelines:
    logs:
      receivers: [otlp]
      exporters: [file]
`, port, outFile)
	cfgFile := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(cfgFile, []byte(cfg), 0o644))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "--config", cfgFile)
	require.NoError(t, cmd.Start())
	defer func() { cancel(); _ = cmd.Wait() }()
	waitPort(t, port)

	core, w, err := NewCore(fmt.Sprintf("http://127.0.0.1:%d", port), zapcore.InfoLevel,
		WithEncoding(JSON),
		WithServiceName("itest-json"), WithFlushInterval(50*time.Millisecond))
	require.NoError(t, err)
	logger := zap.New(core)
	sc, sctx := testSpanContext(t)
	logger.Info("json end to end", zap.String("k", "v"), SpanContext(sctx))
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	require.Eventually(t, func() bool {
		data, err := os.ReadFile(outFile)
		if err != nil {
			return false
		}
		s := strings.ToLower(string(data))

		return strings.Contains(s, "json end to end") &&
			strings.Contains(s, strings.ToLower(sc.TraceID().String())) &&
			strings.Contains(s, strings.ToLower(sc.SpanID().String())) &&
			strings.Contains(s, "itest-json")
	}, 15*time.Second, 200*time.Millisecond, "JSON-encoded record with trace IDs must reach the collector")
}

// TestCollectorEndToEndGRPCTLS ships through NewGRPCCore to a real
// otel-collector OTLP/gRPC receiver behind TLS (self-signed leaf generated
// in-test). The bare host:port endpoint exercises the spec-default secure
// path — TLS with ALPN h2 — that the plaintext variants cannot reach, with
// WithTLSConfig supplying the test CA pool.
func TestCollectorEndToEndGRPCTLS(t *testing.T) {
	bin := os.Getenv("OTELCOL_BIN")
	if bin == "" {
		bin = "/usr/local/bin/otelcol"
	}
	if _, err := os.Stat(bin); err != nil {
		t.Skipf("otel-collector binary not found at %s", bin)
	}

	dir := t.TempDir()
	certFile, keyFile, pool := selfSignedCert(t, dir)
	outFile := filepath.Join(dir, "out.json")
	port := freePort(t)
	cfg := fmt.Sprintf(`
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 127.0.0.1:%d
        tls:
          cert_file: %s
          key_file: %s
exporters:
  file:
    path: %s
service:
  pipelines:
    logs:
      receivers: [otlp]
      exporters: [file]
`, port, certFile, keyFile, outFile)
	cfgFile := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(cfgFile, []byte(cfg), 0o644))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "--config", cfgFile)
	require.NoError(t, cmd.Start())
	defer func() { cancel(); _ = cmd.Wait() }()
	waitPort(t, port)

	// Bare endpoint — no scheme, no WithInsecure — is the spec-default
	// TLS path; only the trust anchor is test-specific.
	core, w, err := NewGRPCCore(fmt.Sprintf("127.0.0.1:%d", port), zapcore.InfoLevel,
		WithTLSConfig(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}),
		WithServiceName("itest-grpc-tls"), WithFlushInterval(50*time.Millisecond),
	)
	require.NoError(t, err)
	logger := zap.New(core)
	logger.Info("grpc tls end to end", zap.String("transport", "grpc-tls"))
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	require.Eventually(t, func() bool {
		data, err := os.ReadFile(outFile)
		if err != nil {
			return false
		}
		s := string(data)

		return strings.Contains(s, "grpc tls end to end") &&
			strings.Contains(s, "grpc-tls") &&
			strings.Contains(s, "itest-grpc-tls")
	}, 15*time.Second, 200*time.Millisecond, "record must reach the collector over gRPC+TLS")
}

// selfSignedCert writes a self-signed 127.0.0.1 certificate + key under dir
// and returns their paths plus a pool trusting the certificate.
func selfSignedCert(t *testing.T, dir string) (certFile, keyFile string, pool *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "zapwire-itest"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	pool = x509.NewCertPool()
	pool.AddCert(cert)

	return certFile, keyFile, pool
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port
}

func waitPort(t *testing.T, port int) {
	t.Helper()
	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 100*time.Millisecond)
		if err == nil {
			c.Close()
		}

		return err == nil
	}, 15*time.Second, 100*time.Millisecond)
}
