package otlp

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestResolveGRPCEndpoint(t *testing.T) {
	cases := []struct {
		name      string
		endpoint  string
		insecure  bool
		hasTLSCfg bool
		wantBase  string
		wantTLS   bool
		wantErr   bool
	}{
		{name: "bare defaults to TLS and 4317", endpoint: "collector.example.com", wantBase: "https://collector.example.com:4317", wantTLS: true},
		{name: "bare with port keeps port", endpoint: "localhost:14317", wantBase: "https://localhost:14317", wantTLS: true},
		{name: "bare insecure is h2c", endpoint: "localhost:4317", insecure: true, wantBase: "http://localhost:4317"},
		{name: "bare with tls config", endpoint: "localhost:4317", hasTLSCfg: true, wantBase: "https://localhost:4317", wantTLS: true},
		{name: "bare insecure + tls config conflict", endpoint: "localhost:4317", insecure: true, hasTLSCfg: true, wantErr: true},
		{name: "http scheme is h2c", endpoint: "http://localhost:4317", wantBase: "http://localhost:4317"},
		{name: "http scheme wins over default-secure", endpoint: "http://localhost:4317", insecure: false, wantBase: "http://localhost:4317"},
		{name: "http scheme + WithInsecure redundant ok", endpoint: "http://localhost:4317", insecure: true, wantBase: "http://localhost:4317"},
		{name: "http scheme + tls config conflict", endpoint: "http://localhost:4317", hasTLSCfg: true, wantErr: true},
		{name: "https scheme", endpoint: "https://collector:4317", wantBase: "https://collector:4317", wantTLS: true},
		{name: "https scheme wins over WithInsecure", endpoint: "https://collector:4317", insecure: true, wantBase: "https://collector:4317", wantTLS: true},
		{name: "empty", endpoint: "", wantErr: true},
		{name: "path rejected", endpoint: "http://localhost:4317/v1/logs", wantErr: true},
		{name: "bare path rejected", endpoint: "localhost:4317/v1/logs", wantErr: true},
		{name: "query rejected", endpoint: "http://localhost:4317?x=1", wantErr: true},
		{name: "unsupported scheme", endpoint: "grpc://localhost:4317", wantErr: true},
		{name: "trailing slash tolerated", endpoint: "http://localhost:4317/", wantBase: "http://localhost:4317"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base, useTLS, err := resolveGRPCEndpoint(tc.endpoint, tc.insecure, tc.hasTLSCfg)
			if tc.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantBase, base)
			require.Equal(t, tc.wantTLS, useTLS)
		})
	}
}

func TestValidateGRPCHeaders(t *testing.T) {
	require.NoError(t, validateGRPCHeaders(map[string]string{"x-api-key": "k", "Authorization": "Bearer abc123 =/+~"}))
	for _, bad := range []string{"grpc-timeout", "Grpc-Encoding", "content-type", "TE", "x-data-bin", ":authority"} {
		require.Error(t, validateGRPCHeaders(map[string]string{bad: "v"}), bad)
	}
	// Values must be printable ASCII (gRPC ASCII-metadata rule) — a
	// deterministic config error must fail at construction, not send time
	// (plan-review pass-1 P1).
	for name, val := range map[string]string{
		"high byte":      "\x80",
		"utf8":           "ümlaut",
		"newline":        "a\nb",
		"control":        "a\x01b",
		"del":            "a\x7fb",
		"leading space":  " v",
		"trailing space": "v ",
	} {
		require.Error(t, validateGRPCHeaders(map[string]string{"x-ok-key": val}), name)
	}
}

// --- fake gRPC server (pure stdlib, h2c) ---

// fakeGRPCServer runs an h2c HTTP/2 server that de-frames Export requests and
// delegates the response to handler. It records every received request message.
type fakeGRPCServer struct {
	srv         *httptest.Server
	mu          sync.Mutex
	msgs        [][]byte
	conns       atomic.Int64
	connsClosed atomic.Int64
}

func (f *fakeGRPCServer) received() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([][]byte(nil), f.msgs...)
}

func newFakeGRPCServer(t *testing.T, handler http.HandlerFunc) *fakeGRPCServer {
	t.Helper()
	f := &fakeGRPCServer{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+grpcMethodPath, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if len(body) >= 5 {
			msg := body[5:]
			if body[0] == 1 && r.Header.Get("Grpc-Encoding") == "gzip" {
				if zr, err := gzip.NewReader(bytes.NewReader(msg)); err == nil {
					msg, _ = io.ReadAll(zr)
					_ = zr.Close()
				}
			}
			f.mu.Lock()
			f.msgs = append(f.msgs, append([]byte(nil), msg...))
			f.mu.Unlock()
		}
		handler(w, r)
	})
	f.srv = httptest.NewUnstartedServer(mux)
	f.srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			f.conns.Add(1)
		case http.StateClosed:
			f.connsClosed.Add(1)
		default:
		}
	}
	protos := new(http.Protocols)
	protos.SetHTTP1(true)
	protos.SetUnencryptedHTTP2(true)
	f.srv.Config.Protocols = protos
	f.srv.Start()
	t.Cleanup(f.srv.Close)

	return f
}

// okResponse writes a framed ExportLogsServiceResponse with trailers.
func okResponse(w http.ResponseWriter, respMsg []byte) {
	w.Header().Set("Content-Type", "application/grpc")
	w.Header().Set(http.TrailerPrefix+"Grpc-Status", "0")
	frame := make([]byte, 5, 5+len(respMsg))
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(respMsg)))
	_, _ = w.Write(append(frame, respMsg...))
}

// trailersOnlyError writes the status in the response HEADERS (no body) —
// exactly how grpc-go reports immediate errors.
func trailersOnlyError(w http.ResponseWriter, code int, msg, detailsBin string) {
	w.Header().Set("Content-Type", "application/grpc")
	w.Header().Set("Grpc-Status", strconv.Itoa(code))
	if msg != "" {
		w.Header().Set("Grpc-Message", msg)
	}
	if detailsBin != "" {
		w.Header().Set("Grpc-Status-Details-Bin", detailsBin)
	}
	w.WriteHeader(http.StatusOK)
}

func newTestGRPCTransport(t *testing.T, url string, opts ...Option) *grpcTransport {
	t.Helper()
	tr, err := newGRPCTransport(url, applyOptions(opts))
	require.NoError(t, err)

	return tr
}

// --- scenarios ---

func TestGRPCAttemptSuccessEmpty(t *testing.T) {
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) { okResponse(w, nil) })
	tr := newTestGRPCTransport(t, f.srv.URL)
	p := tr.prepare([]byte("payload"))
	accept, expErr := tr.attempt(p)
	require.Nil(t, expErr)
	require.Nil(t, accept)
	require.Equal(t, [][]byte{[]byte("payload")}, f.received())
}

func TestGRPCAttemptRequestHeaders(t *testing.T) {
	var got http.Header
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		okResponse(w, nil)
	})
	tr := newTestGRPCTransport(t, f.srv.URL, WithHeaders(map[string]string{"x-api-key": "k"}))
	_, expErr := tr.attempt(tr.prepare([]byte("m")))
	require.Nil(t, expErr)
	require.Equal(t, "application/grpc", got.Get("Content-Type"))
	require.Equal(t, "trailers", got.Get("Te"))
	require.Equal(t, "identity,gzip", got.Get("Grpc-Accept-Encoding"))
	require.Equal(t, "10000m", got.Get("Grpc-Timeout")) // default WithTimeout 10s
	require.Equal(t, "k", got.Get("X-Api-Key"))
	require.Contains(t, got.Get("User-Agent"), "zapwire-otlp")
}

func TestGRPCAttemptPartialSuccess(t *testing.T) {
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) {
		okResponse(w, psBody(3, "spike-partial"))
	})
	tr := newTestGRPCTransport(t, f.srv.URL)
	accept, expErr := tr.attempt(tr.prepare([]byte("m")))
	require.Nil(t, expErr)
	require.NotNil(t, accept)
	require.EqualValues(t, 3, accept.rejected)
	require.Equal(t, "spike-partial", accept.event.Message)
	require.Zero(t, accept.event.GRPCStatus)
}

func TestGRPCAttemptTrailersOnlyUnavailableWithRetryInfo(t *testing.T) {
	bin := base64.RawStdEncoding.EncodeToString(statusBin(14, "throttled", 7*time.Second, true))
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) {
		trailersOnlyError(w, 14, "try%20later", bin)
	})
	tr := newTestGRPCTransport(t, f.srv.URL)
	accept, expErr := tr.attempt(tr.prepare([]byte("m")))
	require.Nil(t, accept)
	require.NotNil(t, expErr)
	require.Equal(t, 14, expErr.GRPCStatus)
	require.True(t, expErr.Retryable)
	require.Equal(t, "try later", expErr.Message) // percent-decoded
	require.Equal(t, 7*time.Second, expErr.retryAfter)
	require.Zero(t, expErr.StatusCode) // gRPC errors ride HTTP 200
}

func TestGRPCAttemptResourceExhausted(t *testing.T) {
	t.Run("without RetryInfo is terminal", func(t *testing.T) {
		f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) {
			trailersOnlyError(w, 8, "quota", "")
		})
		tr := newTestGRPCTransport(t, f.srv.URL)
		_, expErr := tr.attempt(tr.prepare([]byte("m")))
		require.NotNil(t, expErr)
		require.False(t, expErr.Retryable)
	})
	t.Run("with RetryInfo is retryable", func(t *testing.T) {
		bin := base64.StdEncoding.EncodeToString(statusBin(8, "quota", 3*time.Second, true)) // padded variant on purpose
		f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) {
			trailersOnlyError(w, 8, "quota", bin)
		})
		tr := newTestGRPCTransport(t, f.srv.URL)
		_, expErr := tr.attempt(tr.prepare([]byte("m")))
		require.NotNil(t, expErr)
		require.True(t, expErr.Retryable)
		require.Equal(t, 3*time.Second, expErr.retryAfter)
	})
}

func TestGRPCAttemptInvalidArgument(t *testing.T) {
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) {
		trailersOnlyError(w, 3, "bad", "")
	})
	tr := newTestGRPCTransport(t, f.srv.URL)
	_, expErr := tr.attempt(tr.prepare([]byte("m")))
	require.NotNil(t, expErr)
	require.Equal(t, 3, expErr.GRPCStatus)
	require.False(t, expErr.Retryable)
}

func TestGRPCAttemptStatusInRealTrailers(t *testing.T) {
	// Error status delivered via genuine trailers AFTER a body write — the
	// trailer-first resolution path (not trailers-only).
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set(http.TrailerPrefix+"Grpc-Status", "14")
		w.Header().Set(http.TrailerPrefix+"Grpc-Message", "drain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte{}) // headers flushed, no message frame
	})
	tr := newTestGRPCTransport(t, f.srv.URL)
	_, expErr := tr.attempt(tr.prepare([]byte("m")))
	require.NotNil(t, expErr)
	require.Equal(t, 14, expErr.GRPCStatus)
	require.True(t, expErr.Retryable)
}

func TestGRPCAttemptNoGRPCStatusFallsBackToHTTPMapping(t *testing.T) {
	// A non-gRPC intermediary (plain 503, no grpc-status anywhere).
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("upstream down"))
	})
	tr := newTestGRPCTransport(t, f.srv.URL)
	_, expErr := tr.attempt(tr.prepare([]byte("m")))
	require.NotNil(t, expErr)
	require.Equal(t, http.StatusServiceUnavailable, expErr.StatusCode)
	require.Equal(t, grpcUnavailable, expErr.GRPCStatus)
	require.True(t, expErr.Retryable)
	require.Contains(t, expErr.Message, "upstream down")
}

func TestGRPCPrepareGzip(t *testing.T) {
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) { okResponse(w, nil) })
	tr := newTestGRPCTransport(t, f.srv.URL, WithCompression(Gzip))
	p := tr.prepare([]byte("compress me"))
	require.True(t, p.compressed)
	require.Nil(t, p.warn)
	require.Equal(t, byte(1), p.body[0]) // frame compressed-flag
	accept, expErr := tr.attempt(p)
	require.Nil(t, expErr)
	require.Nil(t, accept)
	require.Equal(t, [][]byte{[]byte("compress me")}, f.received()) // server-side gunzip round-trip
}

func TestGRPCGzippedResponseFrame(t *testing.T) {
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) {
		var gz bytes.Buffer
		zw := gzip.NewWriter(&gz)
		_, _ = zw.Write(psBody(2, "gz-partial"))
		_ = zw.Close()
		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set("Grpc-Encoding", "gzip")
		w.Header().Set(http.TrailerPrefix+"Grpc-Status", "0")
		frame := make([]byte, 5, 5+gz.Len())
		frame[0] = 1
		binary.BigEndian.PutUint32(frame[1:5], uint32(gz.Len()))
		_, _ = w.Write(append(frame, gz.Bytes()...))
	})
	tr := newTestGRPCTransport(t, f.srv.URL)
	accept, expErr := tr.attempt(tr.prepare([]byte("m")))
	require.Nil(t, expErr)
	require.NotNil(t, accept)
	require.EqualValues(t, 2, accept.rejected)
	require.Equal(t, "gz-partial", accept.event.Message)
}

func TestGRPCMalformedResponseFrameIsObservabilityOnly(t *testing.T) {
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set(http.TrailerPrefix+"Grpc-Status", "0")
		_, _ = w.Write([]byte{0x00, 0xff}) // truncated frame on an OK status
	})
	tr := newTestGRPCTransport(t, f.srv.URL)
	accept, expErr := tr.attempt(tr.prepare([]byte("m")))
	require.Nil(t, expErr) // server said OK: batch is accepted
	require.NotNil(t, accept)
	require.Zero(t, accept.rejected)
	require.Error(t, accept.event.Err)
}

func TestGRPCAttemptTLS(t *testing.T) {
	// https + WithTLSConfig path over stdlib ALPN h2.
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+grpcMethodPath, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		okResponse(w, nil)
	})
	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	tr := newTestGRPCTransport(t, srv.URL, WithTLSConfig(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}))
	accept, expErr := tr.attempt(tr.prepare([]byte("m")))
	require.Nil(t, expErr)
	require.Nil(t, accept)
}

func TestGRPCConnectionReuse(t *testing.T) {
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) { okResponse(w, nil) })
	tr := newTestGRPCTransport(t, f.srv.URL)
	for i := range 3 {
		_, expErr := tr.attempt(tr.prepare([]byte("m")))
		require.Nil(t, expErr, "attempt %d", i)
	}
	// One h2c connection serves all requests (stdlib pools per-host).
	require.Len(t, f.received(), 3)
	require.EqualValues(t, 1, f.conns.Load(), "all requests must reuse one h2c connection")
}

// flakyRT fails the first n round trips, then delegates.
type flakyRT struct {
	mu    sync.Mutex
	fails int
	inner http.RoundTripper
}

func (f *flakyRT) RoundTrip(r *http.Request) (*http.Response, error) {
	f.mu.Lock()
	fail := f.fails > 0
	if fail {
		f.fails--
	}
	f.mu.Unlock()
	if fail {
		return nil, errors.New("connection reset by peer (injected)")
	}

	return f.inner.RoundTrip(r)
}

func TestGRPCTransportErrorRetryable(t *testing.T) {
	// P0 (plan-review pass 1): dial/reset failures must be RETRYABLE
	// (UNAVAILABLE), unlike the HTTP path's terminal transport errors.
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) { okResponse(w, nil) })
	tr := newTestGRPCTransport(t, f.srv.URL)
	tr.client.Transport = &flakyRT{fails: 1, inner: tr.client.Transport}
	_, expErr := tr.attempt(tr.prepare([]byte("m")))
	require.NotNil(t, expErr)
	require.True(t, expErr.Retryable)
	require.Equal(t, grpcUnavailable, expErr.GRPCStatus)
	require.Error(t, expErr.Err)
}

func TestGRPCDeadlineExceededRetryable(t *testing.T) {
	blocked := make(chan struct{})
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) {
		<-blocked // hold the request past the client timeout
		okResponse(w, nil)
	})
	// Registered AFTER newFakeGRPCServer's cleanup, so LIFO order unblocks the handler before Server.Close.
	t.Cleanup(func() { close(blocked) })
	tr := newTestGRPCTransport(t, f.srv.URL, WithTimeout(50*time.Millisecond))
	_, expErr := tr.attempt(tr.prepare([]byte("m")))
	require.NotNil(t, expErr)
	require.True(t, expErr.Retryable)
	require.Equal(t, grpcDeadlineExceeded, expErr.GRPCStatus)
}

func TestGRPCBodyReadFailureRetryable(t *testing.T) {
	// Partial body then an aborted stream: trailers are unreliable, so the
	// attempt must come back retryable, NOT fall through to the no-status
	// HTTP-mapping path.
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/grpc")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte{0x00, 0x00, 0x00, 0x00, 0xff}) // claims more bytes than sent
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
		panic(http.ErrAbortHandler) // RST_STREAM mid-body
	})
	tr := newTestGRPCTransport(t, f.srv.URL)
	_, expErr := tr.attempt(tr.prepare([]byte("m")))
	require.NotNil(t, expErr)
	require.True(t, expErr.Retryable)
	require.Equal(t, grpcUnavailable, expErr.GRPCStatus)
}

func TestGRPCWriterRetriesTransportError(t *testing.T) {
	// Writer-level proof of the §6.5 promise: first-attempt connection
	// failure → retry → success, zero drops.
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) { okResponse(w, nil) })
	o := applyOptions([]Option{WithRetry(RetryConfig{Initial: 10 * time.Millisecond, MaxInterval: 50 * time.Millisecond, MaxElapsed: 30 * time.Second})})
	tr, err := newGRPCTransport(f.srv.URL, o)
	require.NoError(t, err)
	tr.client.Transport = &flakyRT{fails: 1, inner: tr.client.Transport}
	w := newWriterCore(tr, o)
	go w.run()
	t.Cleanup(func() { _ = w.Close() })

	_, _ = w.Write([]byte("r"))
	require.NoError(t, w.Sync())
	require.Zero(t, w.DroppedLogs())
	require.Len(t, f.received(), 1)
}

func TestGRPCWriterCloseReleasesConnection(t *testing.T) {
	// Close must release the private HTTP/2 client/transport: after Close,
	// the server must observe the connection transition to StateClosed.
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) { okResponse(w, nil) })
	w, err := NewGRPCWriter(f.srv.URL, WithInsecure())
	require.NoError(t, err)

	_, _ = w.Write([]byte("r"))
	require.NoError(t, w.Sync())
	require.Len(t, f.received(), 1)

	require.NoError(t, w.Close())
	require.Eventually(t, func() bool { return f.connsClosed.Load() >= 1 },
		5*time.Second, 10*time.Millisecond,
		"Close must release the private HTTP/2 connection")
}

func TestGRPCTrailingBytesAfterFrameIsObservabilityOnly(t *testing.T) {
	// A valid empty frame followed by one trailing byte: server accepted the
	// batch, but the malformed body is an observability-only event (P1 fix).
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set(http.TrailerPrefix+"Grpc-Status", "0")
		// 5-byte valid empty frame (compressed=0, length=0) + 1 trailing byte
		_, _ = w.Write([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0xff})
	})
	tr := newTestGRPCTransport(t, f.srv.URL)
	accept, expErr := tr.attempt(tr.prepare([]byte("m")))
	require.Nil(t, expErr) // server said OK: batch is accepted
	require.NotNil(t, accept)
	require.Zero(t, accept.rejected)
	require.Error(t, accept.event.Err) // trailing bytes surface as observability event
}
