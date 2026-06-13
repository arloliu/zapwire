package otlp

import (
	"compress/gzip"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// fastRetry keeps tests quick while exercising the real loop.
var fastRetry = RetryConfig{Initial: time.Millisecond, MaxInterval: 4 * time.Millisecond, MaxElapsed: 100 * time.Millisecond}

// newTestWriter builds an un-started Writer (no flush goroutine) pointed at
// url. Defaults are PREPENDED so caller-supplied options (e.g. a custom
// WithRetry) win — options apply in slice order.
func newTestWriter(t *testing.T, url string, opts ...Option) (*Writer, *[]error) {
	t.Helper()
	var errs []error
	opts = append([]Option{
		WithRetry(fastRetry),
		WithErrorHandler(func(e error) { errs = append(errs, e) }),
	}, opts...)
	o := applyOptions(opts)
	tr, err := newHTTPTransport(url, o)
	require.NoError(t, err)
	w := newWriterCore(tr, o)
	t.Cleanup(w.cancel)

	return w, &errs
}

func TestExportSuccess(t *testing.T) {
	var gotBody []byte
	var gotCT string
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		gotCT = r.Header.Get("Content-Type")
		gotBody, _ = io.ReadAll(r.Body)
		rw.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	w, errs := newTestWriter(t, srv.URL)
	w.export([][]byte{{0x10, 0x09}}, true, time.Time{})
	require.Empty(t, *errs)
	require.Zero(t, w.DroppedLogs())
	require.Equal(t, "application/x-protobuf", gotCT)
	require.Equal(t, w.env.sizeFor(w.env.recordCost(2)), len(gotBody))
}

func TestExportRetriesThenSucceeds(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if hits.Add(1) < 3 {
			rw.WriteHeader(http.StatusServiceUnavailable)

			return
		}
		rw.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	w, _ := newTestWriter(t, srv.URL)
	w.export([][]byte{{0x10, 0x09}}, true, time.Time{})
	require.Equal(t, int32(3), hits.Load())
	require.Zero(t, w.DroppedLogs())
}

func TestExport4xxNeverRetried(t *testing.T) {
	for _, status := range []int{400, 413} {
		t.Run(fmt.Sprintf("%d", status), func(t *testing.T) {
			var hits atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				rw.WriteHeader(status)
			}))
			defer srv.Close()

			w, errs := newTestWriter(t, srv.URL)
			w.export([][]byte{{0x10, 0x09}, {0x10, 0x05}}, true, time.Time{})
			require.Equal(t, int32(1), hits.Load())
			require.Equal(t, uint64(2), w.DroppedLogs()) // whole batch counted
			require.Len(t, *errs, 1)
			var ee *ExportError
			require.ErrorAs(t, (*errs)[0], &ee)
			require.Equal(t, status, ee.StatusCode)
			require.False(t, ee.Retryable)
		})
	}
}

func TestExportRetryBudgetExhausted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	w, errs := newTestWriter(t, srv.URL)
	start := time.Now()
	w.export([][]byte{{0x10, 0x09}}, true, time.Time{})
	require.Less(t, time.Since(start), time.Second)
	require.Equal(t, uint64(1), w.DroppedLogs())
	require.NotEmpty(t, *errs)
}

func TestExportRetryAfterBeyondBudgetGivesUp(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		rw.Header().Set("Retry-After", "3600")
		rw.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	w, _ := newTestWriter(t, srv.URL)
	start := time.Now()
	w.export([][]byte{{0x10, 0x09}}, true, time.Time{})
	require.Equal(t, int32(1), hits.Load(), "must give up immediately")
	require.Less(t, time.Since(start), time.Second)
	require.Equal(t, uint64(1), w.DroppedLogs())
}

func TestExportNoRetryWhenDisallowed(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		rw.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	w, _ := newTestWriter(t, srv.URL)
	w.export([][]byte{{0x10, 0x09}}, false, time.Time{}) // Close-drain mode (§5.4)
	require.Equal(t, int32(1), hits.Load())
	require.Equal(t, uint64(1), w.DroppedLogs())
}

func TestExportCancelledDuringBackoff(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Retry-After", "30") // would sleep 30s
		rw.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	w, _ := newTestWriter(t, srv.URL, WithRetry(RetryConfig{Initial: time.Millisecond, MaxInterval: time.Second, MaxElapsed: time.Hour}))
	go func() {
		time.Sleep(20 * time.Millisecond)
		w.cancel()
	}()
	start := time.Now()
	w.export([][]byte{{0x10, 0x09}}, true, time.Time{})
	require.Less(t, time.Since(start), 5*time.Second, "Close must abort the backoff sleep")
	require.Equal(t, uint64(1), w.DroppedLogs()) // counted exactly once (§5.4)
}

func TestPartialSuccessHandling(t *testing.T) {
	// rejected=2 + message → drops counted, handler notified, NO retry.
	respRejected := []byte{0x0a, 0x07, 0x08, 0x02, 0x12, 0x03, 'b', 'a', 'd'}
	// rejected=0 + message → warning only.
	respWarning := []byte{0x0a, 0x05, 0x12, 0x03, 'h', 'm', 'm'}

	for _, tc := range []struct {
		name    string
		resp    []byte
		dropped uint64
		warning bool
	}{
		{"rejected", respRejected, 2, false},
		{"warning", respWarning, 0, true},
		{"clean", nil, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var hits atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(tc.resp)
			}))
			defer srv.Close()

			w, errs := newTestWriter(t, srv.URL)
			w.export([][]byte{{0x10, 0x09}, {0x10, 0x05}, {0x10, 0x0d}}, true, time.Time{})
			require.Equal(t, int32(1), hits.Load(), "partial success must NOT retry")
			require.Equal(t, tc.dropped, w.DroppedLogs())
			if tc.dropped > 0 || tc.warning {
				require.Len(t, *errs, 1)
				var ee *ExportError
				require.ErrorAs(t, (*errs)[0], &ee)
				require.Equal(t, tc.warning, ee.Warning)
			} else {
				require.Empty(t, *errs)
			}
		})
	}
}

func TestGzipAndHeaders(t *testing.T) {
	var gotEnc, gotKey string
	var plain []byte
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		gotEnc = r.Header.Get("Content-Encoding")
		gotKey = r.Header.Get("x-api-key")
		zr, err := gzip.NewReader(r.Body)
		if err == nil {
			plain, _ = io.ReadAll(zr)
		}
		rw.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	w, _ := newTestWriter(t, srv.URL, WithCompression(Gzip), WithHeaders(map[string]string{"x-api-key": "k"}))
	w.export([][]byte{{0x10, 0x09}}, true, time.Time{})
	require.Equal(t, "gzip", gotEnc)
	require.Equal(t, "k", gotKey)
	require.Equal(t, w.env.sizeFor(w.env.recordCost(2)), len(plain))
}

func TestParseRetryAfter(t *testing.T) {
	require.Equal(t, 7*time.Second, parseRetryAfter("7"))
	require.Zero(t, parseRetryAfter(""))
	require.Zero(t, parseRetryAfter("garbage"))
	// HTTP-date in the future (~1 minute): allow generous slack.
	d := parseRetryAfter(time.Now().Add(time.Minute).UTC().Format(http.TimeFormat))
	require.Greater(t, d, 30*time.Second)
	// HTTP-date in the past → 0.
	require.Zero(t, parseRetryAfter(time.Now().Add(-time.Minute).UTC().Format(http.TimeFormat)))
}

func TestNewGRPCWriterValidation(t *testing.T) {
	_, err := NewGRPCWriter("")
	require.ErrorIs(t, err, ErrNoEndpoint)

	_, err = NewGRPCWriter("http://localhost:4317/v1/logs")
	require.Error(t, err) // path rejected

	_, err = NewGRPCWriter("localhost:4317", WithInsecure(), WithTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	require.Error(t, err) // insecure × TLS conflict

	_, err = NewGRPCWriter("localhost:4317", WithInsecure(), WithHeaders(map[string]string{"grpc-timeout": "1S"}))
	require.Error(t, err) // reserved header
}

func TestNewGRPCWriterEndToEnd(t *testing.T) {
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) { okResponse(w, nil) })
	w, err := NewGRPCWriter(f.srv.URL) // http:// scheme → h2c
	require.NoError(t, err)
	_, _ = w.Write([]byte("rec"))
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())
	require.Zero(t, w.DroppedLogs())
	require.Len(t, f.received(), 1)
}

func TestNewHTTPWriterIsNewWriter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	w, err := NewHTTPWriter(srv.URL)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	_, err = NewHTTPWriter("")
	require.ErrorIs(t, err, ErrNoEndpoint)
}

func TestNewGRPCCore(t *testing.T) {
	f := newFakeGRPCServer(t, func(w http.ResponseWriter, _ *http.Request) { okResponse(w, nil) })
	core, w, err := NewGRPCCore(f.srv.URL, zapcore.InfoLevel)
	require.NoError(t, err)
	logger := zap.New(core)
	logger.Info("hello grpc")
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())
	require.Len(t, f.received(), 1)

	_, _, err = NewGRPCCore("", zapcore.InfoLevel)
	require.ErrorIs(t, err, ErrNoEndpoint)
}
