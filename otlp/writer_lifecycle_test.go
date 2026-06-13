package otlp

import (
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// reqCounter is an httptest handler counting requests and their record counts.
type reqCounter struct {
	mu         sync.Mutex
	requests   int
	records    []int // records per request, via counting 0x12 entries in ScopeLogs
	status     atomic.Int32
	retryAfter atomic.Value // string
}

func (rc *reqCounter) handler() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		rc.mu.Lock()
		rc.requests++
		rc.records = append(rc.records, countRecords(body))
		rc.mu.Unlock()
		if ra, _ := rc.retryAfter.Load().(string); ra != "" {
			rw.Header().Set("Retry-After", ra)
		}
		st := int(rc.status.Load())
		if st == 0 {
			st = http.StatusOK
		}
		rw.WriteHeader(st)
	}
}

func (rc *reqCounter) snapshot() (int, []int) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	return rc.requests, append([]int(nil), rc.records...)
}

// countRecords walks request → resource_logs(1) → scope_logs(2) → counts
// log_records(2) entries. Implement with the Task 5 top-level field walker.
func countRecords(req []byte) int {
	rl, _ := findField(req, 1)
	sl, _ := findField(rl, 2)
	n := 0
	b := sl
	for len(b) > 0 {
		tag, tn := uvarint(b)
		b = b[tn:]
		l, ln := uvarint(b)
		if int(tag>>3) == 2 {
			n++
		}
		b = b[ln+int(l):]
	}

	return n
}

func newStartedWriter(t *testing.T, url string, opts ...Option) *Writer {
	t.Helper()
	opts = append([]Option{WithRetry(fastRetry)}, opts...)
	w, err := NewHTTPWriter(url, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	return w
}

func TestNewWriterValidation(t *testing.T) {
	_, err := NewHTTPWriter("")
	require.ErrorIs(t, err, ErrNoEndpoint)
	_, err = NewHTTPWriter("not-a-url")
	require.Error(t, err)
}

func TestBatchByCount(t *testing.T) {
	rc := &reqCounter{}
	srv := httptest.NewServer(rc.handler())
	defer srv.Close()

	w := newStartedWriter(t, srv.URL, WithBatchSize(2), WithFlushInterval(time.Hour))
	for range 4 {
		_, _ = w.Write([]byte{0x10, 0x09})
	}
	require.NoError(t, w.Sync())
	reqs, recs := rc.snapshot()
	require.Equal(t, 2, reqs)
	require.Equal(t, []int{2, 2}, recs)
}

func TestBatchByBytes(t *testing.T) {
	rc := &reqCounter{}
	srv := httptest.NewServer(rc.handler())
	defer srv.Close()

	// Envelope overhead + one ~200B record exceeds 300 bytes → one record per
	// request despite batchSize 512. Records are opaque to the writer; any
	// byte content works for batching tests.
	w := newStartedWriter(t, srv.URL, WithMaxRequestBytes(300), WithFlushInterval(time.Hour))
	rec := make([]byte, 200)
	for range 3 {
		_, _ = w.Write(rec)
	}
	require.NoError(t, w.Sync())
	reqs, recs := rc.snapshot()
	require.Equal(t, 3, reqs)
	require.Equal(t, []int{1, 1, 1}, recs)
}

func TestBatchByInterval(t *testing.T) {
	rc := &reqCounter{}
	srv := httptest.NewServer(rc.handler())
	defer srv.Close()

	w := newStartedWriter(t, srv.URL, WithFlushInterval(20*time.Millisecond))
	_, _ = w.Write([]byte{0x10, 0x09})
	require.Eventually(t, func() bool {
		n, _ := rc.snapshot()

		return n == 1
	}, 2*time.Second, 5*time.Millisecond)
	_ = w // no Sync — the ticker must have flushed
}

func TestOversizedRecordDroppedAtWrite(t *testing.T) {
	rc := &reqCounter{}
	srv := httptest.NewServer(rc.handler())
	defer srv.Close()

	var errs atomic.Int32
	w := newStartedWriter(t, srv.URL, WithMaxRequestBytes(128),
		WithErrorHandler(func(error) { errs.Add(1) }))
	n, err := w.Write(make([]byte, 4096))
	require.NoError(t, err) // zap multi-writer contract
	require.Equal(t, 4096, n)
	require.Equal(t, uint64(1), w.DroppedLogs())
	require.Equal(t, int32(1), errs.Load())
	require.NoError(t, w.Sync())
	reqs, _ := rc.snapshot()
	require.Zero(t, reqs)
}

func TestQueueFullDropPolicies(t *testing.T) {
	// Stalled server: the flush goroutine blocks in export while the queue
	// fills. release unblocks it at test end. Both policies covered.
	for _, policy := range []DropPolicy{DropNewest, DropOldest} {
		t.Run(map[DropPolicy]string{DropNewest: "newest", DropOldest: "oldest"}[policy], func(t *testing.T) {
			release := make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				<-release
				rw.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()
			defer close(release)

			w := newStartedWriter(t, srv.URL, WithQueueSize(2), WithBatchSize(1),
				WithFlushInterval(time.Millisecond), WithDropPolicy(policy))
			for i := range 10 {
				_, _ = w.Write([]byte{0x10, byte(i)})
			}
			require.Eventually(t, func() bool { return w.DroppedLogs() > 0 },
				2*time.Second, time.Millisecond, "queue overflow must surface as counted drops")
		})
	}
}

func TestSyncDuringRetryBackoffReturns(t *testing.T) {
	rc := &reqCounter{}
	rc.status.Store(http.StatusServiceUnavailable)
	srv := httptest.NewServer(rc.handler())
	defer srv.Close()

	w := newStartedWriter(t, srv.URL, WithBatchSize(1), WithFlushInterval(time.Millisecond))
	_, _ = w.Write([]byte{0x10, 0x09})
	start := time.Now()
	require.NoError(t, w.Sync()) // waits ≤ retry budget (fastRetry: 100ms) + slack
	require.Less(t, time.Since(start), 5*time.Second)
	require.Equal(t, uint64(1), w.DroppedLogs())
}

func TestSyncMultiBatchByteCutAgainstRetryingBackend(t *testing.T) {
	// Design §9: multiple pending batches under a tiny WithMaxRequestBytes
	// (one record per request) against a retrying backend — Sync resolves
	// EVERY batch within the per-batch documented bound, and accounting is
	// exact (all records dropped after each batch's budget).
	rc := &reqCounter{}
	rc.status.Store(http.StatusServiceUnavailable)
	srv := httptest.NewServer(rc.handler())
	defer srv.Close()

	w := newStartedWriter(t, srv.URL, WithMaxRequestBytes(300), WithFlushInterval(time.Hour))
	rec := make([]byte, 200) // one record per batch via byte cutting
	for range 3 {
		_, _ = w.Write(rec)
	}
	start := time.Now()
	require.NoError(t, w.Sync())
	// 3 batches × (fastRetry budget 100ms + attempt) — generous CI slack.
	require.Less(t, time.Since(start), 10*time.Second)
	require.Equal(t, uint64(3), w.DroppedLogs(), "every byte-cut batch resolved and counted")

	// The oracle that proves byte cutting (pass-2 P1): the retrying backend
	// saw ≥ 3 requests (each batch retried within its own budget) and EVERY
	// request carried exactly ONE record — a broken implementation shipping
	// all three records as a single retrying batch fails here.
	reqs, recs := rc.snapshot()
	require.GreaterOrEqual(t, reqs, 3)
	for i, n := range recs {
		require.Equal(t, 1, n, "request %d must carry exactly one byte-cut record", i)
	}
}

func TestCloseDuringRetryAfterSleep(t *testing.T) {
	rc := &reqCounter{}
	rc.status.Store(http.StatusServiceUnavailable)
	rc.retryAfter.Store("30") // 30s sleep without cancellation
	srv := httptest.NewServer(rc.handler())
	defer srv.Close()

	w, err := NewHTTPWriter(srv.URL,
		WithRetry(RetryConfig{Initial: time.Millisecond, MaxInterval: time.Second, MaxElapsed: time.Hour}),
		WithBatchSize(1), WithFlushInterval(time.Millisecond))
	require.NoError(t, err)
	_, _ = w.Write([]byte{0x10, 0x09})
	time.Sleep(50 * time.Millisecond) // let the batch enter retry

	start := time.Now()
	require.NoError(t, w.Close())
	require.Less(t, time.Since(start), 5*time.Second, "Close must abort the Retry-After sleep")
	require.Equal(t, uint64(1), w.DroppedLogs(), "cancelled batch counted exactly once")
}

func TestCloseDrainsSingleAttempt(t *testing.T) {
	rc := &reqCounter{}
	srv := httptest.NewServer(rc.handler())
	defer srv.Close()

	w, err := NewHTTPWriter(srv.URL, WithRetry(fastRetry), WithBatchSize(2), WithFlushInterval(time.Hour))
	require.NoError(t, err)
	for range 5 {
		_, _ = w.Write([]byte{0x10, 0x09})
	}
	require.NoError(t, w.Close())
	reqs, recs := rc.snapshot()
	require.GreaterOrEqual(t, reqs, 3) // 2+2+1
	total := 0
	for _, n := range recs {
		total += n
	}
	require.Equal(t, 5, total, "Close drain must export everything")
}

func TestCloseDrainByteCutAgainstFailingBackend(t *testing.T) {
	// Design §9, deterministic shape (pass-2 P1): the FIRST request blocks
	// in-flight while four more records queue behind the busy flush
	// goroutine; Close must (a) let the in-flight attempt finish within its
	// timeout, then (b) drain the queue as byte-cut single-record batches
	// with exactly ONE attempt each (no retries during close), every record
	// counted dropped exactly once.
	entered := make(chan struct{})
	release := make(chan struct{})
	var first atomic.Bool
	first.Store(true)
	rc := &reqCounter{}
	rc.status.Store(http.StatusServiceUnavailable)
	base := rc.handler()
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if first.CompareAndSwap(true, false) {
			close(entered)
			<-release
		}
		base(rw, r)
	}))
	defer srv.Close()

	w, err := NewHTTPWriter(srv.URL, WithRetry(fastRetry), WithQueueSize(8),
		WithMaxRequestBytes(300), WithFlushInterval(time.Millisecond),
		WithTimeout(5*time.Second))
	require.NoError(t, err)
	rec := make([]byte, 200)
	_, _ = w.Write(rec)
	<-entered // batch 1 is in-flight; the flush goroutine is busy in export
	for range 4 {
		_, _ = w.Write(rec) // queue behind the blocked export
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(release) // server answers 503 while Close waits
	}()
	start := time.Now()
	require.NoError(t, w.Close())
	require.Less(t, time.Since(start), 10*time.Second)

	reqs, recs := rc.snapshot()
	require.Equal(t, 5, reqs, "1 in-flight attempt + 4 single-attempt drain batches")
	for i, n := range recs {
		require.Equal(t, 1, n, "request %d must carry exactly one byte-cut record", i)
	}
	require.Equal(t, uint64(5), w.DroppedLogs(), "every record counted dropped exactly once")
}

func TestErrorHandlerMayCallClose(t *testing.T) {
	// Pass-2 P0 pin: the error handler runs outside the admission lock, so a
	// handler that calls Close must not self-deadlock.
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	var w *Writer
	var err error
	w, err = NewHTTPWriter(srv.URL, WithRetry(fastRetry), WithMaxRequestBytes(128),
		WithErrorHandler(func(error) { _ = w.Close() }))
	require.NoError(t, err)
	donec := make(chan struct{})
	go func() {
		_, _ = w.Write(make([]byte, 4096)) // oversized → handler → Close
		close(donec)
	}()
	select {
	case <-donec:
	case <-time.After(10 * time.Second):
		t.Fatal("Write deadlocked against Close inside the error handler")
	}
	require.Equal(t, uint64(1), w.DroppedLogs())
}

func TestCloseWhileRequestInFlight(t *testing.T) {
	// Design §9/§5.4: the in-flight HTTP attempt is allowed to finish within
	// its per-attempt timeout — Close must not cancel a request the server
	// may have already accepted.
	entered := make(chan struct{})
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		rw.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	w, err := NewHTTPWriter(srv.URL, WithRetry(fastRetry),
		WithBatchSize(1), WithFlushInterval(time.Millisecond), WithTimeout(5*time.Second))
	require.NoError(t, err)
	_, _ = w.Write([]byte{0x10, 0x09})
	<-entered // request is in flight
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(release) // server responds 200 while Close is waiting
	}()
	require.NoError(t, w.Close())
	require.Zero(t, w.DroppedLogs(), "in-flight attempt completed; record delivered, not dropped")
}

func TestPostCloseWritesUncounted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	w, err := NewHTTPWriter(srv.URL, WithRetry(fastRetry))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.NoError(t, w.Close()) // idempotent
	n, werr := w.Write([]byte{0x10, 0x09})
	require.NoError(t, werr)
	require.Equal(t, 2, n)
	require.Zero(t, w.DroppedLogs()) // §5.1: silently discarded, uncounted
	require.NoError(t, w.Sync())     // post-close Sync is a nil no-op
}

func TestConcurrentWriteSyncClose(t *testing.T) {
	// Run against BOTH a healthy and a sustained-failure backend (design §9)
	// under -race: exercises the admit gate, retry cancellation, and drain.
	for _, status := range []int{http.StatusOK, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				rw.WriteHeader(status)
			}))
			defer srv.Close()

			w, err := NewHTTPWriter(srv.URL, WithRetry(fastRetry), WithFlushInterval(time.Millisecond))
			require.NoError(t, err)
			var wg sync.WaitGroup
			for range 8 {
				wg.Go(func() {
					for j := range 100 {
						_, _ = w.Write([]byte{0x10, 0x09})
						if j%10 == 0 {
							_ = w.Sync()
						}
					}
				})
			}
			time.Sleep(10 * time.Millisecond)
			_ = w.Close()
			wg.Wait()
		})
	}
}

func TestNewCoreEndToEnd(t *testing.T) {
	rc := &reqCounter{}
	srv := httptest.NewServer(rc.handler())
	defer srv.Close()

	core, w, err := NewHTTPCore(srv.URL, zapcore.InfoLevel, WithServiceName("e2e"), WithRetry(fastRetry))
	require.NoError(t, err)
	logger := zap.New(core)
	_, ctx := testSpanContext(t)
	logger.Info("hello", zap.String("k", "v"), SpanContext(ctx))
	logger.Debug("filtered-out")
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())
	reqs, recs := rc.snapshot()
	require.Equal(t, 1, reqs)
	require.Equal(t, []int{1}, recs)
}

// grpcLifecycleWriter builds a started gRPC writer against a fake server.
func grpcLifecycleWriter(t *testing.T, handler http.HandlerFunc, opts ...Option) (*Writer, *fakeGRPCServer) {
	t.Helper()
	f := newFakeGRPCServer(t, handler)
	w, err := NewGRPCWriter(f.srv.URL, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	return w, f
}

func TestGRPCSyncBarrierFlushesAll(t *testing.T) {
	w, f := grpcLifecycleWriter(t, func(w http.ResponseWriter, _ *http.Request) { okResponse(w, nil) },
		WithBatchSize(2), WithFlushInterval(time.Hour)) // ticker out of the picture
	for range 5 {
		_, _ = w.Write([]byte("r"))
	}
	require.NoError(t, w.Sync())
	require.Zero(t, w.DroppedLogs())
	require.GreaterOrEqual(t, len(f.received()), 3) // 5 records at batch size 2 → ≥3 exports
}

func TestGRPCRetryInfoDelayHonored(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	w, f := grpcLifecycleWriter(t, func(rw http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		calls++
		first := calls == 1
		mu.Unlock()
		if first {
			det := base64.RawStdEncoding.EncodeToString(statusBin(14, "throttled", 50*time.Millisecond, true))
			trailersOnlyError(rw, 14, "throttled", det)

			return
		}
		okResponse(rw, nil)
	}, WithRetry(RetryConfig{Initial: time.Hour, MaxInterval: time.Hour, MaxElapsed: time.Hour}))
	// Initial backoff is 1h — only the 50ms RetryInfo delay can make the
	// retry happen within the test budget.
	start := time.Now()
	_, _ = w.Write([]byte("r"))
	require.NoError(t, w.Sync())
	require.Zero(t, w.DroppedLogs())
	require.Less(t, time.Since(start), 10*time.Second)
	require.Len(t, f.received(), 2) // first attempt + post-RetryInfo retry
}

func TestGRPCCloseDrainsSingleAttempt(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	w, _ := grpcLifecycleWriter(t, func(rw http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		calls++
		mu.Unlock()
		trailersOnlyError(rw, 14, "down", "") // retryable — but Close drain is single-attempt
	}, WithRetry(RetryConfig{Initial: time.Hour, MaxInterval: time.Hour, MaxElapsed: time.Hour}))
	_, _ = w.Write([]byte("r"))
	require.NoError(t, w.Close())
	require.EqualValues(t, 1, w.DroppedLogs())
	mu.Lock()
	defer mu.Unlock()
	require.LessOrEqual(t, calls, 2) // flush-tick attempt at most once + drain attempt
}
