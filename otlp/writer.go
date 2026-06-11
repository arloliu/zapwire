package otlp

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap/zapcore"
)

// Writer is the OTLP/HTTP exporter: a zapcore.WriteSyncer with a bounded
// queue, a single flush goroutine, and OTLP retry semantics (design §5).
// Async-only — Sync() is the flush barrier.
type Writer struct {
	endpoint    string
	client      *http.Client
	headers     map[string]string
	compression Compression
	timeout     time.Duration
	retry       RetryConfig
	maxBytes    int
	batchSize   int
	flushEvery  time.Duration
	dropPolicy  DropPolicy
	errFn       func(error)
	env         *envelope

	queue    chan []byte
	flushReq chan chan struct{}
	done     chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc

	// admit closes the closed-check→enqueue window: Write holds it shared
	// across check+send; Close takes it exclusively after setting closed, so
	// no enqueue can land after the final drain starts (the root writer's
	// lifecycle-barrier discipline, writer.go:175-188,373-384).
	admit sync.RWMutex //nolint:unused // Task 9: Write/Close lifecycle barrier.

	dropped   atomic.Uint64
	closed    atomic.Bool //nolint:unused // Task 9: set by Close, read by Write.
	closeOnce sync.Once   //nolint:unused // Task 9: guards Close idempotency.
}

var _ zapcore.WriteSyncer = (*Writer)(nil)

// newWriterCore builds a Writer WITHOUT starting the flush goroutine
// (NewWriter starts it; tests drive export directly).
func newWriterCore(endpoint string, o options) (*Writer, error) {
	ep, err := resolveEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	w := &Writer{
		endpoint:    ep,
		client:      o.client,
		headers:     o.headers,
		compression: o.compression,
		timeout:     o.timeout,
		retry:       o.retry,
		maxBytes:    o.maxRequestBytes,
		batchSize:   o.batchSize,
		flushEvery:  o.flushInterval,
		dropPolicy:  o.dropPolicy,
		errFn:       o.errFn,
		env:         newEnvelope(o),
		queue:       make(chan []byte, o.queueSize),
		flushReq:    make(chan chan struct{}),
		done:        make(chan struct{}),
	}
	//nolint:gosec // G118: w.cancel is invoked by Close (Task 9) and by test cleanup.
	w.ctx, w.cancel = context.WithCancel(context.Background())

	return w, nil
}

// DroppedLogs reports records not confirmed delivered (queue overflow,
// retry exhaustion, partial-success rejections, cancelled batches).
func (w *Writer) DroppedLogs() uint64 { return w.dropped.Load() }

func (w *Writer) drop(n int, err *ExportError) {
	w.dropped.Add(uint64(n)) //nolint:gosec
	if err != nil {
		w.errFn(err)
	}
}

// export ships one batch with OTLP retry semantics (§5.3). allowRetry=false
// is the Close-drain single-attempt mode (§5.4). Counts the whole batch as
// dropped on failure.
func (w *Writer) export(records [][]byte, allowRetry bool) {
	if len(records) == 0 {
		return
	}
	raw := w.env.assemble(getFrameBuf(), records)
	defer putFrameBuf(raw)
	body := raw
	compressed := false
	if w.compression == Gzip {
		var gz bytes.Buffer
		zw := gzip.NewWriter(&gz)
		if _, err := zw.Write(raw); err == nil && zw.Close() == nil {
			body, compressed = gz.Bytes(), true
		} else {
			// gzip failure: ship uncompressed rather than lose the batch.
			w.errFn(&ExportError{Message: "gzip failed; sent uncompressed"})
		}
	}

	deadline := time.Now().Add(w.retry.MaxElapsed)
	delay := w.retry.Initial
	for {
		expErr := w.attempt(body, compressed)
		if expErr == nil {
			return
		}
		if !allowRetry || !expErr.Retryable || w.ctx.Err() != nil {
			w.drop(len(records), expErr)

			return
		}
		d := delay/2 + rand.N(delay/2+1) //nolint:gosec // jitter in [delay/2, delay]; backoff jitter needs no crypto RNG
		if expErr.retryAfter > 0 {
			d = expErr.retryAfter
		}
		if time.Now().Add(d).After(deadline) {
			// Includes Retry-After beyond the remaining budget (§5.3).
			w.drop(len(records), expErr)

			return
		}
		select {
		case <-time.After(d):
		case <-w.ctx.Done():
			w.drop(len(records), expErr) // cancelled mid-retry: counted once (§5.4)

			return
		}
		if delay *= 2; delay > w.retry.MaxInterval {
			delay = w.retry.MaxInterval
		}
	}
}

// attempt performs one POST. Its context is Background+timeout — NOT the
// lifecycle ctx — so Close never cancels a request the server may have
// already accepted (§5.4); Close interrupts the backoff sleeps instead.
func (w *Writer) attempt(body []byte, compressed bool) *ExportError {
	ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.endpoint, bytes.NewReader(body))
	if err != nil {
		return &ExportError{Err: err}
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	if compressed {
		req.Header.Set("Content-Encoding", "gzip")
	}
	for k, v := range w.headers {
		req.Header.Set(k, v)
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return &ExportError{Err: err} // transport errors are non-retryable (§5.3)
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	switch {
	case resp.StatusCode == http.StatusOK:
		rejected, msg, derr := decodePartialSuccess(respBody)
		switch {
		case derr != nil:
			// Accepted; the malformed response body is observability-only.
			w.errFn(&ExportError{StatusCode: 200, Err: derr})
		case rejected > 0:
			w.drop(int(rejected), &ExportError{StatusCode: 200, Rejected: rejected, Message: msg}) //nolint:gosec
		case msg != "":
			w.errFn(&ExportError{StatusCode: 200, Warning: true, Message: msg})
		}

		return nil // never retried (§5.3)
	case retryableStatus(resp.StatusCode):
		return &ExportError{
			StatusCode: resp.StatusCode,
			Retryable:  true,
			Message:    excerpt(respBody),
			retryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
		}
	default: // 400, 413, anything else: non-retryable
		return &ExportError{StatusCode: resp.StatusCode, Message: excerpt(respBody)}
	}
}

// retryableStatus implements the OTLP/HTTP retryable class (§5.3).
func retryableStatus(code int) bool {
	switch code {
	case http.StatusTooManyRequests, http.StatusBadGateway,
		http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}

	return false
}

// parseRetryAfter accepts delta-seconds or an HTTP-date (spec clarification).
func parseRetryAfter(h string) time.Duration {
	if h == "" {
		return 0
	}
	if secs, err := strconv.Atoi(h); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	if at, err := http.ParseTime(h); err == nil {
		if d := time.Until(at); d > 0 {
			return d
		}
	}

	return 0
}

func excerpt(b []byte) string {
	const max = 256 //nolint:predeclared // local response-excerpt cap; shadowing the builtin is harmless here
	if len(b) > max {
		b = b[:max]
	}

	return string(b)
}

// NewWriter builds the OTLP/HTTP exporter and starts its flush goroutine.
// The caller owns the Writer and must Close it. Only writer/envelope-end
// options take effect (design §6).
//
// Returns:
//   - *Writer: the exporter; satisfies zapcore.WriteSyncer
//   - error: ErrNoEndpoint or an endpoint validation error
func NewWriter(endpoint string, opts ...Option) (*Writer, error) {
	w, err := newWriterCore(endpoint, applyOptions(opts))
	if err != nil {
		return nil, err
	}
	go w.run()

	return w, nil
}

// Write enqueues one encoded LogRecord. It never blocks beyond the
// non-blocking enqueue: full queue → drop per policy; post-Close → silently
// discarded, uncounted (§5.1). Always returns (len(p), nil) to honor zap's
// multi-writer contract. The shared admit lock spans check+enqueue so a
// concurrent Close cannot strand a record after the final drain — and the
// user error handler is invoked AFTER releasing it, so a handler that calls
// Close cannot self-deadlock (plan-review pass-2 P0).
func (w *Writer) Write(p []byte) (int, error) {
	var notify *ExportError
	w.admit.RLock()
	switch {
	case w.closed.Load():
		// fallthrough to unlock; silently discarded, uncounted
	case w.env.sizeFor(w.env.recordCost(len(p))) > w.maxBytes:
		w.dropped.Add(1)
		notify = &ExportError{Message: "record exceeds WithMaxRequestBytes"}
	default:
		rec := make([]byte, len(p))
		copy(rec, p) // zap frees the encoder buffer after Write returns (§5.1)
		select {
		case w.queue <- rec:
		default:
			if w.dropPolicy == DropOldest {
				select {
				case <-w.queue:
					w.dropped.Add(1)
				default:
				}
				select {
				case w.queue <- rec:
				default:
					w.dropped.Add(1)
				}
			} else {
				w.dropped.Add(1)
			}
		}
	}
	w.admit.RUnlock()
	if notify != nil {
		w.errFn(notify) // outside admit: handler may call Close/Sync safely
	}

	return len(p), nil
}

// run is the single flush goroutine (§5.2): one select over lifecycle, Sync
// requests, the ticker, and the queue. Exports run inline — a retrying batch
// head-of-line blocks later batches; the bounded queue absorbs and overflows
// to counted drops.
func (w *Writer) run() {
	defer close(w.done)
	ticker := time.NewTicker(w.flushEvery)
	defer ticker.Stop()

	var batch [][]byte
	var tagged int

	flush := func(allowRetry bool) {
		if len(batch) > 0 {
			w.export(batch, allowRetry)
			batch, tagged = batch[:0], 0
		}
	}
	add := func(rec []byte, allowRetry bool) {
		cost := w.env.recordCost(len(rec))
		// Byte-aware cut BEFORE adding (§5.2): never assemble past maxBytes.
		if len(batch) > 0 && w.env.sizeFor(tagged+cost) > w.maxBytes {
			flush(allowRetry)
		}
		batch = append(batch, rec)
		tagged += cost
		if len(batch) >= w.batchSize || w.env.sizeFor(tagged) >= w.maxBytes {
			flush(allowRetry)
		}
	}
	drainQueued := func(allowRetry bool) {
		for {
			select {
			case rec := <-w.queue:
				add(rec, allowRetry)
			default:
				flush(allowRetry)

				return
			}
		}
	}

	for {
		select {
		case <-w.ctx.Done():
			drainQueued(false) // final drain: one attempt per batch (§5.4)

			return
		case ack := <-w.flushReq:
			drainQueued(true) // Sync barrier: everything enqueued before the call
			close(ack)
		case <-ticker.C:
			flush(true)
		case rec := <-w.queue:
			add(rec, true)
		}
	}
}

// Sync flushes every record enqueued before the call and waits for
// resolution (exported or dropped). Worst case is pending_batches ×
// (retry budget + attempt timeout) — a barrier, not a hard deadline (§5.4).
// Post-Close, Sync is a nil no-op.
func (w *Writer) Sync() error {
	if w.closed.Load() {
		return nil
	}
	ack := make(chan struct{})
	select {
	case w.flushReq <- ack:
	case <-w.ctx.Done():
		return nil
	}
	select {
	case <-ack:
	case <-w.done:
	}

	return nil
}

// Close stops intake, aborts any backoff sleep (the in-flight HTTP attempt
// finishes within its own timeout), drains the queue with one attempt per
// batch, and releases resources. Idempotent; always nil (§5.4).
func (w *Writer) Close() error {
	w.closeOnce.Do(func() {
		w.closed.Store(true)
		// Wait out in-flight Writes (they hold admit shared); once acquired,
		// every future Write observes closed and discards — so the cancel +
		// final drain below cannot be raced by a late enqueue.
		w.admit.Lock()
		w.admit.Unlock() //nolint:staticcheck // barrier, not a critical section
		w.cancel()
		<-w.done
	})

	return nil
}
