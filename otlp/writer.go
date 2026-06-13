package otlp

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap/zapcore"
)

// Writer is the OTLP exporter (OTLP/HTTP via NewWriter/NewHTTPWriter,
// OTLP/gRPC via NewGRPCWriter): a zapcore.WriteSyncer with a bounded queue,
// a single flush goroutine, and OTLP retry semantics (design §5).
// Async-only — Sync() is the flush barrier.
type Writer struct {
	tr         transport
	retry      RetryConfig
	maxBytes   int
	batchSize  int
	flushEvery time.Duration
	dropPolicy DropPolicy
	errFn      func(error)
	env        *envelope

	queue    chan []byte
	flushReq chan chan struct{}
	done     chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc

	// admit closes the closed-check→enqueue window: Write holds it shared
	// across check+send; Close takes it exclusively after setting closed, so
	// no enqueue can land after the final drain starts (the root writer's
	// lifecycle-barrier discipline; see Write and Close).
	admit sync.RWMutex

	dropped   atomic.Uint64
	closed    atomic.Bool
	closeOnce sync.Once
}

var _ zapcore.WriteSyncer = (*Writer)(nil)

// newWriterCore builds a Writer around a transport WITHOUT starting the
// flush goroutine (the constructors start it; tests drive export directly).
func newWriterCore(tr transport, o options) *Writer {
	w := &Writer{
		tr:         tr,
		retry:      o.retry,
		maxBytes:   o.maxRequestBytes,
		batchSize:  o.batchSize,
		flushEvery: o.flushInterval,
		dropPolicy: o.dropPolicy,
		errFn:      o.errFn,
		env:        newEnvelope(o),
		queue:      make(chan []byte, o.queueSize),
		flushReq:   make(chan chan struct{}),
		done:       make(chan struct{}),
	}
	//nolint:gosec // G118: w.cancel is invoked by Close and by test cleanup.
	w.ctx, w.cancel = context.WithCancel(context.Background())

	return w
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
	p := w.tr.prepare(raw)
	if p.fail != nil {
		// Fatal prepare outcome (JSON transcode failure): there is no valid
		// body to ship — count the whole batch and never call attempt. Same
		// on the flush and Close-drain paths (both funnel through here).
		w.drop(len(records), p.fail)

		return
	}
	if p.warn != nil {
		w.errFn(p.warn)
	}

	deadline := time.Now().Add(w.retry.MaxElapsed)
	delay := w.retry.Initial
	for {
		accept, expErr := w.tr.attempt(p)
		if expErr == nil {
			switch {
			case accept == nil: // clean accept
			case accept.rejected > 0:
				w.drop(int(accept.rejected), accept.event)
			case accept.event != nil:
				w.errFn(accept.event)
			}

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

// NewWriter builds the OTLP/HTTP exporter and starts its flush goroutine.
// Equivalent to NewHTTPWriter. The caller owns the Writer and must Close it.
// Only writer/envelope-end options take effect (design §6).
//
// Returns:
//   - *Writer: the exporter; satisfies zapcore.WriteSyncer
//   - error: ErrNoEndpoint or an endpoint validation error
func NewWriter(endpoint string, opts ...Option) (*Writer, error) {
	o := applyOptions(opts)
	tr, err := newHTTPTransport(endpoint, o)
	if err != nil {
		return nil, err
	}
	w := newWriterCore(tr, o)
	go w.run()

	return w, nil
}

// NewHTTPWriter is the explicit symmetric counterpart of NewGRPCWriter; it
// is exactly NewWriter (OTLP/HTTP, the spec's default protocol).
//
// Returns:
//   - *Writer: the exporter; satisfies zapcore.WriteSyncer
//   - error: ErrNoEndpoint or an endpoint validation error
func NewHTTPWriter(endpoint string, opts ...Option) (*Writer, error) {
	return NewWriter(endpoint, opts...)
}

// NewGRPCWriter builds the OTLP/gRPC exporter and starts its flush
// goroutine. Endpoint forms (design 2026-06-12 §5):
//
//	"host:4317"          TLS (spec default); WithInsecure() selects h2c
//	"http://host:4317"   plaintext h2c (scheme wins)
//	"https://host:4317"  TLS (scheme wins); WithTLSConfig for custom CA/mTLS
//
// URL paths are rejected — gRPC always posts to the fixed method path. The
// transport owns its HTTP/2-only client (WithHTTPClient is a no-op here)
// and does not traverse proxies. The caller owns the Writer and must Close
// it.
//
// Returns:
//   - *Writer: the exporter; satisfies zapcore.WriteSyncer
//   - error: ErrNoEndpoint, endpoint validation, option conflicts, or
//     reserved WithHeaders keys
func NewGRPCWriter(endpoint string, opts ...Option) (*Writer, error) {
	o := applyOptions(opts)
	tr, err := newGRPCTransport(endpoint, o)
	if err != nil {
		return nil, err
	}
	w := newWriterCore(tr, o)
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
		w.tr.close()
	})

	return nil
}
