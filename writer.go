package zapwire

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Writer ships logs to a processor over a reconnecting byte stream. It implements
// io.Writer and zapcore.WriteSyncer (see core.go). A stalled consumer causes counted drops
// rather than an unbounded block: sync writes wait only up to the bounded write timeout, and
// async enqueue never blocks.
type Writer struct {
	transport Transport
	enc       Encoder
	framer    Framer
	cfg       config

	mu   sync.RWMutex
	conn net.Conn

	// writeMu serializes SetWriteDeadline+Write so concurrent writers cannot extend one
	// another's in-flight deadline.
	writeMu sync.Mutex

	// lifecycleMu gates async enqueue against Close. Async writers hold it (RLock) across the
	// closed-check and the queue send; Close takes it (Lock) as a barrier before close(done),
	// so no payload can be enqueued after the final flush. It nests with nothing.
	lifecycleMu sync.RWMutex

	closed atomic.Bool

	reconnectCh chan struct{}
	done        chan struct{}
	ctx         context.Context
	cancel      context.CancelFunc

	droppedLogs    atomic.Uint64
	reconnectCount atomic.Uint64

	// async-only (nil in sync mode)
	queue     chan []byte
	flushReq  chan chan struct{}
	flushDone chan struct{}

	scratchPool sync.Pool // *scratch
}

type scratch struct {
	enc   []byte
	frame []byte
}

var _ io.Writer = (*Writer)(nil)

// New creates a Writer. It attempts an immediate connection so logs flow at once when the
// endpoint is already up; otherwise it starts disconnected and reconnects in the
// background. An error is returned only for nil transport/encoder/framer.
func New(t Transport, enc Encoder, framer Framer, opts ...Option) (*Writer, error) {
	switch {
	case t == nil:
		return nil, ErrNoTransport
	case enc == nil:
		return nil, ErrNoEncoder
	case framer == nil:
		return nil, ErrNoFramer
	}

	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}
	cfg = normalizeConfig(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	w := &Writer{
		transport:   t,
		enc:         enc,
		framer:      framer,
		cfg:         cfg,
		reconnectCh: make(chan struct{}, 1),
		done:        make(chan struct{}),
		ctx:         ctx,
		cancel:      cancel,
	}
	w.scratchPool.New = func() any { return &scratch{} }

	conn, err := w.dial()
	if err != nil {
		w.reportErr(fmt.Errorf("zapwire: initial connect to %s://%s failed: %w",
			t.Network(), t.Address(), err))
	} else {
		w.conn = conn
	}

	go w.reconnectLoop()
	if err != nil {
		w.triggerReconnect()
	}

	if cfg.mode == ModeAsync {
		w.queue = make(chan []byte, cfg.bufferSize)
		w.flushReq = make(chan chan struct{})
		w.flushDone = make(chan struct{})
		go w.flushLoop()
	}

	return w, nil
}

// Write encodes p, frames it, and ships it. Dropped logs return (len(p), nil); only encode
// failures return a non-nil error (to satisfy zap's multi-writer contract).
func (w *Writer) Write(p []byte) (int, error) {
	if w.closed.Load() {
		return len(p), nil
	}
	if w.cfg.mode == ModeAsync {
		return w.writeAsync(p)
	}

	return w.writeSync(p)
}

func (w *Writer) writeSync(p []byte) (int, error) {
	s, _ := w.scratchPool.Get().(*scratch)
	defer w.scratchPool.Put(s)

	payload, err := w.enc.Encode(s.enc[:0], p)
	if err != nil {
		return 0, fmt.Errorf("zapwire: encode: %w", err)
	}
	s.enc = payload

	frame, err := w.framer.Frame(s.frame[:0], [][]byte{payload})
	if err != nil {
		return 0, fmt.Errorf("zapwire: frame: %w", err)
	}
	s.frame = frame

	w.writeFrame(frame, 1)

	return len(p), nil
}

// Close stops the background goroutines, flushes any async buffer, and closes the
// connection. It is safe to call multiple times.
func (w *Writer) Close() error {
	if !w.closed.CompareAndSwap(false, true) {
		return nil
	}
	// Barrier: wait for any in-flight async writer to finish its enqueue. closed is now true,
	// so writers arriving after this drop instead of sending; writers already holding the
	// RLock complete their send before we close(done), and flushLoop's done-case drains them.
	w.lifecycleMu.Lock()
	w.lifecycleMu.Unlock() //nolint:staticcheck // intentional lock/unlock barrier, not a guarded section
	w.cancel()
	close(w.done)
	if w.cfg.mode == ModeAsync {
		<-w.flushDone // final flush completes before we tear the connection down
	}

	w.mu.Lock()
	conn := w.conn
	w.conn = nil
	w.mu.Unlock()
	if conn != nil {
		return conn.Close() //nolint:wrapcheck // direct conn.Close passthrough
	}

	return nil
}

// IsConnected reports whether a live connection is currently held.
func (w *Writer) IsConnected() bool { return w.hasConn() && !w.closed.Load() }

// DroppedLogs returns the count of logs dropped due to connection/buffer pressure.
func (w *Writer) DroppedLogs() uint64 { return w.droppedLogs.Load() }

// ReconnectCount returns the number of successful background (re)connections.
func (w *Writer) ReconnectCount() uint64 { return w.reconnectCount.Load() }

// writeFrame performs the bounded write; n is the number of logs the frame carries (for
// accurate drop accounting).
func (w *Writer) writeFrame(frame []byte, n int) {
	conn := w.getConn()
	if conn == nil {
		w.droppedLogs.Add(uint64(n)) //nolint:gosec // n is a positive log count
		w.triggerReconnect()

		return
	}

	w.writeMu.Lock()
	if err := conn.SetWriteDeadline(time.Now().Add(w.cfg.writeTimeout)); err != nil {
		w.writeMu.Unlock()
		w.droppedLogs.Add(uint64(n)) //nolint:gosec // n is a positive log count
		w.handleWriteError(conn, err)

		return
	}
	_, err := conn.Write(frame)
	w.writeMu.Unlock()
	if err != nil {
		w.droppedLogs.Add(uint64(n)) //nolint:gosec // n is a positive log count
		w.handleWriteError(conn, err)
	}
}

func (w *Writer) dial() (net.Conn, error) { return w.transport.Dial(w.ctx) }

func (w *Writer) getConn() net.Conn {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.conn
}

func (w *Writer) hasConn() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.conn != nil
}

func (w *Writer) triggerReconnect() {
	select {
	case w.reconnectCh <- struct{}{}:
	default:
	}
}

func (w *Writer) drainReconnectCh() {
	for {
		select {
		case <-w.reconnectCh:
		default:
			return
		}
	}
}

func (w *Writer) reconnectLoop() {
	for {
		select {
		case <-w.done:
			return
		case <-w.reconnectCh:
			w.drainReconnectCh()
		}
		if w.hasConn() {
			continue
		}

		backoff := w.cfg.reconnectInterval
		for attempt := 1; attempt <= w.cfg.maxRetries; attempt++ {
			select {
			case <-w.done:
				return
			default:
			}
			if w.tryConnect() {
				w.reconnectCount.Add(1)

				break
			}
			if attempt < w.cfg.maxRetries {
				select {
				case <-w.done:
					return
				case <-time.After(backoff):
				}
				backoff = min(backoff*2, w.cfg.maxReconnect)
			}
		}
	}
}

func (w *Writer) tryConnect() bool {
	conn, err := w.dial()
	if err != nil {
		return false
	}

	w.mu.Lock()
	if w.closed.Load() {
		w.mu.Unlock()
		_ = conn.Close()

		return false
	}
	old := w.conn
	w.conn = conn
	w.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}

	return true
}

// handleWriteError discards the failed connection and triggers a reconnect, unless a
// concurrent reconnect has already installed a healthy replacement.
func (w *Writer) handleWriteError(failed net.Conn, err error) {
	w.mu.Lock()
	if w.conn != failed {
		w.mu.Unlock()

		return
	}
	w.conn = nil
	w.mu.Unlock()

	_ = failed.Close()
	w.reportErr(fmt.Errorf("zapwire: connection error: %w", err))
	w.triggerReconnect()
}

func (w *Writer) reportErr(err error) {
	if w.cfg.onError != nil {
		w.cfg.onError(err)

		return
	}
	_, _ = fmt.Fprintln(os.Stderr, err.Error())
}

func (w *Writer) writeAsync(p []byte) (int, error) {
	// Async payloads are owned by the queue until flushed, so they cannot share a pooled
	// buffer. (Queue-slot pooling is a documented future optimization.)
	payload, err := w.enc.Encode(nil, p)
	if err != nil {
		return 0, fmt.Errorf("zapwire: encode: %w", err)
	}

	// Hold the lifecycle gate across the closed-check and the enqueue so a payload can never
	// reach the queue after Close's barrier. Without this, a writer preempted past the
	// Write-level closed-check could send to the still-open buffered queue after flushLoop has
	// exited, stranding the log with no drop accounting.
	w.lifecycleMu.RLock()
	defer w.lifecycleMu.RUnlock()
	if w.closed.Load() {
		w.droppedLogs.Add(1)

		return len(p), nil
	}
	w.enqueue(payload)

	return len(p), nil
}

func (w *Writer) enqueue(payload []byte) {
	select {
	case w.queue <- payload:
		return
	default:
	}
	// Buffer full.
	if w.cfg.dropPolicy == DropOldest {
		select {
		case <-w.queue:
			w.droppedLogs.Add(1)
		default:
		}
		select {
		case w.queue <- payload:
			return
		default:
		}
	}
	w.droppedLogs.Add(1)
}

// Sync flushes the async buffer. It is a no-op in sync mode. Honors zap's flush contract.
func (w *Writer) Sync() error {
	if w.cfg.mode != ModeAsync || w.closed.Load() {
		return nil
	}
	ack := make(chan struct{})
	select {
	case w.flushReq <- ack:
		<-ack
	case <-w.done:
	}

	return nil
}

func (w *Writer) flushLoop() {
	defer close(w.flushDone)
	ticker := time.NewTicker(w.cfg.flushInterval)
	defer ticker.Stop()

	batch := make([][]byte, 0, w.cfg.batchSize)
	frameBuf := make([]byte, 0, 4096)

	for {
		select {
		case <-w.done:
			w.flushAll(batch, frameBuf)

			return
		case ack := <-w.flushReq:
			frameBuf = w.flushAll(batch, frameBuf)
			batch = batch[:0]
			close(ack)
		case <-ticker.C:
			batch = w.drain(batch)
			frameBuf = w.flush(batch, frameBuf)
			batch = batch[:0]
		case payload := <-w.queue:
			batch = append(batch, payload)
			batch = w.drain(batch)
			if len(batch) >= w.cfg.batchSize {
				frameBuf = w.flush(batch, frameBuf)
				batch = batch[:0]
			}
		}
	}
}

// drain tops up batch with currently-queued payloads, up to batchSize.
func (w *Writer) drain(batch [][]byte) [][]byte {
	for len(batch) < w.cfg.batchSize {
		select {
		case p := <-w.queue:
			batch = append(batch, p)
		default:
			return batch
		}
	}

	return batch
}

// flushAll drains and flushes the entire queue (plus any partial batch already accumulated),
// in batchSize-bounded frames, until the queue is empty. Used by Sync() and shutdown so the
// whole buffer is delivered, not just one batch. Returns the (possibly grown) frame buffer.
func (w *Writer) flushAll(batch [][]byte, frameBuf []byte) []byte {
	for {
		batch = w.drain(batch)
		if len(batch) == 0 {
			break
		}
		frameBuf = w.flush(batch, frameBuf)
		batch = batch[:0]
	}

	return frameBuf
}

// flush frames and writes the batch, returning the (possibly grown) frame buffer for reuse.
func (w *Writer) flush(batch [][]byte, frameBuf []byte) []byte {
	if len(batch) == 0 {
		return frameBuf
	}
	frame, err := w.framer.Frame(frameBuf[:0], batch)
	if err != nil {
		w.droppedLogs.Add(uint64(len(batch))) //nolint:gosec // batch length is non-negative
		w.reportErr(fmt.Errorf("zapwire: frame batch: %w", err))

		return frameBuf
	}
	w.writeFrame(frame, len(batch))

	return frame
}
