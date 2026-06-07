# Phase 01 — Core foundation (package `zapwire`)

Builds the processor-agnostic core: `Transport` (UDS/TCP), the `Encoder`/`Framer`
interfaces, `Option`s, and the `Writer` — connection manager with background reconnect
(ported from `tmp/fluent/writer.go`), bounded drop-on-stall writes, and configurable
sync/async dispatch. Stdlib + `zapcore` only.

> **Porting note:** `tmp/fluent/writer.go` and `tmp/fluent/writer_test.go` are in the
> working tree (gitignored). They are the proven source for the connection manager and the
> reconnect tests — read them, copy the relevant logic in, and generalize `socketPath` →
> `Transport`. Do not `import` from `tmp/`.

---

### Task 1.1: Errors and Transport

**Files:**
- Create: `errors.go`, `transport.go`
- Test: `transport_test.go`

- [ ] **Step 1: Write the failing test**

`transport_test.go`:
```go
package zapwire

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUDS_DialsListeningSocket(t *testing.T) {
	path := filepath.Join(os.TempDir(), "zapwire_uds_test.sock")
	_ = os.Remove(path)
	ln, err := net.Listen("unix", path)
	require.NoError(t, err)
	defer func() { _ = ln.Close(); _ = os.Remove(path) }()

	tr := UDS(path)
	require.Equal(t, "unix", tr.Network())
	require.Equal(t, path, tr.Address())

	conn, err := tr.Dial(context.Background())
	require.NoError(t, err)
	require.NoError(t, conn.Close())
}

func TestTCP_DialsListeningPort(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()

	tr := TCP(ln.Addr().String())
	require.Equal(t, "tcp", tr.Network())

	conn, err := tr.Dial(context.Background())
	require.NoError(t, err)
	require.NoError(t, conn.Close())
}

func TestUDS_DialMissingSocketFailsFast(t *testing.T) {
	tr := UDS(filepath.Join(os.TempDir(), "zapwire_absent.sock"))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := tr.Dial(ctx)
	require.Error(t, err)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./... -run TestUDS -run TestTCP`
Expected: FAIL — `undefined: UDS` / `undefined: TCP`.

- [ ] **Step 3: Write `errors.go`**

```go
package zapwire

import "errors"

var (
	// ErrNoTransport is returned by New when transport is nil.
	ErrNoTransport = errors.New("zapwire: transport is required")
	// ErrNoEncoder is returned by New when encoder is nil.
	ErrNoEncoder = errors.New("zapwire: encoder is required")
	// ErrNoFramer is returned by New when framer is nil.
	ErrNoFramer = errors.New("zapwire: framer is required")
)
```

- [ ] **Step 4: Write `transport.go`**

```go
package zapwire

import (
	"context"
	"net"
	"time"
)

const defaultDialTimeout = 3 * time.Second

// Transport is a reconnectable byte-stream endpoint (UDS or TCP).
type Transport interface {
	// Dial establishes a new connection, honoring ctx for cancellation/timeout.
	Dial(ctx context.Context) (net.Conn, error)
	// Network reports the net package network ("unix" or "tcp").
	Network() string
	// Address reports the dial address (socket path or host:port).
	Address() string
}

type netTransport struct {
	network string
	address string
	timeout time.Duration
}

// UDS returns a Transport that connects to a Unix domain socket at path.
func UDS(path string) Transport {
	return &netTransport{network: "unix", address: path, timeout: defaultDialTimeout}
}

// TCP returns a Transport that connects to a TCP host:port address.
func TCP(addr string) Transport {
	return &netTransport{network: "tcp", address: addr, timeout: defaultDialTimeout}
}

func (t *netTransport) Dial(ctx context.Context) (net.Conn, error) {
	d := net.Dialer{Timeout: t.timeout}

	return d.DialContext(ctx, t.network, t.address) //nolint:wrapcheck // surfaced verbatim by caller
}

func (t *netTransport) Network() string { return t.network }
func (t *netTransport) Address() string { return t.address }
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./... -run 'TestUDS|TestTCP' -race`
Expected: PASS. (`transport.go` is self-contained; `defaultDialTimeout` lives here.)

- [ ] **Step 6: Commit**

```bash
git add errors.go transport.go transport_test.go
git commit -m "feat: add Transport interface with UDS and TCP implementations"
```

---

### Task 1.2: Encoder/Framer interfaces and Options

**Files:**
- Create: `encoder.go`, `options.go`
- Test: `options_test.go`

- [ ] **Step 1: Write the failing test**

`options_test.go`:
```go
package zapwire

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	c := defaultConfig()
	require.Equal(t, ModeSync, c.mode)
	require.Equal(t, defaultWriteTimeout, c.writeTimeout)
	require.Equal(t, defaultBufferSize, c.bufferSize)
	require.Equal(t, DropNewest, c.dropPolicy)
}

func TestOptionsApply(t *testing.T) {
	c := defaultConfig()
	for _, o := range []Option{
		WithAsyncMode(),
		WithWriteTimeout(50 * time.Millisecond),
		WithBufferSize(2048),
		WithBatchSize(64),
		WithFlushInterval(10 * time.Millisecond),
		WithDropPolicy(DropOldest),
		WithMaxRetries(5),
		WithReconnect(time.Second, 10*time.Second),
	} {
		o(&c)
	}
	require.Equal(t, ModeAsync, c.mode)
	require.Equal(t, 50*time.Millisecond, c.writeTimeout)
	require.Equal(t, 2048, c.bufferSize)
	require.Equal(t, 64, c.batchSize)
	require.Equal(t, DropOldest, c.dropPolicy)
	require.Equal(t, 5, c.maxRetries)
	require.Equal(t, time.Second, c.reconnectInterval)
	require.Equal(t, 10*time.Second, c.maxReconnect)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./... -run TestOptions -run TestDefaultConfig`
Expected: FAIL — `undefined: defaultConfig`.

- [ ] **Step 3: Write `encoder.go`**

```go
package zapwire

// Encoder converts the bytes zap hands the WriteSyncer (a JSON log line, in the v1
// transcode path) into a single per-entry wire payload. It appends to dst and returns the
// extended slice so callers can pool the backing buffer. It must NOT add framing.
type Encoder interface {
	Encode(dst, record []byte) ([]byte, error)
}

// Framer wraps one or more per-entry payloads into a single wire frame written to the
// socket. len(payloads) == 1 in sync mode and N in async/batched mode. It appends to dst
// and returns the extended slice.
type Framer interface {
	Frame(dst []byte, payloads [][]byte) ([]byte, error)
}
```

- [ ] **Step 4: Write `options.go`**

```go
package zapwire

import "time"

// Mode selects the delivery model.
type Mode int

const (
	// ModeSync writes each log inline with a bounded deadline (never blocks the app).
	ModeSync Mode = iota
	// ModeAsync enqueues logs and flushes batched frames from a background goroutine.
	ModeAsync
)

// DropPolicy selects what to discard when the async buffer is full.
type DropPolicy int

const (
	// DropNewest discards the incoming log when the buffer is full.
	DropNewest DropPolicy = iota
	// DropOldest evicts the oldest queued log to make room for the incoming one.
	DropOldest
)

const (
	defaultWriteTimeout      = 100 * time.Millisecond
	defaultReconnectInterval = 100 * time.Millisecond
	defaultMaxReconnect      = 3 * time.Second
	defaultMaxRetries        = 30
	defaultBufferSize        = 4096
	defaultFlushInterval     = 200 * time.Millisecond
	defaultBatchSize         = 128
)

type config struct {
	mode              Mode
	writeTimeout      time.Duration
	reconnectInterval time.Duration
	maxReconnect      time.Duration
	maxRetries        int
	bufferSize        int
	flushInterval     time.Duration
	batchSize         int
	dropPolicy        DropPolicy
	onError           func(error)
}

func defaultConfig() config {
	return config{
		mode:              ModeSync,
		writeTimeout:      defaultWriteTimeout,
		reconnectInterval: defaultReconnectInterval,
		maxReconnect:      defaultMaxReconnect,
		maxRetries:        defaultMaxRetries,
		bufferSize:        defaultBufferSize,
		flushInterval:     defaultFlushInterval,
		batchSize:         defaultBatchSize,
		dropPolicy:        DropNewest,
	}
}

// Option configures a Writer.
type Option func(*config)

// WithSyncMode selects synchronous, write-per-log delivery (the default).
func WithSyncMode() Option { return func(c *config) { c.mode = ModeSync } }

// WithAsyncMode selects buffered, batched background delivery.
func WithAsyncMode() Option { return func(c *config) { c.mode = ModeAsync } }

// WithWriteTimeout bounds each socket write.
func WithWriteTimeout(d time.Duration) Option { return func(c *config) { c.writeTimeout = d } }

// WithBufferSize sets the async queue capacity (number of buffered logs).
func WithBufferSize(n int) Option { return func(c *config) { c.bufferSize = n } }

// WithBatchSize caps how many logs a single async flush frames together.
func WithBatchSize(n int) Option { return func(c *config) { c.batchSize = n } }

// WithFlushInterval sets the async max time a log waits before being flushed.
func WithFlushInterval(d time.Duration) Option { return func(c *config) { c.flushInterval = d } }

// WithDropPolicy selects the full-buffer drop behavior (async).
func WithDropPolicy(p DropPolicy) Option { return func(c *config) { c.dropPolicy = p } }

// WithMaxRetries bounds reconnect attempts per burst.
func WithMaxRetries(n int) Option { return func(c *config) { c.maxRetries = n } }

// WithReconnect sets the initial and max reconnect backoff intervals.
func WithReconnect(initial, maxInterval time.Duration) Option {
	return func(c *config) { c.reconnectInterval = initial; c.maxReconnect = maxInterval }
}

// WithErrorHandler installs a callback for transport/encode errors. Defaults to stderr.
func WithErrorHandler(fn func(error)) Option { return func(c *config) { c.onError = fn } }
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./... -run 'TestOptions|TestDefaultConfig' -race`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add encoder.go options.go options_test.go
git commit -m "feat: add Encoder/Framer interfaces and writer options"
```

---

### Task 1.3: Writer — connection manager, reconnect, sync dispatch

**Files:**
- Create: `writer.go`
- Test: `testsupport_test.go`, `writer_test.go`

- [ ] **Step 1: Write test support (mock servers + trivial codecs)**

`testsupport_test.go`:
```go
package zapwire

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// rawEncoder passes the record through unchanged.
type rawEncoder struct{}

func (rawEncoder) Encode(dst, record []byte) ([]byte, error) { return append(dst, record...), nil }

// errEncoder always fails (to exercise the encode-error path).
type errEncoder struct{}

func (errEncoder) Encode([]byte, []byte) ([]byte, error) { return nil, fmt.Errorf("boom") }

// lineFramer joins payloads each terminated by '\n' (newline framing).
type lineFramer struct{}

func (lineFramer) Frame(dst []byte, payloads [][]byte) ([]byte, error) {
	for _, p := range payloads {
		dst = append(dst, p...)
		dst = append(dst, '\n')
	}

	return dst, nil
}

func randomSocketPath(t *testing.T) string {
	t.Helper()

	return filepath.Join(os.TempDir(), fmt.Sprintf("zapwire_%d.sock", time.Now().UnixNano()))
}

// readServer accepts one UDS connection and streams everything it reads to recv.
type readServer struct {
	ln   net.Listener
	recv chan []byte
	path string
}

func startReadServer(t *testing.T, path string) *readServer {
	t.Helper()
	ln, err := net.Listen("unix", path)
	require.NoError(t, err)
	s := &readServer{ln: ln, recv: make(chan []byte, 64), path: path}
	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 4096)
		for {
			n, rerr := conn.Read(buf)
			if n > 0 {
				b := make([]byte, n)
				copy(b, buf[:n])
				s.recv <- b
			}
			if rerr != nil {
				return
			}
		}
	}()

	return s
}

func (s *readServer) stop() { _ = s.ln.Close(); _ = os.Remove(s.path) }

// rawUDSServer can sever the live connection to drive reconnect tests.
type rawUDSServer struct {
	t         *testing.T
	path      string
	ln        net.Listener
	mu        sync.Mutex
	conns     []net.Conn
	connected chan struct{}
}

func startRawUDSServer(t *testing.T, path string) *rawUDSServer {
	t.Helper()
	ln, err := net.Listen("unix", path)
	require.NoError(t, err)
	s := &rawUDSServer{t: t, path: path, ln: ln, connected: make(chan struct{})}
	go func() {
		first := true
		for {
			conn, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			s.mu.Lock()
			s.conns = append(s.conns, conn)
			s.mu.Unlock()
			if first {
				first = false
				close(s.connected)
			}
			go func(c net.Conn) {
				b := make([]byte, 4096)
				for {
					if _, rerr := c.Read(b); rerr != nil {
						return
					}
				}
			}(conn)
		}
	}()

	return s
}

func (s *rawUDSServer) waitConnected() {
	select {
	case <-s.connected:
	case <-time.After(time.Second):
		s.t.Fatal("rawUDSServer: timed out waiting for first connection")
	}
}

func (s *rawUDSServer) close() {
	_ = s.ln.Close()
	s.mu.Lock()
	for _, c := range s.conns {
		_ = c.Close()
	}
	s.conns = nil
	s.mu.Unlock()
	_ = os.Remove(s.path)
}
```

- [ ] **Step 2: Write the failing sync-writer tests**

`writer_test.go`:
```go
package zapwire

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_RequiresDeps(t *testing.T) {
	_, err := New(nil, rawEncoder{}, lineFramer{})
	require.ErrorIs(t, err, ErrNoTransport)
	_, err = New(UDS("/x"), nil, lineFramer{})
	require.ErrorIs(t, err, ErrNoEncoder)
	_, err = New(UDS("/x"), rawEncoder{}, nil)
	require.ErrorIs(t, err, ErrNoFramer)
}

func TestWriter_Sync_DeliversFrame(t *testing.T) {
	path := randomSocketPath(t)
	srv := startReadServer(t, path)
	defer srv.stop()

	w, err := New(UDS(path), rawEncoder{}, lineFramer{})
	require.NoError(t, err)
	defer w.Close()
	require.True(t, w.IsConnected())

	n, err := w.Write([]byte(`{"msg":"hello"}`))
	require.NoError(t, err)
	require.Equal(t, len(`{"msg":"hello"}`), n)

	select {
	case got := <-srv.recv:
		require.Equal(t, "{\"msg\":\"hello\"}\n", string(got))
	case <-time.After(time.Second):
		t.Fatal("did not receive framed log")
	}
}

func TestWriter_EncodeError_Returned(t *testing.T) {
	path := randomSocketPath(t)
	srv := startReadServer(t, path)
	defer srv.stop()

	w, err := New(UDS(path), errEncoder{}, lineFramer{})
	require.NoError(t, err)
	defer w.Close()

	n, err := w.Write([]byte(`{}`))
	require.Error(t, err)
	require.Equal(t, 0, n)
}

func TestWriter_NoConn_DropsButReportsConsumed(t *testing.T) {
	// No server: connection never establishes.
	w, err := New(UDS(randomSocketPath(t)), rawEncoder{}, lineFramer{}, WithMaxRetries(1))
	require.NoError(t, err)
	defer w.Close()
	require.False(t, w.IsConnected())

	n, err := w.Write([]byte(`{"a":1}`))
	require.NoError(t, err)
	require.Equal(t, len(`{"a":1}`), n)
	require.Positive(t, w.DroppedLogs())
}

func TestWriter_ClosedIsIdempotent(t *testing.T) {
	w, err := New(UDS(randomSocketPath(t)), rawEncoder{}, lineFramer{}, WithMaxRetries(1))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.NoError(t, w.Close())
	n, err := w.Write([]byte(`x`))
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

func TestWriter_ReconnectsAfterDrop(t *testing.T) {
	path := randomSocketPath(t)
	defer func() { _ = os.Remove(path) }()

	srv := startRawUDSServer(t, path)
	w, err := New(UDS(path), rawEncoder{}, lineFramer{})
	require.NoError(t, err)
	defer w.Close()
	require.True(t, w.IsConnected())
	srv.waitConnected()

	_, _ = w.Write([]byte(`{"a":1}`))
	srv.close()

	require.Eventually(t, func() bool {
		_, werr := w.Write([]byte(`{"a":1}`))
		require.NoError(t, werr)

		return !w.IsConnected()
	}, 3*time.Second, 20*time.Millisecond)

	srv2 := startRawUDSServer(t, path)
	defer srv2.close()

	require.Eventually(t, func() bool {
		_, werr := w.Write([]byte(`{"a":1}`))
		require.NoError(t, werr)

		return w.IsConnected()
	}, 5*time.Second, 50*time.Millisecond)
	require.GreaterOrEqual(t, w.ReconnectCount(), uint64(1))
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./... -run TestWriter -run TestNew`
Expected: FAIL — `undefined: New`.

- [ ] **Step 4: Write `writer.go` (connection manager + reconnect + sync dispatch)**

Port the connection-manager methods from `tmp/fluent/writer.go`, generalizing `dial` to use
`Transport` and dropping the UDS-specific `os.Stat` precheck (a missing socket makes `Dial`
fail fast already).

```go
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
// io.Writer and zapcore.WriteSyncer (see core.go). A stalled consumer causes drops
// (counted), never a blocked application goroutine.
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
	if cfg.maxRetries <= 0 {
		cfg.maxRetries = defaultMaxRetries
	}

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
		w.droppedLogs.Add(uint64(n))
		w.triggerReconnect()

		return
	}

	w.writeMu.Lock()
	_ = conn.SetWriteDeadline(time.Now().Add(w.cfg.writeTimeout))
	_, err := conn.Write(frame)
	w.writeMu.Unlock()
	if err != nil {
		w.droppedLogs.Add(uint64(n))
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
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./... -run 'TestNew|TestWriter' -race`
Expected: PASS (all sync writer + reconnect tests green). `writeAsync`/`flushLoop` are added
in Task 1.4 — declare them as stubs now if the compiler complains, OR implement Task 1.4
before running async paths. Minimal stubs:
```go
func (w *Writer) writeAsync(p []byte) (int, error) { return len(p), nil }
func (w *Writer) flushLoop()                        {}
```
(Replace these in Task 1.4.)

- [ ] **Step 6: Commit**

```bash
git add writer.go testsupport_test.go writer_test.go
git commit -m "feat: add Writer with connection manager, reconnect and sync delivery"
```

---

### Task 1.4: Writer — async ring buffer, batching, Sync, drop policy

**Files:**
- Modify: `writer.go` (replace the Task 1.3 stubs)
- Test: `writer_async_test.go`

- [ ] **Step 1: Write the failing async tests**

`writer_async_test.go`:
```go
package zapwire

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWriter_Async_BatchesAndFlushesOnSync(t *testing.T) {
	path := randomSocketPath(t)
	srv := startReadServer(t, path)
	defer srv.stop()

	w, err := New(UDS(path), rawEncoder{}, lineFramer{},
		WithAsyncMode(), WithFlushInterval(time.Hour)) // only Sync() triggers a flush
	require.NoError(t, err)
	defer w.Close()
	require.True(t, w.IsConnected())

	for i := range 5 {
		_, werr := w.Write([]byte{'a' + byte(i)})
		require.NoError(t, werr)
	}
	require.NoError(t, w.Sync())

	select {
	case got := <-srv.recv:
		require.Equal(t, "a\nb\nc\nd\ne\n", string(got)) // one batched frame
	case <-time.After(2 * time.Second):
		t.Fatal("no flushed batch received")
	}
}

func TestWriter_Async_FlushesOnInterval(t *testing.T) {
	path := randomSocketPath(t)
	srv := startReadServer(t, path)
	defer srv.stop()

	w, err := New(UDS(path), rawEncoder{}, lineFramer{},
		WithAsyncMode(), WithFlushInterval(20*time.Millisecond))
	require.NoError(t, err)
	defer w.Close()

	_, _ = w.Write([]byte("x"))

	select {
	case got := <-srv.recv:
		require.Equal(t, "x\n", string(got))
	case <-time.After(2 * time.Second):
		t.Fatal("interval flush did not fire")
	}
}

func TestWriter_Async_DropNewestWhenFull(t *testing.T) {
	// Tiny buffer, no flushing (huge interval), no server: everything piles up then drops.
	w, err := New(UDS(randomSocketPath(t)), rawEncoder{}, lineFramer{},
		WithAsyncMode(), WithBufferSize(2), WithFlushInterval(time.Hour),
		WithMaxRetries(1), WithDropPolicy(DropNewest))
	require.NoError(t, err)
	defer w.Close()

	for range 50 {
		_, werr := w.Write([]byte("y"))
		require.NoError(t, werr)
	}
	require.Positive(t, w.DroppedLogs())
}

func TestWriter_Async_CloseFlushesRemaining(t *testing.T) {
	path := randomSocketPath(t)
	srv := startReadServer(t, path)
	defer srv.stop()

	w, err := New(UDS(path), rawEncoder{}, lineFramer{},
		WithAsyncMode(), WithFlushInterval(time.Hour))
	require.NoError(t, err)
	require.True(t, w.IsConnected())

	_, _ = w.Write([]byte("z"))
	require.NoError(t, w.Close()) // must flush "z\n" before closing the conn

	select {
	case got := <-srv.recv:
		require.True(t, bytes.Equal(got, []byte("z\n")))
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not flush buffered log")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./... -run TestWriter_Async -race`
Expected: FAIL (stubs deliver nothing; assertions time out).

- [ ] **Step 3: Replace the stubs with the real async implementation**

In `writer.go`, replace the `writeAsync`/`flushLoop` stubs and add `Sync`, `enqueue`,
`drain`, `flush`:

```go
func (w *Writer) writeAsync(p []byte) (int, error) {
	// Async payloads are owned by the queue until flushed, so they cannot share a pooled
	// buffer. (Queue-slot pooling is a documented future optimization.)
	payload, err := w.enc.Encode(nil, p)
	if err != nil {
		return 0, fmt.Errorf("zapwire: encode: %w", err)
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
			batch = w.drain(batch)
			_ = w.flush(batch, frameBuf)

			return
		case ack := <-w.flushReq:
			batch = w.drain(batch)
			frameBuf = w.flush(batch, frameBuf)
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

// flush frames and writes the batch, returning the (possibly grown) frame buffer for reuse.
func (w *Writer) flush(batch [][]byte, frameBuf []byte) []byte {
	if len(batch) == 0 {
		return frameBuf
	}
	frame, err := w.framer.Frame(frameBuf[:0], batch)
	if err != nil {
		w.droppedLogs.Add(uint64(len(batch)))
		w.reportErr(fmt.Errorf("zapwire: frame batch: %w", err))

		return frameBuf
	}
	w.writeFrame(frame, len(batch))

	return frame
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./... -run TestWriter_Async -race`
Expected: PASS (batching, interval flush, drop, close-flush all green).

- [ ] **Step 5: Full core test pass**

Run: `go test ./... -race && make lint`
Expected: PASS, lint clean.

- [ ] **Step 6: Commit**

```bash
git add writer.go writer_async_test.go
git commit -m "feat: add async buffered delivery with batching and drop policy"
```

---

### Task 1.5: zapcore wiring (`NewCore`)

**Files:**
- Create: `core.go`
- Test: `core_test.go`

- [ ] **Step 1: Write the failing test**

`core_test.go`:
```go
package zapwire

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestNewCore_LogsFlowToWriter(t *testing.T) {
	path := randomSocketPath(t)
	srv := startReadServer(t, path)
	defer srv.stop()

	w, err := New(UDS(path), rawEncoder{}, lineFramer{})
	require.NoError(t, err)
	defer w.Close()

	enc := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	logger := zap.New(NewCore(enc, w, zap.InfoLevel))
	logger.Info("hello", zap.Int("n", 1))

	select {
	case got := <-srv.recv:
		require.Contains(t, string(got), `"msg":"hello"`)
		require.Contains(t, string(got), `"n":1`)
	case <-time.After(time.Second):
		t.Fatal("log did not reach the writer")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./... -run TestNewCore`
Expected: FAIL — `undefined: NewCore`.

- [ ] **Step 3: Write `core.go`**

```go
package zapwire

import "go.uber.org/zap/zapcore"

// Writer satisfies zapcore.WriteSyncer (Write + Sync).
var _ zapcore.WriteSyncer = (*Writer)(nil)

// NewCore builds a zapcore.Core that encodes entries with enc and ships the bytes to ws at
// or above level. It is a thin convenience over zapcore.NewCore so callers need not import
// zapcore for the common case.
func NewCore(enc zapcore.Encoder, ws zapcore.WriteSyncer, level zapcore.LevelEnabler) zapcore.Core {
	return zapcore.NewCore(enc, ws, level)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./... -run TestNewCore -race`
Expected: PASS.

- [ ] **Step 5: Tidy, lint, commit**

```bash
go mod tidy
make lint && go test ./... -race
git add core.go core_test.go go.mod go.sum
git commit -m "feat: add NewCore convenience and zapcore.WriteSyncer assertion"
```

---

**Phase 01 done when:** `go test ./... -race` and `make lint` pass; a `zap.Logger` built on
`NewCore` delivers logs through a `Writer` over UDS in both sync and async modes. Proceed to
`02-fluent.md`.
