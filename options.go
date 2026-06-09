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
//
// Returns:
//   - Option: sets the delivery mode to ModeSync
func WithSyncMode() Option { return func(c *config) { c.mode = ModeSync } }

// WithAsyncMode selects buffered, batched background delivery.
//
// Returns:
//   - Option: sets the delivery mode to ModeAsync
func WithAsyncMode() Option { return func(c *config) { c.mode = ModeAsync } }

// WithWriteTimeout bounds each socket write.
//
// Parameters:
//   - d: per-write deadline; a non-positive value is replaced by the default
//
// Returns:
//   - Option: sets the bounded write timeout
func WithWriteTimeout(d time.Duration) Option { return func(c *config) { c.writeTimeout = d } }

// WithBufferSize sets the async queue capacity (number of buffered logs).
//
// Parameters:
//   - n: queue capacity in logs; a non-positive value is replaced by the
//     default
//
// Returns:
//   - Option: sets the async buffer size
func WithBufferSize(n int) Option { return func(c *config) { c.bufferSize = n } }

// WithBatchSize caps how many logs a single async flush frames together.
//
// Parameters:
//   - n: max logs per flushed frame; a non-positive value is replaced by the
//     default
//
// Returns:
//   - Option: sets the async batch size
func WithBatchSize(n int) Option { return func(c *config) { c.batchSize = n } }

// WithFlushInterval sets the async max time a log waits before being flushed.
//
// Parameters:
//   - d: max time a log waits before a flush; a non-positive value is replaced
//     by the default
//
// Returns:
//   - Option: sets the async flush interval
func WithFlushInterval(d time.Duration) Option { return func(c *config) { c.flushInterval = d } }

// WithDropPolicy selects the full-buffer drop behavior (async).
//
// Parameters:
//   - p: which log to discard when the buffer is full (DropNewest or
//     DropOldest)
//
// Returns:
//   - Option: sets the async drop policy
func WithDropPolicy(p DropPolicy) Option { return func(c *config) { c.dropPolicy = p } }

// WithMaxRetries bounds reconnect attempts per burst.
//
// Parameters:
//   - n: max reconnect attempts per burst; a non-positive value is replaced by
//     the default
//
// Returns:
//   - Option: sets the reconnect-attempt ceiling
func WithMaxRetries(n int) Option { return func(c *config) { c.maxRetries = n } }

// WithReconnect sets the initial and max reconnect backoff intervals.
//
// Parameters:
//   - initial: first backoff interval; a non-positive value is replaced by the
//     default
//   - maxInterval: backoff ceiling; a non-positive value is replaced by the
//     default
//
// Returns:
//   - Option: sets the reconnect backoff bounds
func WithReconnect(initial, maxInterval time.Duration) Option {
	return func(c *config) { c.reconnectInterval = initial; c.maxReconnect = maxInterval }
}

// WithErrorHandler installs a callback for transport/encode errors. Defaults to
// stderr.
//
// Parameters:
//   - fn: callback invoked with each transport/encode error
//
// Returns:
//   - Option: installs the error callback
func WithErrorHandler(fn func(error)) Option { return func(c *config) { c.onError = fn } }

// normalizeConfig replaces any non-positive tunable with its default. Public options accept
// raw values, so this is the single guard that keeps an invalid size/interval from panicking
// a channel/ticker construction or silently disabling batching. Clamping (rather than an
// error) is the documented contract on the affected With* options.
func normalizeConfig(c config) config {
	if c.writeTimeout <= 0 {
		c.writeTimeout = defaultWriteTimeout
	}
	if c.reconnectInterval <= 0 {
		c.reconnectInterval = defaultReconnectInterval
	}
	if c.maxReconnect <= 0 {
		c.maxReconnect = defaultMaxReconnect
	}
	if c.maxRetries <= 0 {
		c.maxRetries = defaultMaxRetries
	}
	if c.bufferSize <= 0 {
		c.bufferSize = defaultBufferSize
	}
	if c.flushInterval <= 0 {
		c.flushInterval = defaultFlushInterval
	}
	if c.batchSize <= 0 {
		c.batchSize = defaultBatchSize
	}

	return c
}
