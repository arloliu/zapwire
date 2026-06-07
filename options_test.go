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

func TestNormalizeConfig_ClampsNonPositiveTunables(t *testing.T) {
	c := normalizeConfig(config{}) // every tunable zero-valued
	require.Equal(t, defaultWriteTimeout, c.writeTimeout)
	require.Equal(t, defaultReconnectInterval, c.reconnectInterval)
	require.Equal(t, defaultMaxReconnect, c.maxReconnect)
	require.Equal(t, defaultMaxRetries, c.maxRetries)
	require.Equal(t, defaultBufferSize, c.bufferSize)
	require.Equal(t, defaultFlushInterval, c.flushInterval)
	require.Equal(t, defaultBatchSize, c.batchSize)

	// Negative values clamp too.
	c = normalizeConfig(config{bufferSize: -1, batchSize: -1, flushInterval: -time.Second})
	require.Equal(t, defaultBufferSize, c.bufferSize)
	require.Equal(t, defaultBatchSize, c.batchSize)
	require.Equal(t, defaultFlushInterval, c.flushInterval)
}

// TestNew_AsyncInvalidSizesDoNotPanic guards the original bug: a negative bufferSize panicked
// in make(chan) AFTER go w.reconnectLoop() launched, leaking a goroutine with no Writer to
// Close it. New must clamp first so construction neither panics nor returns an error here.
func TestNew_AsyncInvalidSizesDoNotPanic(t *testing.T) {
	w, err := New(UDS(randomSocketPath(t)), rawEncoder{}, lineFramer{},
		WithAsyncMode(), WithBufferSize(-1), WithBatchSize(-1),
		WithFlushInterval(-time.Second), WithMaxRetries(1))
	require.NoError(t, err)
	require.NotNil(t, w)
	require.NoError(t, w.Close())
}
